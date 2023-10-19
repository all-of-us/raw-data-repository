import datetime
import logging
from re import match as re_match

from sqlalchemy import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord, BQFieldTypeEnum
from rdr_service.model.bq_questionnaires import PDR_CODE_TO_MODULE_LIST
from rdr_service.code_constants import PPI_SYSTEM, PMI_SKIP_CODE
from rdr_service.participant_enums import QuestionnaireResponseStatus, TEST_HPO_NAME


class BQPDRQuestionnaireResponseGenerator(BigQueryGenerator):
    """
    Generate a questionnaire module response BQRecord
    """
    ro_dao = None

    def make_bqrecord(self, p_id, module_id, latest=False, convert_to_enum=False):
        """
        Generate a list of questionnaire module BQRecords for the given participant id.
        :param p_id: participant id
        :param module_id: A questionnaire module id, IE: 'TheBasics'.  Note that the code table can have multiple
                          entries that match this value, so we automatically filter on the system (PPI_SYSTEM) as well
        :param latest: only process the most recent response if True
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQTable object, List of BQRecord objects
        """

        # Query to get all the question codes across all questionnaire versions of the module.
        _question_code_sql = """
            select c.value
            from code c
            inner join (
                select distinct qq.code_id
                from questionnaire_question qq where qq.questionnaire_id in (
                    select qc.questionnaire_id from questionnaire_concept qc
                            where qc.code_id = (
                                select code_id from code c2 where c2.value = :module_id and c2.system = :system
                            )
                )
            ) qq2 on qq2.code_id = c.code_id
            order by c.code_id;
        """

        # Query to get a list of questionnaire_response_id values for this participant and module
        # The list will be from most recently received/replayed response to earliest.  This mirrors how the
        # deprecated stored procedure sp_get_questionnaire_answers used to order its results
        #
        # PDR-254:  Updating the module response data to include a test_participant flag as a common field
        # Intended to make filtering module response data by test_participant status more efficient in BigQuery
        _participant_module_responses_sql = """
            select qr.questionnaire_id, qr.questionnaire_response_id, qr.created, qr.authored, qr.language,
                   qr.participant_id, qh2.external_id, qr.status,
                   CASE
                       WHEN p.is_test_participant = 1  or p.is_ghost_id = 1 or h.name = :test_hpo THEN 1
                       ELSE 0
                   END as test_participant
            from questionnaire_response qr
            inner join questionnaire_history qh2 on qh2.questionnaire_id = qr.questionnaire_id
                       and qh2.version = qr.questionnaire_version
            inner join participant p on p.participant_id = qr.participant_id
            left join hpo h on p.hpo_id = h.hpo_id
            -- Screen out classification DUPLICATE (1) responses from the PDR data [PDR-640]
            where qr.participant_id = :p_id and qr.classification_type != 1 and qr.classification_type != 6
                and qr.questionnaire_id IN (
                select q.questionnaire_id from questionnaire q
                inner join questionnaire_history qh on q.version = qh.version
                       and qh.questionnaire_id = q.questionnaire_id
                inner join questionnaire_concept qc on qc.questionnaire_id = q.questionnaire_id
                      and qc.questionnaire_version = qh.version
                inner join code c on c.code_id = qc.code_id
                where c.value = :module_id and c.system = :system
                order by qr.created DESC
            );
        """

        # Query to get questionnaire answers for a specific questionnaire_response_id (no ordering needed)
        _response_answers_sql = """
            SELECT qr.questionnaire_id,
               qq.code_id,
               (select c.value from code c where c.code_id = qq.code_id and c.system = :system)
                        as question_code,
               COALESCE((SELECT c.value from code c where c.code_id = qra.value_code_id),
                        qra.value_integer, qra.value_decimal,
                        qra.value_boolean, qra.value_string, qra.value_system,
                        qra.value_uri, qra.value_date, qra.value_datetime) as answer
            FROM questionnaire_response qr
                     INNER JOIN questionnaire_response_answer qra
                                ON qra.questionnaire_response_id = qr.questionnaire_response_id
                     INNER JOIN questionnaire_question qq
                                ON qra.question_id = qq.questionnaire_question_id
            WHERE qr.questionnaire_response_id = :qr_id
                  and (qra.ignore is null or qra.ignore = 0)
        """

        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=True)

        table = PDR_CODE_TO_MODULE_LIST.get(module_id, None)
        if table is None:
            logging.info('Generator: ignoring questionnaire module id [{0}].'.format(module_id))
            return None, list()

        # This section refactors how the module answer data is built, replacing  the sp_get_questionnaire_answers
        # stored procedure.  That procedure's logic was no longer consistent with how codebook structures are defined
        # in the new REDCap-managed DRC process.  TODO: tech debt for PDR PostgreSQL to consider changes to the
        #  code table so we can more robustly handle the codebook definitions we get from REDCap
        with self.ro_dao.session() as session:
            question_codes = session.execute(_question_code_sql, {'module_id': module_id, 'system': PPI_SYSTEM})
            pdr_schema = table().get_schema()
            pdr_field_list = {}
            expected_pdr_columns = [col['name'] for col in pdr_schema.get_fields()]
            for code in question_codes:
                # make_bq_field_name() will either return code.value if it can be used as the PDR table column name,
                # or a mapped version that has converted embedded spaces or / chars, etc., to underscores
                pdr_col_name, _ = pdr_schema.make_bq_field_name(code.value)
                if pdr_col_name not in expected_pdr_columns:
                    logging.warning(f'Unknown question code {code.value} not in {table().get_name()} schema')
                else:
                    pdr_field_list[code.value] = pdr_col_name

            # Retrieve all the responses for this participant/module ID (most recent first)
            qnans = []
            responses = session.execute(_participant_module_responses_sql, {'module_id': module_id, 'p_id': p_id,
                                                                            'test_hpo': TEST_HPO_NAME,
                                                                            'system': PPI_SYSTEM})
            for qr in responses:
                # Populate the response metadata (created, authored, etc.) into a data dict
                data = self.ro_dao.to_dict(qr, result_proxy=responses)
                # PDR-235:  Adding response FHIR status enum values (IN_PROGRESS, COMPLETED, ...)  to the metadata
                # dict.  Providing both string and integer key/value pairs, per the PDR BigQuery schema conventions
                if isinstance(data['status'], int):
                    data['status_id'] = int(QuestionnaireResponseStatus(data['status']))
                    data['status'] = str(QuestionnaireResponseStatus(data['status']))

                answers = session.execute(_response_answers_sql, {'qr_id': qr.questionnaire_response_id,
                                                                  'system': PPI_SYSTEM})
                # Initialize values for each question code/column name in the data dict to null
                ans_dict = {pdr_field_list[field]: None for field in pdr_field_list}
                for ans in answers:
                    # When going through the answers results, have to pass the question_code string to the mapping
                    # function so we match up with the PDR table column/field names (in case they were also mapped)
                    mapped_field, _ = pdr_schema.make_bq_field_name(ans.question_code)
                    if mapped_field not in pdr_field_list:
                        logging.info("""questionnaireResponseID {0} contains previously unrecognized question code {1}
                                for module {2}
                            """.format(qr.questionnaire_response_id, ans.question_code, module_id))

                    # Handle multi-select question codes (can have multiple questionnaire_response_answer results)
                    # Mirrors GROUP_CONCAT SQL logic.  Note (see PDR-1785):  If a single questionnaire response
                    # contains duplicate question/answer values, don't concatenate duplicate answers
                    if mapped_field in pdr_field_list and ans_dict[mapped_field]:
                        prev_answer = ans_dict[mapped_field]
                        if ans.answer and ans.answer not in prev_answer:
                            ans_dict[mapped_field] = ",".join([prev_answer, ans.answer])
                    else:
                        ans_dict[mapped_field] = ans.answer

                # Merge all the answers from this response payload with the response metadata and save
                data.update(ans_dict)
                qnans.append(data)

            if len(qnans) == 0:
                return None, list()

            bqrs = list()
            for qnan in qnans:
                bqr = BQRecord(schema=pdr_schema, data=qnan, convert_to_enum=convert_to_enum)
                bqr.participant_id = p_id  # reset participant_id.

                fields = bqr.get_fields()
                for field in fields:
                    fld_name = field['name']
                    if fld_name in (
                        'id',
                        'created',
                        'modified',
                        'authored',
                        'language',
                        'participant_id',
                        'questionnaire_response_id',
                        'questionnaire_id',
                        'external_id',
                        'status',
                        'status_id'
                    ):
                        continue

                    fld_value = getattr(bqr, fld_name, None)
                    # Check to see if the field has been forced to boolean and set the default.
                    if field['type'] == BQFieldTypeEnum.INTEGER.name:
                        setattr(bqr, fld_name, 0)
                    if fld_value is None:
                        continue
                    # question responses values need to be coerced to a String type.
                    if isinstance(fld_value, (datetime.date, datetime.datetime)):
                        setattr(bqr, fld_name, fld_value.isoformat())
                    # Check to see if the field has been forced to boolean, set to 1 because we have a value.
                    elif field['type'] == BQFieldTypeEnum.INTEGER.name:
                        if fld_value != PMI_SKIP_CODE:
                            setattr(bqr, fld_name, 1)
                    # Truncate zip code fields to 3 chars (PDR fields are strings), provided the field
                    # has a digit as its first non-whitespace char.  This does NOT try to discover all possible
                    # invalid zipcode string conditions before truncating, but ensures coded values like PMI_Skip
                    # don't get truncated
                    elif fld_name in ('StreetAddress_PIIZIP', 'EmploymentWorkAddress_ZipCode') and \
                           len(fld_value.lstrip()) > 2 and re_match(r'^\s*\d+', fld_value):
                        setattr(bqr, fld_name, fld_value.lstrip()[:3])
                    else:
                        setattr(bqr, fld_name, str(fld_value))

                bqrs.append(bqr)
                if latest:
                    break

        return table, bqrs

def bq_questionnaire_update_task(p_id, qr_id):
    """
    Cloud Task: Generate a BQ questionnaire response record from the given p_id and questionnaire response id.
    :param p_id: participant id
    :param qr_id: A questionnaire response id.
    """
    # determine what the module id is for the given questionnaire response id.
    sql = text("""
    select c.value from
        questionnaire_response qr inner join questionnaire_concept qc on qr.questionnaire_id = qc.questionnaire_id
        inner join code c on qc.code_id = c.code_id and c.system = :system
    where qr.questionnaire_response_id = :qr_id
  """)

    ro_dao = BigQuerySyncDao(backup=True)
    w_dao = BigQuerySyncDao()
    qr_gen = BQPDRQuestionnaireResponseGenerator()
    module_id = None

    with ro_dao.session() as ro_session:

        results = ro_session.execute(sql, {'qr_id': qr_id, 'system': PPI_SYSTEM})
        if results:
            for row in results:
                module_id = row.value
                break

            if not module_id:
                logging.warning(f'No questionnaire module id found for questionnaire response id {qr_id}')
                return

            table, bqrs = qr_gen.make_bqrecord(p_id, module_id, latest=True)
            with w_dao.session() as w_session:
                for bqr in bqrs:
                    qr_gen.save_bqrecord(qr_id, bqr, bqtable=table, w_dao=w_dao, w_session=w_session)
