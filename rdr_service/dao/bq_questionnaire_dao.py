import datetime
import logging

from sqlalchemy import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_questionnaires import BQPDRTheBasics, BQPDRConsentPII, BQPDRLifestyle, \
    BQPDROverallHealth, BQPDRDVEHRSharing, BQPDREHRConsentPII, BQPDRFamilyHistory, \
    BQPDRHealthcareAccess, BQPDRPersonalMedicalHistory, BQPDRCOPEMay, BQPDRCOPENov, BQPDRCOPEDec


class BQPDRQuestionnaireResponseGenerator(BigQueryGenerator):
    """
    Generate a questionnaire module response BQRecord
    """
    ro_dao = None

    # TODO:  Can we cut down on DB hits by maintaining a class dict variable that saves the list of question codes
    # previously retrieved for each module in the make_bqrecord() table_map?  Is that safe during
    # a longer-running rebuild tasks, or could we miss POST Questionnaire updates that impact this generator?

    def make_bqrecord(self, p_id, module_id, latest=False, convert_to_enum=False):
        """
        Generate a list of questionnaire module BQRecords for the given participant id.
        :param p_id: participant id
        :param module_id: A questionnaire module id, IE: 'TheBasics'.
        :param latest: only process the most recent response if True
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQTable object, List of BQRecord objects
        """

        # Query to get all the question codes across all questionnaire versions of the module.
        _question_code_sql = """
            -- select c.code_id, c.value
            select c.value
            from code c
            inner join (
                select distinct qq.code_id
                from questionnaire_question qq where qq.questionnaire_id in (
                    select qc.questionnaire_id from questionnaire_concept qc
                            where qc.code_id = (
                                select code_id from code c2 where c2.value = :module_id )
                )
            ) qq2 on qq2.code_id = c.code_id
            order by c.code_id;
        """

        # Query to get a list of questionnaire_response_id values for this participant and module
        _participant_module_responses_sql = """
            select qr.questionnaire_id, qr.questionnaire_response_id, qr.created, qr.authored, qr.language,
                   qr.participant_id
            from questionnaire_response qr
            where qr.participant_id = :p_id and questionnaire_id IN (
                select q.questionnaire_id from questionnaire q
                inner join questionnaire_history qh on q.version = qh.version
                       and qh.questionnaire_id = q.questionnaire_id
                inner join questionnaire_concept qc on qc.questionnaire_id = q.questionnaire_id
                inner join code c on c.code_id = qc.code_id
                where c.value = :module_id
                order by qr.created DESC
            );
        """

        # Query to get questionnaire answers for a specific questionnaire_response_id
        _response_answers_sql = """
            SELECT qr.questionnaire_id,
               qq.code_id,
               (select c.value from code c where c.code_id = qq.code_id) as code_name,
               COALESCE((SELECT c.value from code c where c.code_id = qra.value_code_id),
                        qra.value_integer, qra.value_decimal,
                        qra.value_boolean, qra.value_string, qra.value_system,
                        qra.value_uri, qra.value_date, qra.value_datetime) as answer
            FROM questionnaire_response qr
                     INNER JOIN questionnaire_response_answer qra
                                ON qra.questionnaire_response_id = qr.questionnaire_response_id
                     INNER JOIN questionnaire_question qq
                                ON qra.question_id = qq.questionnaire_question_id
                     INNER JOIN questionnaire q
                                ON qq.questionnaire_id = q.questionnaire_id
            WHERE qr.questionnaire_response_id = :qr_id
            order by qr.created DESC;
        """

        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=True)

        table_map = {
            'TheBasics': BQPDRTheBasics,
            'ConsentPII': BQPDRConsentPII,
            'Lifestyle': BQPDRLifestyle,
            'OverallHealth': BQPDROverallHealth,
            'DVEHRSharing': BQPDRDVEHRSharing,
            'EHRConsentPII': BQPDREHRConsentPII,
            'FamilyHistory': BQPDRFamilyHistory,
            'HealthcareAccess': BQPDRHealthcareAccess,
            'PersonalMedicalHistory': BQPDRPersonalMedicalHistory,
            'COPE': BQPDRCOPEMay,
            'cope_nov': BQPDRCOPENov,
            'cope_dec': BQPDRCOPEDec
        }
        table = table_map.get(module_id, None)
        if table is None:
            logging.info('Generator: ignoring questionnaire module id [{0}].'.format(module_id))
            return None, list()

        # This section refactors how the module answer data is built, replacing  the sp_get_questionnaire_answers
        # stored procedure.  That procedure's logic was no longer consistent with how codebook structures are defined
        # in the new REDCap-managed DRC process.  TODO: tech debt for PDR PostgreSQL to consider changes to the
        #  code table sp we can more robustly handle the codebook definitions we get from REDCap
        with self.ro_dao.session() as session:

            question_codes = session.execute(_question_code_sql, {'module_id': module_id})
            # Start with all known question codes for this module, with answer values set to None
            fields = {qc.value: None for qc in question_codes}

            # Retrieve all the responses for this participant/module ID (most recent first)
            qnans = []
            responses = session.execute(_participant_module_responses_sql, {'module_id': module_id, 'p_id': p_id})
            for qr in responses:
                # Populate the response metadata (created, authored, etc.) into a data dict
                data = self.ro_dao.to_dict(qr, result_proxy=responses)

                answers = session.execute(_response_answers_sql, {'qr_id': qr.questionnaire_response_id})
                # Response payloads are processed in reverse chronological order.  Partial responses with a subset of
                # codes/answers may be merged with earlier responses that have a more complete payload.
                ans_dict = {}
                for ans in answers:
                    # Note: BigQuery will ignore unrecognized fields, but log when the response has codes we're seeing
                    # for the first time, in case we need to confirm BQ schemas are in sync with RDR code table
                    if ans.code_name not in fields.keys():
                        logging.warning("""questionnaireResponseID {0} contains previously unrecognized answer code {1}
                                for module {3}
                            """.format(qr.questionnaire_response_id, ans.code_name, module_id))

                    # Handle multi-select question codes (such as ethnicity or gender identity response options) where
                    # user provided more than one answer.  Add to comma-separated list if there's already an answer val
                    # This reproduces the GROUP_CONCAT SQL logic from the deprecated sp_get_questionnaire_answers proc
                    if ans.code_name in ans_dict.keys() and ans_dict[ans.code_name]:
                        prev_answer = ans_dict[ans.code_name]
                        ans_dict[ans.code_name] = ",".join([prev_answer, ans.answer])
                    else:
                        ans_dict[ans.code_name] = ans.answer

                # Populate any answers that were not part of a more recent partial responses already processed
                for code_key, answer_val in ans_dict.items():
                    if not fields[code_key]:
                        fields[code_key] = answer_val

                # Merge the updated full survey fields dict with the response's metadata
                data.update(fields)
                qnans.append(data)

            if len(qnans) == 0:
                return None, list()

            bqrs = list()
            for qnan in qnans:
                bqr = BQRecord(schema=table().get_schema(), data=qnan, convert_to_enum=convert_to_enum)
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
                        'questionnaire_response_id'
                    ):
                        continue

                    fld_value = getattr(bqr, fld_name, None)
                    if fld_value is None:  # Let empty strings pass.
                        continue
                    # question responses values need to be coerced to a String type.
                    if isinstance(fld_value, (datetime.date, datetime.datetime)):
                        setattr(bqr, fld_name, fld_value.isoformat())
                    else:
                        setattr(bqr, fld_name, str(fld_value))

                    # Truncate zip codes to 3 digits
                    if fld_name in ('StreetAddress_PIIZIP', 'EmploymentWorkAddress_ZipCode') and len(fld_value) > 2:
                        setattr(bqr, fld_name, fld_value[:3])

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
        inner join code c on qc.code_id = c.code_id
    where qr.questionnaire_response_id = :qr_id
  """)

    ro_dao = BigQuerySyncDao(backup=True)
    w_dao = BigQuerySyncDao()
    qr_gen = BQPDRQuestionnaireResponseGenerator()
    module_id = None

    with ro_dao.session() as ro_session:

        results = ro_session.execute(sql, {'qr_id': qr_id})
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
