#! /bin/env python
#
# Template for RDR tool python program.
#
import logging
from sqlalchemy import and_, case, insert, or_, text
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql.functions import coalesce, concat
from typing import Type

from rdr_service.clock import CLOCK
from rdr_service import config
from rdr_service.code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE,\
    EMPLOYMENT_ZIPCODE_QUESTION_CODE, STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE, ZIPCODE_QUESTION_CODE
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.etl.model.src_clean import QuestionnaireAnswersByModule, SrcClean
from rdr_service.model.code import Code
from rdr_service.model.hpo import HPO
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer, \
    QuestionnaireResponseClassificationType
from rdr_service.participant_enums import QuestionnaireResponseStatus, WithdrawalStatus
from rdr_service.services.gcp_utils import gcp_sql_export_csv
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "curation"
tool_desc = "Support tool for Curation ETL process"


EXPORT_BATCH_SIZE = 10000

# TODO: Rewrite the Curation ETL bash scripts into multiple Classes here.


class CurationExportClass(ToolBase):
    """
    Export the data from the Curation ETL process.
    """
    tables = ['pid_rid_mapping', 'care_site', 'condition_era', 'condition_occurrence', 'cost', 'death',
              'device_exposure', 'dose_era', 'drug_era', 'drug_exposure', 'fact_relationship',
              'location', 'measurement', 'observation_period', 'payer_plan_period', 'visit_detail',
              'person', 'procedure_occurrence', 'provider', 'visit_occurrence', 'metadata', 'note_nlp']

    # Observation takes a while and ends up timing the client out. The server will continue to process and the client
    # will print out a message describing how to continue to track it, but for now it crashes the script so it has
    # to be last. Breaking it out into it's own list to allow for custom processing before 'finishing' the script
    # TODO: gracefully handle observation's timeout
    problematic_tables = ['observation']

    def __init__(self, args, gcp_env=None, tool_name=None):
        super(CurationExportClass, self).__init__(args, gcp_env, tool_name)
        self.db_conn = None

    def get_field_names(self, table, exclude=None):
        """
        Run a query and get the field list.
        :param table: table name
        :param exclude: list of excluded fields names.
        :return: list of field names
        """
        if not exclude:
            exclude = []
        cursor = self.db_conn.cursor()
        cursor.execute(f"select * from cdm.{table} limit 1")
        cursor.fetchone()
        fields = [f[0] for f in cursor.description if f[0] not in exclude]
        cursor.close()
        return fields

    def export_table(self, table):
        """
        Export table to cloud bucket
        :param table: Table name
        :return:
        """
        cloud_file = f'gs://{self.args.export_path}/{table}.csv'

        # We have to add a row at the start for the CSV headers, Google hasn't implemented another way yet
        # https://issuetracker.google.com/issues/111342008
        column_names = self.get_field_names(table, ['id'])
        header_string = ','.join([f"'{column_name}'" for column_name in column_names])

        # We need to handle NULLs and convert them to empty strings as gcloud sql has a bug when putting them in a csv
        # https://issuetracker.google.com/issues/64579566
        # NULL characters (\0) can also corrupt the output file, so they're removed.
        # And whitespace was trimmed before so that's moved into the SQL as well
        # Newlines and double-quotes are also replaced with spaces and single-quotes, respectively
        field_list = [f"TRIM(REPLACE(REPLACE(REPLACE(COALESCE({name}, ''), '\\0', ''), '\n', ' '), '\\\"', '\\\''))"
                      for name in column_names]

        # Unions are unordered, so the headers do not always end up at the top of the file.
        # The below format forces the headers to the top of the file
        # This is needed because gcloud export sql doesn't support column headers and
        # Curation would like them in the file for schema validation (ROC-687)
        sql_string = f"""
            SELECT {','.join(column_names)}
            FROM
            (
                (
                    SELECT
                      1 as sort_col,
                      {header_string}
                )
                UNION ALL
                (
                    SELECT 2,
                        {','.join(field_list)}
                    FROM {table}
                )
            ) a
            ORDER BY a.sort_col ASC
        """

        _logger.info(f'exporting {table}')
        gcp_sql_export_csv(self.args.project, sql_string, cloud_file, database='cdm')

    def export_cope_map(self):
        cope_map = self.get_server_config()[config.COPE_FORM_ID_MAP]
        cope_external_id_flat_list = []
        external_id_to_month_cases = []
        for external_ids_str, month in cope_map.items():
            quoted_ids = [f"'{external_id}'" for external_id in external_ids_str.split(',')]
            for quoted_id in quoted_ids:
                cope_external_id_flat_list.append(quoted_id)

            external_id_to_month_cases.append(f"when qh.external_id in ({','.join(quoted_ids)}) then '{month.lower()}'")

        export_sql = f"""
            SELECT participant_id, questionnaire_response_id, semantic_version, cope_month
            FROM
            (
                (
                    SELECT
                      1 as sort_col,
                      'participant_id', 'questionnaire_response_id', 'semantic_version', 'cope_month'
                )
                UNION ALL
                (
                    SELECT
                      2 as sort_col,
                      participant_id, questionnaire_response_id, semantic_version,
                      CASE {' '.join(external_id_to_month_cases)}
                      END AS 'cope_month'
                    FROM questionnaire_history qh
                    INNER JOIN questionnaire_response qr ON qr.questionnaire_id = qh.questionnaire_id
                        AND qr.questionnaire_version = qh.version
                    WHERE qh.external_id IN ({','.join(cope_external_id_flat_list)})
                )
            ) a
            ORDER BY a.sort_col ASC
        """
        export_name = 'cope_survey_semantic_version_map'
        cloud_file = f'gs://{self.args.export_path}/{export_name}.csv'

        _logger.info(f'exporting {export_name}')
        gcp_sql_export_csv(self.args.project, export_sql, cloud_file, database='rdr')

    def export_participant_id_map(self):
        dao = ParticipantDao()
        export_sql = dao.get_participant_id_mapping(is_sql=True)

        export_name = 'participant_id_mapping'
        cloud_file = f'gs://{self.args.export_path}/{export_name}.csv'

        _logger.info(f'exporting {export_name}')
        gcp_sql_export_csv(self.args.project, export_sql, cloud_file, database='rdr')

    def run_curation_export(self):
        # Because there are no models for the data stored in the 'cdm' database, we'll
        # just use a standard MySQLDB connection.
        self.db_conn = self.gcp_env.make_mysqldb_connection(user='alembic', database='cdm')

        if not self.args.export_path.startswith('gs://all-of-us-rdr-prod-cdm/'):
            raise NameError("Export path must start with 'gs://all-of-us-rdr-prod-cdm/'.")
        if self.args.export_path.endswith('/'):  # Remove trailing slash if present.
            self.args.export_path = self.args.export_path[5:-1]

        if self.args.table:
            _logger.info(f"Exporting {self.args.table} to {self.args.export_path}...")
            self.export_table(self.args.table)
            return 0

        _logger.info(f"Exporting tables to {self.args.export_path}...")
        for table in self.tables:
            self.export_table(table)

        self.export_cope_map()
        self.export_participant_id_map()

        for table in self.problematic_tables:
            self.export_table(table)

        return 0

    @staticmethod
    def _create_tables(session, table_class_list):
        for table_class in table_class_list:
            table_metadata = table_class.__table__

            # Drop and create table
            table_metadata.drop(session.bind, checkfirst=True)
            table_metadata.create(session.bind)

    @staticmethod
    def _set_rdr_model_schema(model_class_list):
        """
        When using a session set for the CDM database, any models referenced from the RDR database
        need to have their schema set. Doing so results in output that explicitly references the RDR
        database (ie: setting the schema will change it from "questionnaire_response"
        to "rdr.questionnaire_response" in the sql generated).
        """
        for rdr_model_class in model_class_list:
            rdr_model_class.__table__.schema = 'rdr'

    @staticmethod
    def _module_code_or_external_id_if_cope(code_reference: Type[Code]):
        return case(
            [(code_reference.value == 'COPE', QuestionnaireHistory.externalId)],
            else_=code_reference.value
        )

    def _populate_questionnaire_answers_by_module(self, session):
        self._set_rdr_model_schema([Code, QuestionnaireResponse, QuestionnaireConcept, QuestionnaireHistory,
                                    QuestionnaireQuestion, QuestionnaireResponseAnswer])
        column_map = {
            QuestionnaireAnswersByModule.participant_id: QuestionnaireResponse.participantId,
            QuestionnaireAnswersByModule.authored: QuestionnaireResponse.authored,
            QuestionnaireAnswersByModule.created: QuestionnaireResponse.created,
            QuestionnaireAnswersByModule.survey: self._module_code_or_external_id_if_cope(Code),
            QuestionnaireAnswersByModule.response_id: QuestionnaireResponse.questionnaireResponseId,
            QuestionnaireAnswersByModule.question_code_id: QuestionnaireQuestion.codeId
        }

        # QuestionnaireResponse is implicitly the first table, others are joined
        answers_by_module_select = session.query(*column_map.values()).join(
            QuestionnaireConcept,
            and_(
                QuestionnaireConcept.questionnaireId == QuestionnaireResponse.questionnaireId,
                QuestionnaireConcept.questionnaireVersion == QuestionnaireResponse.questionnaireVersion
            )
        ).join(
            Code,
            Code.codeId == QuestionnaireConcept.codeId
        ).join(
            QuestionnaireHistory,
            and_(
                QuestionnaireHistory.questionnaireId == QuestionnaireResponse.questionnaireId,
                QuestionnaireHistory.version == QuestionnaireResponse.questionnaireVersion
            )
        ).join(
            QuestionnaireResponseAnswer,
            QuestionnaireResponseAnswer.questionnaireResponseId == QuestionnaireResponse.questionnaireResponseId
        ).join(
            QuestionnaireQuestion
        ).filter(
            QuestionnaireResponse.status != QuestionnaireResponseStatus.IN_PROGRESS,
            QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE
        )

        insert_query = insert(QuestionnaireAnswersByModule).from_select(column_map.keys(), answers_by_module_select)
        session.execute(insert_query)

    @staticmethod
    def _get_base_src_clean_answers_select(session):
        module_code = aliased(Code)
        question_code = aliased(Code)
        answer_code = aliased(Code)

        # TODO: when the responses with these answers in the valueInteger field are cleaned up, we can remove this
        zipcode_question_codes_to_remap = [EMPLOYMENT_ZIPCODE_QUESTION_CODE, ZIPCODE_QUESTION_CODE]

        column_map = {
            SrcClean.participant_id: Participant.participantId,
            SrcClean.research_id: Participant.researchId,
            SrcClean.external_id: Participant.externalId,
            SrcClean.survey_name: module_code.value,
            SrcClean.date_of_survey: coalesce(QuestionnaireResponse.authored, QuestionnaireResponse.created),
            SrcClean.question_ppi_code: question_code.value,
            SrcClean.question_code_id: QuestionnaireQuestion.codeId,
            SrcClean.value_ppi_code: answer_code.value,
            SrcClean.topic_value: answer_code.topic,
            SrcClean.value_code_id: QuestionnaireResponseAnswer.valueCodeId,
            SrcClean.value_number: case([(
                # Only set value number if the question code is not one of the zip codes to re-map
                question_code.value.notin_(zipcode_question_codes_to_remap),
                coalesce(QuestionnaireResponseAnswer.valueDecimal, QuestionnaireResponseAnswer.valueInteger)
            )]),
            SrcClean.value_boolean: QuestionnaireResponseAnswer.valueBoolean,
            SrcClean.value_date: coalesce(
                QuestionnaireResponseAnswer.valueDate,
                QuestionnaireResponseAnswer.valueDateTime
            ),
            SrcClean.value_string: coalesce(
                func.left(QuestionnaireResponseAnswer.valueString, 1024),
                QuestionnaireResponseAnswer.valueDate,
                QuestionnaireResponseAnswer.valueDateTime,
                answer_code.display,
                case([  # Use valueInteger if the question code should be re-mapped
                    (question_code.value.in_(zipcode_question_codes_to_remap),
                     QuestionnaireResponseAnswer.valueInteger)
                ])
            ),
            SrcClean.questionnaire_response_id: QuestionnaireResponse.questionnaireResponseId,
            SrcClean.unit_id: concat(
                'cln.',
                case(
                    [
                        (QuestionnaireResponseAnswer.valueCodeId.isnot(None), 'code'),
                        (QuestionnaireResponseAnswer.valueInteger.isnot(None), 'int'),
                        (QuestionnaireResponseAnswer.valueDecimal.isnot(None), 'dec'),
                        (QuestionnaireResponseAnswer.valueBoolean.isnot(None), 'bool'),
                        (QuestionnaireResponseAnswer.valueDate.isnot(None), 'date'),
                        (QuestionnaireResponseAnswer.valueDateTime.isnot(None), 'dtime'),
                        (QuestionnaireResponseAnswer.valueString.isnot(None), 'str'),
                    ],
                    else_=''
                )
            ),
            SrcClean.filter: literal_column('0')
        }

        # Participant is implicitly the first table, others are joined
        questionnaire_answers_select = session.query(*column_map.values()).join(
            HPO
        ).join(
            QuestionnaireResponse
        ).join(
            QuestionnaireConcept,
            and_(
                QuestionnaireConcept.questionnaireId == QuestionnaireResponse.questionnaireId,
                QuestionnaireConcept.questionnaireVersion == QuestionnaireResponse.questionnaireVersion
            )
        ).join(
            QuestionnaireResponseAnswer
        ).join(
            QuestionnaireQuestion,
            QuestionnaireQuestion.questionnaireQuestionId == QuestionnaireResponseAnswer.questionId
        ).join(
            QuestionnaireHistory,
            and_(
                QuestionnaireHistory.questionnaireId == QuestionnaireResponse.questionnaireId,
                QuestionnaireHistory.version == QuestionnaireResponse.questionnaireVersion
            )
        ).join(
            question_code,
            question_code.codeId == QuestionnaireQuestion.codeId
        ).outerjoin(
            answer_code,
            answer_code.codeId == QuestionnaireResponseAnswer.valueCodeId
        ).outerjoin(
            module_code,
            module_code.codeId == QuestionnaireConcept.codeId
        ).outerjoin(
            ParticipantSummary,
            ParticipantSummary.participantId == Participant.participantId
        ).filter(
            Participant.withdrawalStatus != WithdrawalStatus.NO_USE,
            Participant.isGhostId.isnot(True),
            Participant.isTestParticipant.isnot(True),
            Participant.participantOrigin != 'careevolution',
            HPO.name != 'TEST',
            or_(
                and_(QuestionnaireResponseAnswer.valueCodeId.isnot(None), answer_code.codeId.isnot(None)),
                QuestionnaireResponseAnswer.valueInteger.isnot(None),
                QuestionnaireResponseAnswer.valueDecimal.isnot(None),
                QuestionnaireResponseAnswer.valueBoolean.isnot(None),
                QuestionnaireResponseAnswer.valueDate.isnot(None),
                QuestionnaireResponseAnswer.valueDateTime.isnot(None),
                QuestionnaireResponseAnswer.valueString.isnot(None)
            ),
            QuestionnaireResponse.status != QuestionnaireResponseStatus.IN_PROGRESS,
            QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE,
            and_(
                ParticipantSummary.dateOfBirth.isnot(None),
                func.timestampdiff(text('YEAR'), ParticipantSummary.dateOfBirth, CLOCK.now()) >= 18
            )
        )

        return column_map, questionnaire_answers_select, module_code, question_code

    def _populate_src_clean(self, session):
        self._set_rdr_model_schema([Code, HPO, Participant, QuestionnaireQuestion, ParticipantSummary,
                                    QuestionnaireResponse, QuestionnaireResponseAnswer])

        # These modules should have the latest answers for each question,
        # rather than the answers from the latest response
        rolled_up_module_codes = [CONSENT_FOR_STUDY_ENROLLMENT_MODULE]

        responses_by_module_subquery = session.query(
            QuestionnaireAnswersByModule.participant_id,
            QuestionnaireAnswersByModule.response_id,
            QuestionnaireAnswersByModule.survey,
            QuestionnaireAnswersByModule.authored,
            QuestionnaireAnswersByModule.created
        ).distinct().subquery()

        column_map, questionnaire_answers_select, module_code, question_code \
            = self._get_base_src_clean_answers_select(session)

        latest_responses_select = questionnaire_answers_select.outerjoin(
            responses_by_module_subquery,
            and_(
                responses_by_module_subquery.c.participant_id == QuestionnaireResponse.participantId,
                responses_by_module_subquery.c.response_id != QuestionnaireResponse.questionnaireResponseId,
                responses_by_module_subquery.c.survey == self._module_code_or_external_id_if_cope(module_code),
                case(  # If the authored date for the responses match, then join based on the created date instead
                    [(responses_by_module_subquery.c.authored == QuestionnaireResponse.authored,
                      responses_by_module_subquery.c.created > QuestionnaireResponse.created)],
                    else_=(responses_by_module_subquery.c.authored > QuestionnaireResponse.authored)
                )
            )
        ).filter(
            responses_by_module_subquery.c.participant_id.is_(None),
            module_code.system == PPI_SYSTEM,
            module_code.value.notin_(rolled_up_module_codes)
        )

        insert_latest_responses_query = insert(SrcClean).from_select(column_map.keys(), latest_responses_select)
        session.execute(insert_latest_responses_query)

        street_address_1_code = session.query(Code).filter(Code.value == STREET_ADDRESS_QUESTION_CODE).one()

        rolled_up_responses_select = questionnaire_answers_select.outerjoin(
            QuestionnaireAnswersByModule,
            and_(
                QuestionnaireAnswersByModule.participant_id == QuestionnaireResponse.participantId,
                QuestionnaireAnswersByModule.response_id != QuestionnaireResponse.questionnaireResponseId,
                QuestionnaireAnswersByModule.survey == self._module_code_or_external_id_if_cope(module_code),
                case(
                    [(
                        # Any street address 2 answers should also be ignored if there are any later
                        # street address 1 answers
                        question_code.value == STREET_ADDRESS2_QUESTION_CODE,
                        QuestionnaireAnswersByModule.question_code_id.in_([
                            QuestionnaireQuestion.codeId,
                            street_address_1_code.codeId
                        ])
                    )],
                    else_=QuestionnaireAnswersByModule.question_code_id == QuestionnaireQuestion.codeId
                ),
                case(  # If the authored date for the responses match, then join based on the created date instead
                    [(QuestionnaireAnswersByModule.authored == QuestionnaireResponse.authored,
                      QuestionnaireAnswersByModule.created > QuestionnaireResponse.created)],
                    else_=(QuestionnaireAnswersByModule.authored > QuestionnaireResponse.authored)
                )
            )
        ).filter(
            QuestionnaireAnswersByModule.id.is_(None),
            module_code.system == PPI_SYSTEM,
            module_code.value.in_(rolled_up_module_codes)
        )

        insert_query = insert(SrcClean).from_select(column_map.keys(), rolled_up_responses_select)
        session.execute(insert_query)

    def populate_cdm_database(self):
        with self.get_session(database_name='cdm', alembic=True) as session:  # using alembic to get CREATE permission
            self._create_tables(session, [
                QuestionnaireAnswersByModule,
                SrcClean
            ])

        # using alembic here to get the database_factory code to set up a connection to the CDM database
        with self.get_session(database_name='cdm', alembic=True, isolation_level='READ UNCOMMITTED') as session:
            self._populate_questionnaire_answers_by_module(session)
            self._populate_src_clean(session)

        return 0

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        super(CurationExportClass, self).run()

        if self.args.command == 'export':
            return self.run_curation_export()
        elif self.args.command == 'cdm-data':
            return self.populate_cdm_database()

        return 0


def add_additional_arguments(parser):
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    subparsers = parser.add_subparsers(dest='command')

    export_parser = subparsers.add_parser('export')
    export_parser.add_argument("--export-path", help="Bucket path to export to", required=True, type=str)  # noqa
    export_parser.add_argument("--table", help="Export a specific table", type=str, default=None)  # noqa

    subparsers.add_parser('cdm-data')


def run():
    cli_run(tool_cmd, tool_desc, CurationExportClass, add_additional_arguments)
