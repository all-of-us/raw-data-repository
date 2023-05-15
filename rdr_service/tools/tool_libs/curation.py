#! /bin/env python
#
# Template for RDR tool python program.
#
import os
from datetime import datetime
import logging
import pytz
import sqlalchemy.orm.session
from sqlalchemy import and_, case, insert, or_, text, not_, literal
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql.functions import coalesce, concat
from typing import Type, List, Callable, Union

from rdr_service import config
from rdr_service import api_util
from rdr_service.code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE, PMI_SKIP_CODE, \
    EMPLOYMENT_ZIPCODE_QUESTION_CODE, STREET_ADDRESS_QUESTION_CODE, STREET_ADDRESS2_QUESTION_CODE, ZIPCODE_QUESTION_CODE
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.etl.model.src_clean import QuestionnaireAnswersByModule, SrcClean, Location, CareSite, Provider, \
    Person, Death, ObservationPeriod, PayerPlanPeriod, VisitOccurrence, ConditionOccurrence, ProcedureOccurrence, \
    Observation, Measurement, Note, DrugExposure, DeviceExposure, Cost, FactRelationship, ConditionEra, DrugEra, \
    DoseEra, Metadata, NoteNlp, VisitDetail, SrcParticipant, SrcMapped, SrcPersonLocation, SrcGender, SrcRace, \
    SrcEthnicity, SrcMeas, MeasurementCodeMap, MeasurementValueCodeMap, SrcMeasMapped, SrcVisits, TempObsTarget, \
    TempObsEndUnion, TempObsEndUnionPart, TempObsEnd, TempObs, TempFactRelSd, PidRidMapping, \
    QuestionnaireResponseAdditionalInfo
from rdr_service.model.code import Code
from rdr_service.model.hpo import HPO
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer, \
    QuestionnaireResponseClassificationType
from rdr_service.model.curation_etl import CdrExcludedCode
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.participant_enums import QuestionnaireResponseStatus, WithdrawalStatus, CdrEtlCodeType,\
    QuestionnaireStatus, DeceasedReportStatus
from rdr_service.services.gcp_utils import gcp_sql_export_csv
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.dao.curation_etl_dao import CdrEtlRunHistoryDao, CdrEtlSurveyHistoryDao, CdrExcludedCodeDao
from rdr_service.services.system_utils import list_chunks

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "curation"
tool_desc = "Support tool for Curation ETL process"


EXPORT_BATCH_SIZE = 10000
CHUNK_SIZE = 1000

# TODO: Rewrite the Curation ETL bash scripts into multiple Classes here.


class CurationExportClass(ToolBase):
    """
    Export the data from the Curation ETL process.
    """
    tables = ['pid_rid_mapping', 'care_site', 'condition_era', 'condition_occurrence', 'cost', 'death',
              'device_exposure', 'dose_era', 'drug_era', 'drug_exposure', 'fact_relationship',
              'location', 'measurement', 'observation_period', 'payer_plan_period', 'visit_detail',
              'person', 'procedure_occurrence', 'provider', 'visit_occurrence', 'metadata', 'note_nlp',
              'questionnaire_response_additional_info']

    # Observation takes a while and ends up timing the client out. The server will continue to process and the client
    # will print out a message describing how to continue to track it, but for now it crashes the script so it has
    # to be last. Breaking it out into it's own list to allow for custom processing before 'finishing' the script
    # TODO: gracefully handle observation's timeout
    problematic_tables = ['observation']

    def __init__(self, args, gcp_env=None, tool_name=None, replica=False):
        super(CurationExportClass, self).__init__(args, gcp_env, tool_name, replica)
        self.db_conn = None
        self.cdr_etl_run_history_dao = CdrEtlRunHistoryDao()
        self.cdr_etl_survey_history_dao = CdrEtlSurveyHistoryDao()
        self.pid_list: List[int] = []
        self.include_surveys: List[str] = []
        self.exclude_surveys: List[str] = []
        self.exclude_pid_list: List[int] = []
        self.cutoff_date = None
        self.include_in_person_pm: bool = True
        self.include_remote_pm: bool = True

    @classmethod
    def _render_export_select(cls, export_sql, column_name_list):
        # We need to handle NULLs and convert them to empty strings as gcloud sql has a bug when putting them in a csv
        # https://issuetracker.google.com/issues/64579566
        # NULL characters (\0) can also corrupt the output file, so they're removed.
        # And whitespace was trimmed before so that's moved into the SQL as well
        # Newlines and double-quotes are also replaced with spaces and single-quotes, respectively
        data_select_list = [
            f"TRIM(REPLACE(REPLACE(REPLACE(COALESCE({name}, ''), '\\0', ''), '\n', ' '), '\\\"', '\\\''))"
            for name in column_name_list
        ]

        # We have to add a row at the start for the CSV headers, Google hasn't implemented another way yet
        # https://issuetracker.google.com/issues/111342008
        # The below format forces the headers to the top of the file.
        # Curation would like them in the file for schema validation (ROC-687)
        return f"""
            SELECT {', '.join(column_name_list)}
            FROM
            (
                (
                    SELECT
                      1 as sort_col,
                      {', '.join([f"'{column_name}'" for column_name in column_name_list])}
                )
                UNION ALL
                (
                    SELECT 2 as sort_col, {','.join(data_select_list)}
                    FROM ({export_sql}) as data
                )
            ) a
            ORDER BY a.sort_col ASC
        """

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
        sql_string = self._render_export_select(
            export_sql=f"SELECT * FROM {table}",
            column_name_list=self.get_field_names(table, exclude=['id'])
        )

        _logger.info(f'exporting {table}')
        cloud_file = f'gs://{self.args.export_path}/{table}.csv'
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

        export_sql = self._render_export_select(
            export_sql=f"""
                SELECT
                  qr.participant_id, questionnaire_response_id, semantic_version,
                  CASE {' '.join(external_id_to_month_cases)}
                  END AS 'cope_month',
                  p.participant_origin
                FROM questionnaire_history qh
                INNER JOIN questionnaire_response qr ON qr.questionnaire_id = qh.questionnaire_id
                    AND qr.questionnaire_version = qh.version
                JOIN participant p on qr.participant_id = p.participant_id
                WHERE qh.external_id IN ({','.join(cope_external_id_flat_list)})
            """,
            column_name_list=['participant_id', 'questionnaire_response_id', 'semantic_version', 'cope_month', 'src_id']
        )
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

    def export_etl_run_info(self):
        with self.get_session() as session:
            etl_run_info_sql = self.cdr_etl_run_history_dao.get_last_etl_run_info(session, is_sql=True)
            etl_run_code_info = self.cdr_etl_survey_history_dao.get_last_etl_run_code_history(session, is_sql=True)

        run_info_export_name = 'cdr_etl_run_info'
        code_info_export_name = 'cdr_etl_run_code_info'

        cloud_file = f'gs://{self.args.export_path}/{run_info_export_name}.csv'
        _logger.info(f'exporting {run_info_export_name}')
        gcp_sql_export_csv(self.args.project, etl_run_info_sql, cloud_file, database='rdr')

        cloud_file = f'gs://{self.args.export_path}/{code_info_export_name}.csv'
        _logger.info(f'exporting {code_info_export_name}')
        gcp_sql_export_csv(self.args.project, etl_run_code_info, cloud_file, database='rdr')

    def export_survey_conduct(self):
        export_sql = self._render_export_select(
            export_sql=f"""
                SELECT qr.questionnaire_response_id survey_conduct_id,
                        p.participant_id person_id,
                        COALESCE(voc_c.concept_id, 0) survey_concept_id,
                        NULL survey_start_date,
                        NULL survey_start_datetime,
                        DATE(qr.authored) survey_end_date,
                        qr.authored survey_end_datetime,
                        0 provider_id,
                        CASE WHEN
                            qr.non_participant_author = 'CATI' THEN     42530794
                            ELSE                                        0
                        END assisted_concept_id,
                        0 respondent_type_concept_id,
                        0 timing_concept_id,
                        CASE WHEN
                            qr.non_participant_author = 'CATI' THEN     42530794
                            ELSE                                        42531021
                        END collection_method_concept_id,
                        CASE WHEN
                            qr.non_participant_author = 'CATI' THEN     'Telephone'
                            ELSE                                        'No matching concept'
                        END assisted_source_value,
                        NULL respondent_type_source_value,
                        '' timing_source_value,
                        CASE WHEN
                            qr.non_participant_author = 'CATI' THEN     'Telephone'
                            ELSE                                        'Electronic'
                        END collection_method_source_value,
                        mc.value survey_source_value,
                        mc.code_id survey_source_concept_id,
                        qr.questionnaire_response_id survey_source_identifier,
                        0 validated_survey_concept_id,
                        '' validated_survey_source_value,
                        NULL survey_version_number,
                        '' visit_occurrence_id,
                        '' response_visit_occurrence_id,
                        p.participant_origin src_id
                FROM questionnaire_response qr
                INNER JOIN questionnaire_concept qc
                    ON qc.questionnaire_id = qr.questionnaire_id AND qc.questionnaire_version = qr.questionnaire_version
                INNER JOIN code mc ON mc.code_id = qc.code_id
                INNER JOIN participant p ON p.participant_id = qr.participant_id
                LEFT JOIN voc.concept voc_c
                    ON voc_c.concept_code = mc.value AND voc_c.vocabulary_id = 'PPI'
                    AND voc_c.domain_id = 'observation' AND voc_c.concept_class_id = 'module'
                WHERE qr.questionnaire_response_id in (
                    SELECT DISTINCT sc.questionnaire_response_id
                    FROM cdm.src_clean sc
                    WHERE sc.filter = 0
                )
            """,
            column_name_list=[
                'survey_conduct_id',
                'person_id',
                'survey_concept_id',
                'survey_start_date',
                'survey_start_datetime',
                'survey_end_date',
                'survey_end_datetime',
                'provider_id',
                'assisted_concept_id',
                'respondent_type_concept_id',
                'timing_concept_id',
                'collection_method_concept_id',
                'assisted_source_value',
                'respondent_type_source_value',
                'timing_source_value',
                'collection_method_source_value',
                'survey_source_value',
                'survey_source_concept_id',
                'survey_source_identifier',
                'validated_survey_concept_id',
                'validated_survey_source_value',
                'survey_version_number',
                'visit_occurrence_id',
                'response_visit_occurrence_id',
                'src_id'
            ]
        )
        export_name = 'survey_conduct'
        cloud_file = f'gs://{self.args.export_path}/{export_name}.csv'

        _logger.info(f'exporting {export_name}')
        gcp_sql_export_csv(self.args.project, export_sql, cloud_file, database='rdr')

    def run_curation_export(self):
        # Because there are no models for the data stored in the 'cdm' database, we'll
        # just use a standard MySQLDB connection.
        self.db_conn = self.gcp_env.make_mysqldb_connection(user='alembic', database='cdm')

        if not any((self.args.export_path.startswith('gs://all-of-us-rdr-prod-cdm/'),
                    self.args.export_path.startswith('gs://all-of-us-rdr-stable-cdm'))):
            raise NameError("Export path must start with 'gs://all-of-us-rdr-prod-cdm/'"
                            "or 'gs://all-of-us-rdr-stable-cdm/'.")
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
        self.export_survey_conduct()
        self.export_etl_run_info()

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

    def _populate_questionnaire_answers_by_module(self, session, pid_list:List[int], cutoff_date=None):
        session.execute("TRUNCATE TABLE questionnaire_answers_by_module")
        self._set_rdr_model_schema([Code, QuestionnaireResponse, QuestionnaireConcept, QuestionnaireHistory,
                                    QuestionnaireQuestion, QuestionnaireResponseAnswer, CdrExcludedCode])
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
            QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE,
            QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.PROFILE_UPDATE,
            QuestionnaireResponse.participantId.in_(pid_list)
        )
        if cutoff_date:
            answers_by_module_select = answers_by_module_select.filter(
                QuestionnaireResponse.authored < cutoff_date
            )
        if self.include_surveys:
            answers_by_module_select = answers_by_module_select.filter(
                Code.value.in_(self.include_surveys)
            )
        elif self.exclude_surveys:
            answers_by_module_select = answers_by_module_select.filter(
                Code.value.notin_(self.exclude_surveys)
            )

        insert_query = insert(QuestionnaireAnswersByModule).from_select(column_map.keys(), answers_by_module_select)
        session.execute(insert_query)

    @classmethod
    def _null_if_answer_ignored(cls, else_value):
        return case(
            [
                (QuestionnaireResponseAnswer.ignore.is_(True), None)
            ],
            else_=else_value
        )

    @classmethod
    def _get_base_src_clean_answers_select(cls, session, pid_list:List[int], cutoff_date=None, include_surveys=None,
                                           exclude_surveys=None):
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
            SrcClean.value_ppi_code: case(
                [
                    (QuestionnaireResponseAnswer.ignore.is_(True), PMI_SKIP_CODE)
                ],
                else_=answer_code.value
            ),
            SrcClean.topic_value: answer_code.topic,
            SrcClean.is_invalid: QuestionnaireResponseAnswer.ignore.is_(True),
            SrcClean.value_code_id: cls._null_if_answer_ignored(else_value=QuestionnaireResponseAnswer.valueCodeId),
            SrcClean.value_number: cls._null_if_answer_ignored(else_value=case([(
                # Only set value number if the question code is not one of the zip codes to re-map
                question_code.value.notin_(zipcode_question_codes_to_remap),
                coalesce(QuestionnaireResponseAnswer.valueDecimal, QuestionnaireResponseAnswer.valueInteger)
            )])),
            SrcClean.value_boolean: cls._null_if_answer_ignored(else_value=QuestionnaireResponseAnswer.valueBoolean),
            SrcClean.value_date: cls._null_if_answer_ignored(else_value=coalesce(
                QuestionnaireResponseAnswer.valueDate,
                QuestionnaireResponseAnswer.valueDateTime
            )),
            SrcClean.value_string: cls._null_if_answer_ignored(else_value=coalesce(
                func.left(QuestionnaireResponseAnswer.valueString, 1024),
                QuestionnaireResponseAnswer.valueDate,
                QuestionnaireResponseAnswer.valueDateTime,
                answer_code.display,
                case([  # Use valueInteger if the question code should be re-mapped
                    (question_code.value.in_(zipcode_question_codes_to_remap),
                     QuestionnaireResponseAnswer.valueInteger)
                ])
            )),
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
            SrcClean.filter: literal_column('0'),
            SrcClean.src_id: Participant.participantOrigin
        }

        questionnaire_answers_select = session.query(*column_map.values()).select_from(
            Participant
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
        ).filter(
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
            QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.PROFILE_UPDATE,

            not_(QuestionnaireConcept.codeId.in_(
                session.query(CdrExcludedCode.codeId).filter(
                    CdrExcludedCode.codeType == CdrEtlCodeType.MODULE).subquery())),
            not_(QuestionnaireQuestion.codeId.in_(
                session.query(CdrExcludedCode.codeId).filter(
                    CdrExcludedCode.codeType == CdrEtlCodeType.QUESTION).subquery())),
            or_(
                QuestionnaireResponseAnswer.valueCodeId.is_(None),
                not_(QuestionnaireResponseAnswer.valueCodeId.in_(
                    session.query(CdrExcludedCode.codeId).filter(
                        CdrExcludedCode.codeType == CdrEtlCodeType.ANSWER).subquery()))
            ),
            QuestionnaireResponse.participantId.in_(pid_list)
        )

        if cutoff_date is not None:
            questionnaire_answers_select = questionnaire_answers_select.filter(
                QuestionnaireResponse.authored < cutoff_date
            )

        if include_surveys:
            questionnaire_answers_select = questionnaire_answers_select.filter(
                module_code.value.in_(include_surveys)
            )
        elif exclude_surveys:
            questionnaire_answers_select = questionnaire_answers_select.filter(
                module_code.value.notin_(exclude_surveys)
            )

        return column_map, questionnaire_answers_select, module_code, question_code

    def _populate_src_clean(self, session, pid_list:List[int], cutoff_date=None):

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
            = self._get_base_src_clean_answers_select(session, pid_list, cutoff_date, self.include_surveys,
                                                      self.exclude_surveys)

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

    def _select_participant_ids(self, session, origin:str, cutoff_date:datetime=None) -> None:
        """ Generates a list of PIDs to build the src_clean tables with"""
        self._set_rdr_model_schema([HPO, Participant, ParticipantSummary])
        query = session.query(
            Participant.participantId
        ).join(
            ParticipantSummary,
            Participant.participantId == ParticipantSummary.participantId
        ).join(
            HPO,
            Participant.hpoId == HPO.hpoId
        ).filter(
            or_(
                Participant.isGhostId.isnot(True),
                and_(
                    ParticipantSummary.participantId.isnot(None),
                    Participant.dateAddedGhost > datetime(2022, 3, 18),
                    or_(
                        ParticipantSummary.consentForElectronicHealthRecords != QuestionnaireStatus.UNSET,
                        ParticipantSummary.questionnaireOnTheBasics == QuestionnaireStatus.SUBMITTED
                    )
                )
            ),
            Participant.isTestParticipant.isnot(True),
            HPO.name != 'TEST',
            ParticipantSummary.dateOfBirth.isnot(None),
            ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored.isnot(None),
            func.timestampdiff(text('YEAR'), ParticipantSummary.dateOfBirth,
                               ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored) >= 18

        )
        if cutoff_date:
            query = query.filter(
                or_(
                    and_(
                        ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored < cutoff_date,
                        ParticipantSummary.withdrawalStatus != WithdrawalStatus.NO_USE
                    ),
                    and_(
                        ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored < cutoff_date,
                        ParticipantSummary.withdrawalStatus == WithdrawalStatus.NO_USE,
                        ParticipantSummary.withdrawalAuthored >= cutoff_date
                    )
                )
            )
        else:
            query = query.filter(
                ParticipantSummary.withdrawalStatus != WithdrawalStatus.NO_USE
            )

        if origin == 'vibrent':
            query = query.filter(
                Participant.participantOrigin == 'vibrent'
            )
        elif origin == 'careevolution':
            query = query.filter(
                Participant.participantOrigin == 'careevolution'
            )
        if self.exclude_pid_list:
            query = query.filter(
                Participant.participantId.notin_(self.exclude_pid_list)
            )
        result = query.order_by(Participant.participantId).all()
        self.pid_list = [pid[0] for pid in result]

    def _populate_death_table(self, session: sqlalchemy.orm.session):
        # Populates death table from deceased_report
        self._set_rdr_model_schema([DeceasedReport])
        column_map = {
            Death.id: literal("0"),
            Death.person_id: DeceasedReport.participantId,
            Death.death_date: DeceasedReport.dateOfDeath,
            Death.death_datetime: DeceasedReport.dateOfDeath.label('date_of_death_datetime'),
            Death.death_type_concept_id: literal("32809"),
            Death.cause_concept_id: literal(None),
            Death.cause_source_value: literal(None),
            Death.cause_source_concept_id: literal(None),
            Death.src_id: literal("healthpro")
        }
        deceased_select = session.query(*column_map.values()).select_from(
            DeceasedReport
        ).join(
         Person,
         DeceasedReport.participantId == Person.person_id
        ).filter(
            DeceasedReport.status == DeceasedReportStatus.APPROVED
        )
        insert_query = insert(Death).from_select(column_map.keys(), deceased_select)
        session.execute(insert_query)

    def populate_cdm_database(self):
        """ Generates the src_clean table which is used to populate the rest of the ETL tables """
        surveys_file_name = None
        include_flag = False
        filter_options = {}

        if self.args.cutoff:
            cutoff_date = api_util.parse_date(self.args.cutoff, '%Y-%m-%d')
            self.cutoff_date = cutoff_date.replace(tzinfo=pytz.UTC)
            _logger.info(f"populating cdm data with cutoff date {self.args.cutoff}...")
        else:
            _logger.info("populating cdm data without cutoff date")
        _logger.info(f"{self.args}")
        if not any((self.args.participant_origin, self.args.participant_list_file)):
            raise NameError("One of parameters participant-origin or participant-list-file is required")
        elif all((self.args.participant_origin, self.args.participant_list_file)):
            raise NameError(
                "Only one of parameters participant-origin or participant-list-file may be used")

        if all((self.args.include_surveys, self.args.exclude_surveys)):
            raise NameError("Cannot use both --include-surveys and --exclude-surveys")

        if self.args.participant_list_file:
            if not os.path.exists(self.args.participant_list_file):
                raise NameError(f'File {self.args.participant_list_file} was not found.')
            with open(self.args.participant_list_file, encoding='utf-8-sig') as pid_file:
                lines = pid_file.readlines()
                for line in lines:
                    self.pid_list.append(int(line.strip()))
            filter_options["participant_list_file"] = self.args.participant_list_file
        else:
            filter_options["participant_origin"] = self.args.participant_origin

        if self.args.exclude_participants:
            if not os.path.exists(self.args.exclude_participants):
                raise NameError(f'File {self.args.exclude_participants} was not found.')
            with open(self.args.exclude_participants, encoding='utf-8-sig') as pid_file:
                lines = pid_file.readlines()
                for line in lines:
                    self.exclude_pid_list.append(int(line.strip()))
            filter_options["participant_exclude_file"] = self.args.exclude_participants

        if self.args.include_surveys:
            surveys_file_name = self.args.include_surveys
            include_flag = True
        elif self.args.exclude_surveys:
            surveys_file_name = self.args.exclude_surveys

        if surveys_file_name:
            surveys = []
            if not os.path.exists(surveys_file_name):
                raise NameError(f'File {surveys_file_name} was not found.')
            with open(surveys_file_name, encoding='utf-8-sig') as surveys_file:
                lines = surveys_file.readlines()
                for line in lines:
                    surveys.append(line.strip())
            if include_flag:
                self.include_surveys = surveys
                filter_options["include_surveys"] = surveys
            else:
                self.exclude_surveys = surveys
                filter_options["exclude_surveys"] = surveys

        if self.args.omit_surveys:
            filter_options["omit_surveys"] = True

        if self.args.omit_measurements:
            filter_options["omit_measurements"] = True

        if self.args.exclude_in_person_pm:
            self.include_in_person_pm = False
            filter_options["in_person_pm"] = False
        else:
            filter_options["in_person_pm"] = True

        if self.args.exclude_remote_pm:
            self.include_remote_pm = False
            filter_options["remote_pm"] = False
        else:
            filter_options["remote_pm"] = True

        # save ETL running info into ETL history table
        if not self.args.vocabulary:
            raise NameError(
                "parameter vocabulary must be set, example: gs://curation-vocabulary/aou_vocab_20220201/")

        with self.get_session() as session:
            etl_history = self.cdr_etl_run_history_dao.create_etl_history_record(session, self.cutoff_date,
                                                                                 self.args.vocabulary, filter_options)
        # Create cdm tables
        self._initialize_cdm()

        # using alembic here to get the database_factory code to set up a connection to the CDM database
        with self.get_session(database_name='cdm', alembic=True, isolation_level='READ UNCOMMITTED') as session:
            if not self.args.participant_list_file:
                _logger.debug("Selecting participant IDs")
                self._select_participant_ids(session, self.args.participant_origin, self.cutoff_date)

            _logger.debug(f"Populating with {len(self.pid_list)} PIDs")
            _logger.debug("Populating src_clean")
            self.run_function_on_pids(self._build_src_clean, session, "src_clean")

            self._finalize_src_clean(session)

            self.run_function_on_pids(self._filter_question, session, "filtering src_clean")
            _logger.debug("Populating src_participant")
            self.run_function_on_pids(self._populate_src_participant, session, "src_participant")
            self.run_function_on_pids(self._populate_src_mapped, session, "src_mapped")

            self._populate_src_tables(session)
            if not self.args.omit_measurements:
                _logger.debug("Populating measurements")
                self._populate_measurements(session, self.cutoff_date, self.include_in_person_pm,
                                            self.include_remote_pm)
            if not self.args.omit_surveys:
                _logger.debug("Populating observation survey data")
                self.run_function_on_pids(self._populate_observation_surveys, session, "observation survey data")
                self._populate_questionnaire_response_additional_info(session)
            self._populate_death_table(session)
            _logger.debug("Finalizing ETL")
            self._finalize_cdm(session)

        _logger.debug("Saving ETL run history")
        with self.get_session() as session:
            self.cdr_etl_survey_history_dao.save_include_exclude_code_history_for_etl_run(session, etl_history.id)
            self.cdr_etl_run_history_dao.update_etl_end_time(session, etl_history.id)

        return 0

    def _build_src_clean(self, session:sqlalchemy.orm.session.Session, participant_id_subset:List[int]):
        self._populate_questionnaire_answers_by_module(session, participant_id_subset, self.cutoff_date)
        self._populate_src_clean(session, participant_id_subset, self.cutoff_date)

    def run_function_on_pids(self, _func: Callable, session: sqlalchemy.orm.session.Session, description: str,
                             chunk_size=1000):
        chunk = 1
        full_pid_list_len = len(self.pid_list)
        chunks = int(full_pid_list_len / chunk_size) + 1
        for participant_id_subset in list_chunks(lst=self.pid_list, chunk_size=chunk_size):
            _logger.debug(f"{description}: Chunk {chunk} of {chunks}")
            chunk += 1
            _func(session, participant_id_subset)

    def manage_etl_exclude_code(self):
        if not self.args.operation or self.args.operation not in ['add', 'remove']:
            raise NameError("parameter operation must be set for exclude-code command "
                            "and the value should be add or remove")
        if not self.args.code_value:
            raise NameError("parameter code-value must be set for manage-code command")
        if not self.args.code_type or self.args.code_type not in ['module', 'question', 'answer']:
            raise NameError("parameter code-type must be set for manage-code command "
                            "and the value should be module, question or answer")

        code_values = self.args.code_value.split(',')
        code_type_map = {
            'module': CdrEtlCodeType.MODULE,
            'question': CdrEtlCodeType.QUESTION,
            'answer': CdrEtlCodeType.ANSWER
        }
        dao = CdrExcludedCodeDao()
        with self.get_session() as session:
            if self.args.operation == 'remove':
                for value in code_values:
                    dao.remove_excluded_code(session, value, code_type_map[self.args.code_type])
            elif self.args.operation == 'add':
                for value in code_values:
                    dao.add_excluded_code(session, value, code_type_map[self.args.code_type])

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
        elif self.args.command == 'exclude-code':
            return self.manage_etl_exclude_code()

        return 0

    def _initialize_cdm(self):
        with self.get_session(database_name='cdm', alembic=True) as session:  # using alembic to get CREATE permission
            self._create_tables(session, [
                QuestionnaireAnswersByModule,
                SrcClean,
                SrcParticipant,
                Note,
                DrugExposure,
                DeviceExposure,
                Cost,
                FactRelationship,
                ConditionEra,
                DrugEra,
                DoseEra,
                Metadata,
                NoteNlp,
                VisitDetail,
                Location,
                CareSite,
                Provider,
                Person,
                Death,
                ObservationPeriod,
                PayerPlanPeriod,
                VisitOccurrence,
                ConditionOccurrence,
                ProcedureOccurrence,
                Observation,
                Measurement,
                SrcMapped,
                SrcPersonLocation,
                SrcGender,
                SrcRace,
                SrcEthnicity,
                SrcMeas,
                MeasurementCodeMap,
                MeasurementValueCodeMap,
                SrcMeasMapped,
                SrcVisits,
                TempObsTarget,
                TempObsEndUnion,
                TempObsEndUnionPart,
                TempObsEnd,
                TempObs,
                TempFactRelSd,
                PidRidMapping,
                QuestionnaireResponseAdditionalInfo
            ])

    def _finalize_cdm(self, session, drop_tables: bool = False, drop_columns: bool = True):
        # -- In patient surveys data only organs transplantation information
        # -- fits the procedure_occurrence table.
        session.execute("""INSERT INTO cdm.procedure_occurrence
                            SELECT
                                NULL                                        AS procedure_occurrence_id,
                                src_m1.participant_id                       AS person_id,
                                COALESCE(vc.concept_id, 0)                  AS procedure_concept_id,
                                src_m2.value_date                           AS procedure_date,
                                TIMESTAMP(src_m2.value_date)                AS procedure_datetime,
                                581412                                      AS procedure_type_concept_id,   -- 581412, Procedure Recorded from a Survey
                                0                                           AS modifier_concept_id,
                                NULL                                        AS quantity,
                                NULL                                        AS provider_id,
                                NULL                                        AS visit_occurrence_id,
                                NULL                                        AS visit_detail_id,
                                stcm.source_code                            AS procedure_source_value,
                                COALESCE(stcm.source_concept_id, 0)         AS procedure_source_concept_id,
                                NULL                                        AS modifier_source_value,
                                'procedure'                                 AS unit_id,
                                src_m1.src_id                               AS src_id
                            FROM cdm.src_mapped src_m1
                            INNER JOIN cdm.source_to_concept_map stcm
                                ON src_m1.value_ppi_code = stcm.source_code
                                AND stcm.source_vocabulary_id = 'ppi-proc'
                            INNER JOIN cdm.src_mapped src_m2
                                ON src_m1.participant_id = src_m2.participant_id
                                AND src_m2.question_ppi_code = 'OrganTransplant_Date'
                                AND src_m2.value_date IS NOT NULL
                            LEFT JOIN voc.concept vc
                                ON stcm.target_concept_id = vc.concept_id
                                AND vc.standard_concept = 'S'
                                AND vc.invalid_reason IS NULL
                                """)




        session.execute("""INSERT INTO cdm.temp_obs_target
                            -- VISIT_OCCURENCE
                            SELECT
                                0 AS id,
                                person_id,
                                visit_start_date                               AS start_date,
                                COALESCE(visit_end_date, visit_start_date) AS end_date
                            FROM cdm.visit_occurrence

                            UNION
                            -- CONDITION_OCCURRENCE
                            SELECT
                                0 AS id,
                                person_id,
                                condition_start_date                                   AS start_date,
                                COALESCE(condition_end_date, condition_start_date) AS end_date
                            FROM cdm.condition_occurrence

                            UNION
                            -- PROCEDURE_OCCURRENCE
                            SELECT
                                0 AS id,
                                person_id,
                                procedure_date                    AS start_date,
                                procedure_date                    AS end_date
                            FROM cdm.procedure_occurrence

                            UNION
                            -- OBSERVATION
                            SELECT
                                0 AS id,
                                person_id,
                                observation_date                    AS start_date,
                                observation_date                    AS end_date
                            FROM cdm.observation

                            UNION
                            -- MEASUREMENT
                            SELECT
                                0 AS id,
                                person_id,
                                measurement_date                    AS start_date,
                                measurement_date                    AS end_date
                            FROM cdm.measurement

                            UNION
                            -- DEVICE_EXPOSURE
                            SELECT
                                0 AS id,
                                person_id,
                                device_exposure_start_date                                          AS start_date,
                                COALESCE( device_exposure_end_date, device_exposure_start_date) AS end_date
                            FROM cdm.device_exposure

                            UNION
                            -- DRUG_EXPOSURE
                            SELECT
                                0 AS id,
                                person_id,
                                drug_exposure_start_date                                        AS start_date,
                                COALESCE( drug_exposure_end_date, drug_exposure_start_date) AS end_date
                            FROM cdm.drug_exposure
                                    """)
        session.execute("""CREATE INDEX temp_obs_target_idx_start ON cdm.temp_obs_target (person_id, start_date);
                           CREATE INDEX temp_obs_target_idx_end ON cdm.temp_obs_target (person_id, end_date);
                        """)
        session.execute("""SELECT NULL INTO @partition_expr;
                            SELECT NULL INTO @last_part_expr;
                            SELECT NULL INTO @row_number;
                            SELECT NULL INTO @reset_num;

                            INSERT INTO cdm.temp_obs_end_union
                            SELECT
                              0                           AS id,
                              person_id                   AS person_id,
                              start_date                  AS event_date,
                              -1                          AS event_type,
                              row_number                  AS start_ordinal
                            FROM
                                ( SELECT
                                    @partition_expr := person_id                                AS partition_expr,
                                    @reset_num :=
                                        CASE
                                            WHEN @partition_expr = @last_part_expr THEN 0
                                            ELSE 1
                                        END                                                     AS reset_num,
                                    @last_part_expr := @partition_expr                          AS last_part_expr,
                                    @row_number :=
                                        CASE
                                            WHEN @reset_num = 0 THEN @row_number + 1
                                            ELSE 1
                                        END                                                     AS row_number,
                                    person_id,
                                    start_date
                                  FROM cdm.temp_obs_target
                                  ORDER BY
                                    person_id,
                                    start_date
                                ) F
                            UNION ALL
                            SELECT
                              0                                 AS id,
                              person_id                         AS person_id,
                              (end_date + INTERVAL 1 DAY)       AS event_date,
                              1                                 AS event_type,
                              NULL                              AS start_ordinal
                            FROM cdm.temp_obs_target
                                    """)
        # -- We need to re-count event ordinal number in 'overall_ord' and define null start_ordinal
        # -- by start_ordinal of start_event. So events, owned by the same observation, will have
        # -- the same ordinal number - the start_ordinal of start observation event.
        # -- overall_ord is overall counter of start and end observations events.
        session.execute("""SELECT NULL INTO @partition_expr;
                            SELECT NULL INTO @last_part_expr;
                            SELECT NULL INTO @row_number;
                            SELECT NULL INTO @reset_num;
                            SELECT NULL INTO @row_max;
                            INSERT INTO cdm.temp_obs_end_union_part
                            SELECT
                                0                                    AS id,
                                person_id                            AS person_id,
                                event_date                           AS event_date,
                                event_type                           AS event_type,
                                row_max                              AS start_ordinal,
                                row_number                           AS overall_ord
                            FROM  (
                                    SELECT
                                        @partition_expr := person_id                                 AS partition_expr,
                                        @reset_num :=
                                            CASE
                                                WHEN @partition_expr = @last_part_expr THEN 0
                                                ELSE 1
                                            END                                                      AS reset_num,
                                        @last_part_expr := @partition_expr                           AS last_part_expr,
                                        @row_number :=
                                            CASE
                                                WHEN @reset_num = 0 THEN @row_number + 1
                                                ELSE 1
                                            END                                                      AS row_number,
                                        @row_max :=
                                            CASE
                                                WHEN @reset_num = 1 THEN start_ordinal
                                                ELSE COALESCE(start_ordinal, @row_max)
                                            END                                                      AS row_max,
                                        person_id,
                                        event_date,
                                        event_type,
                                        start_ordinal
                                    FROM cdm.temp_obs_end_union
                                    ORDER BY
                                        person_id,
                                        event_date,
                                        event_type
                                        ) F
        """)
        # -- Here we just filter observations ends. As start_ordinal of start and
        # -- end events is the same, expression
        # -- (2 * start_ordinal) == e.overall_ord gives us observation end event.
        session.execute("""INSERT INTO  cdm.temp_obs_end
                            SELECT
                                0                                             AS id,
                                person_id                                     AS person_id,
                                (event_date - INTERVAL 1 DAY)                 AS end_date,
                                start_ordinal                                 AS start_ordinal,
                                overall_ord                                   AS overall_ord
                            FROM cdm.temp_obs_end_union_part e
                            WHERE
                                (2 * e.start_ordinal) - e.overall_ord = 0
                        """)
        session.execute("""CREATE INDEX temp_obs_end_idx ON cdm.temp_obs_end (person_id, end_date)""")
        # -- Here we form observations start and end dates. For each start_date
        # -- we look for minimal end_date for the particular person observation.
        session.execute("""INSERT INTO cdm.temp_obs
                            SELECT
                                0                             AS id,
                                dt.person_id,
                                dt.start_date                 AS observation_start_date,
                                MIN(e.end_date)               AS observation_end_date
                            FROM cdm.temp_obs_target dt
                            JOIN cdm.temp_obs_end e
                                ON dt.person_id = e.person_id AND
                                e.end_date >= dt.start_date
                            GROUP BY
                                dt.person_id,
                                dt.start_date
                                    """)
        session.execute("""CREATE INDEX temp_obs_idx ON cdm.temp_obs (person_id, observation_end_date)""")

        # -- observation_period is formed as merged possibly intersecting
        # -- tmp_obs intervals
        session.execute("""INSERT INTO cdm.observation_period
                            SELECT
                                NULL                                    AS observation_period_id,
                                temp_obs.person_id                               AS person_id,
                                MIN(observation_start_date)             AS observation_period_start_date,
                                observation_end_date                    AS observation_period_end_date,
                                44814725                                AS period_type_concept_id,         -- 44814725, Period inferred by algorithm
                                'observ_period'                       AS unit_id,
                                p.src_id                              AS src_id
                            FROM cdm.temp_obs
                            JOIN person p on temp_obs.person_id = p.id
                            GROUP BY
                                person_id,
                                observation_end_date
                                    """)




        session.execute("""INSERT INTO cdm.pid_rid_mapping
                            SELECT DISTINCT sc.participant_id, sc.research_id, sc.external_id, sc.src_id
                            FROM cdm.src_clean sc join cdm.person p on sc.participant_id=p.person_id
                                    """)
        if drop_tables:
            # Drop Temporary Tables
            session.execute("""  DROP TABLE IF EXISTS cdm.src_gender;
                                DROP TABLE IF EXISTS cdm.src_race;
                                DROP TABLE IF EXISTS cdm.src_person_location;
                            """)
            session.execute("""DROP TABLE IF EXISTS cdm.temp_cdm_observation_period;
                                DROP TABLE IF EXISTS cdm.temp_obs_target;
                                DROP TABLE IF EXISTS cdm.temp_obs_end_union;
                                DROP TABLE IF EXISTS cdm.temp_obs_end;
                                DROP TABLE IF EXISTS cdm.temp_obs_end_union_part;
                                DROP TABLE IF EXISTS cdm.temp_obs;
            """)
            session.execute("""DROP TABLE IF EXISTS cdm.tmp_visits_src""")
            session.execute("""DROP TABLE IF EXISTS cdm.tmp_fact_rel_sd;""")
            session.execute("""DROP TABLE IF EXISTS cdm.tmp_cv_concept_lk;
                               DROP TABLE IF EXISTS cdm.tmp_vcv_concept_lk;
                            """)

        if drop_columns:
            # Drop columns only used for ETL purposes
            session.execute("""ALTER TABLE cdm.care_site DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.condition_era DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.condition_occurrence DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.cost DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.device_exposure DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.dose_era DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.drug_era DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.drug_exposure DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.fact_relationship DROP COLUMN unit_id, DROP COLUMN id;""")
            session.execute("""
                                ALTER TABLE cdm.location DROP COLUMN unit_id;
                                ALTER TABLE cdm.measurement DROP COLUMN unit_id, DROP COLUMN parent_id, DROP COLUMN id;
                                ALTER TABLE cdm.observation DROP COLUMN unit_id, DROP COLUMN meas_id;
                                ALTER TABLE cdm.observation_period DROP COLUMN unit_id;
                                ALTER TABLE cdm.payer_plan_period DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.person DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.procedure_occurrence DROP COLUMN unit_id;
                                ALTER TABLE cdm.provider DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.visit_occurrence DROP COLUMN unit_id, DROP COLUMN id;
                                ALTER TABLE cdm.metadata DROP COLUMN id;
                                ALTER TABLE cdm.death DROP COLUMN id;
                                        """)

    @staticmethod
    def _finalize_src_clean(session):

        session.execute("Delete from voc.concept WHERE concept_id IN (1585549, 1585565, 1585548)")
        # Update cdm.src_clean to filter specific surveys.
        session.execute("UPDATE combined_survey_filter SET survey_name = REPLACE(survey_name, '\r', '');")
        session.execute("""UPDATE cdm.src_clean
                            INNER JOIN cdm.combined_survey_filter ON
                                cdm.src_clean.survey_name = cdm.combined_survey_filter.survey_name
                            SET cdm.src_clean.filter = 1
                            WHERE TRUE""")

        # Update cdm.src_clean to filter specific survey questions.
        session.execute("UPDATE combined_question_filter SET question_ppi_code = REPLACE(question_ppi_code, '\r', '')")
        session.execute("""CREATE INDEX src_cln_p_id ON cdm.src_clean (participant_id);
                           CREATE INDEX src_cln_filter ON cdm.src_clean (filter)""")

    @staticmethod
    def _filter_question(session, pid_list):
        session.execute(f"""UPDATE cdm.src_clean
                            INNER JOIN cdm.combined_question_filter ON
                                cdm.src_clean.question_ppi_code = cdm.combined_question_filter.question_ppi_code
                            SET cdm.src_clean.filter = 1
                            WHERE cdm.src_clean.participant_id IN ({",".join([str(pid) for pid in pid_list])})""")


    @staticmethod
    def _populate_src_participant(session, pid_list):
        session.execute(f"""INSERT INTO cdm.src_participant
                            SELECT
                                f1.participant_id,
                                f1.latest_date_of_survey,
                                f1.date_of_birth,
                                f1.src_id
                            FROM
                                (SELECT
                                    t1.participant_id           AS participant_id,
                                    t1.latest_date_of_survey    AS latest_date_of_survey,
                                    MAX(DATE(t2.value_date))    AS date_of_birth,
                                    t1.src_id                   AS src_id
                                FROM
                                    (
                                    SELECT
                                        src_c.participant_id        AS participant_id,
                                        MAX(src_c.date_of_survey)   AS latest_date_of_survey,
                                        src_c.src_id                AS src_id
                                    FROM cdm.src_clean src_c
                                    WHERE
                                        src_c.question_ppi_code = 'PIIBirthInformation_BirthDate'
                                        AND src_c.value_date IS NOT NULL
                                        AND src_c.participant_id IN ({",".join([str(pid) for pid in pid_list])})
                                    GROUP BY
                                        src_c.participant_id,
                                        src_c.src_id
                                    ) t1
                                INNER JOIN cdm.src_clean t2
                                    ON t1.participant_id = t2.participant_id
                                    AND t1.latest_date_of_survey = t2.date_of_survey
                                    AND t2.question_ppi_code = 'PIIBirthInformation_BirthDate'
                                GROUP BY
                                    t1.participant_id,
                                    t1.latest_date_of_survey,
                                    t1.src_id
                                ) f1""")

    @staticmethod
    def _populate_src_mapped(session, pid_list):
        session.execute(f"""INSERT INTO cdm.src_mapped
                            SELECT
                                0                                   AS id,
                                src_c.participant_id                AS participant_id,
                                src_c.date_of_survey                AS date_of_survey,
                                src_c.question_ppi_code             AS question_ppi_code,
                                src_c.question_code_id              AS question_code_id,
                                COALESCE(vc1.concept_id, 0)         AS question_source_concept_id,
                                COALESCE(vc2.concept_id, 0)         AS question_concept_id,
                                src_c.value_ppi_code                AS value_ppi_code,
                                src_c.topic_value                   AS topic_value,
                                src_c.value_code_id                 AS value_code_id,
                                COALESCE(vc3.concept_id, 0)         AS value_source_concept_id,
                                CASE
                                    WHEN src_c.is_invalid = 1 THEN 2000000010
                                    ELSE COALESCE(vc4.concept_id, 0)
                                END                                 AS value_concept_id,
                                src_c.value_number                  AS value_number,
                                src_c.value_boolean                 AS value_boolean,
                                CASE
                                    WHEN src_c.value_boolean = 1 THEN 45877994
                                    WHEN src_c.value_boolean = 0 THEN 45878245
                                    ELSE 0
                                END                                 AS value_boolean_concept_id,
                                src_c.value_date                    AS value_date,
                                src_c.value_string                  AS value_string,
                                src_c.questionnaire_response_id     AS questionnaire_response_id,
                                src_c.unit_id                       AS unit_id,
                                src_c.is_invalid                    as is_invalid,
                                src_c.src_id                        AS src_id
                            FROM cdm.src_clean src_c
                            JOIN cdm.src_participant src_p
                                ON  src_c.participant_id = src_p.participant_id
                            LEFT JOIN voc.tmp_voc_concept vc1
                                ON  src_c.question_ppi_code = vc1.concept_code
                            LEFT JOIN voc.tmp_con_rel_mapsto vcr1
                                ON  vc1.concept_id = vcr1.concept_id_1
                            LEFT JOIN voc.tmp_voc_concept_s vc2
                                ON  vcr1.concept_id_2 = vc2.concept_id
                            LEFT JOIN voc.tmp_voc_concept vc3
                                ON  src_c.value_ppi_code = vc3.concept_code
                            LEFT JOIN voc.tmp_con_rel_mapstoval vcr2
                                ON  vc3.concept_id = vcr2.concept_id_1
                            LEFT JOIN voc.tmp_voc_concept_s vc4
                                ON  vcr2.concept_id_2 = vc4.concept_id
                            WHERE src_c.participant_id IN ({",".join([str(pid) for pid in pid_list])})
                            AND src_c.filter = 0
                            """)

    def _populate_src_tables(self, session):
        session.execute("""
                ALTER TABLE cdm.src_mapped ADD KEY (question_ppi_code);
                CREATE INDEX mapped_p_id_and_ppi ON cdm.src_mapped (participant_id, question_ppi_code);
                CREATE INDEX mapped_qr_id_and_ppi ON cdm.src_mapped (questionnaire_response_id, question_ppi_code)
                        """)

        session.execute("""
                INSERT INTO cdm.src_person_location
                SELECT
                    src_participant.participant_id        AS participant_id,
                    MAX(m_address_1.value_string)         AS address_1,
                    MAX(m_address_2.value_string)         AS address_2,
                    MAX(m_city.value_string)              AS city,
                    MAX(m_zip.value_string)               AS zip,
                    MAX(m_state.value_ppi_code)           AS state_ppi_code,
                    MAX(RIGHT(m_state.value_ppi_code, 2)) AS state,
                    NULL                                  AS location_id
                FROM src_participant
                  INNER JOIN
                    cdm.src_mapped m_address_1
                      ON src_participant.participant_id = m_address_1.participant_id
                     AND m_address_1.question_ppi_code = 'PIIAddress_StreetAddress'
                  LEFT JOIN
                    cdm.src_mapped m_address_2
                      ON m_address_1.questionnaire_response_id = m_address_2.questionnaire_response_id
                     AND m_address_2.question_ppi_code = 'PIIAddress_StreetAddress2'
                  LEFT JOIN
                    cdm.src_mapped m_city
                      ON m_address_1.questionnaire_response_id = m_city.questionnaire_response_id
                     AND m_city.question_ppi_code = 'StreetAddress_PIICity'
                  LEFT JOIN
                    cdm.src_mapped m_zip
                      ON m_address_1.questionnaire_response_id = m_zip.questionnaire_response_id
                     AND m_zip.question_ppi_code = 'StreetAddress_PIIZIP'
                  LEFT JOIN
                    cdm.src_mapped m_state
                      ON m_address_1.questionnaire_response_id = m_state.questionnaire_response_id
                     AND m_state.question_ppi_code = 'StreetAddress_PIIState'
                WHERE m_address_1.date_of_survey =
                  (SELECT MAX(date_of_survey)
                     FROM cdm.src_mapped m_address_1_2
                    WHERE m_address_1_2.participant_id = m_address_1.participant_id
                      AND m_address_1_2.question_ppi_code = 'PIIAddress_StreetAddress')
                GROUP BY src_participant.participant_id;
                """)

        session.execute("""
        INSERT INTO cdm.location (location_id, address_1, address_2, city, state, zip, county, location_source_value, unit_id)
            SELECT DISTINCT
                NULL                            AS location_id,
                src.address_1                   AS address_1,
                src.address_2                   AS address_2,
                src.city                        AS city,
                src.state                       AS state,
                src.zip                         AS zip,
                NULL                            AS county,
                src.state_ppi_code              AS location_source_value,
                'loc'                           AS unit_id
            FROM cdm.src_person_location src
        """)

        session.execute("CREATE INDEX location_address ON cdm.location (address_1, zip)")

        session.execute("""UPDATE cdm.src_person_location person_loc, cdm.location loc
                           SET person_loc.location_id = loc.location_id
                         WHERE person_loc.address_1 <=> loc.address_1
                           AND person_loc.address_2 <=> loc.address_2
                           AND person_loc.city <=> loc.city
                           AND person_loc.state <=> loc.state
                           AND person_loc.zip <=> loc.zip
        """)

        # -- Map many non-standard genders from src_mapped to allowed
        # -- by cdm standards by 'source_to_concept_map' relation.
        session.execute("""INSERT INTO cdm.src_gender
                            SELECT DISTINCT
                                src_m.participant_id                    AS person_id,
                                MIN(stcm1.source_code)                  AS ppi_code,
                                MIN(stcm1.source_concept_id)            AS gender_source_concept_id,
                                MIN(COALESCE(vc1.concept_id, 0))        AS gender_target_concept_id
                            FROM cdm.src_mapped src_m
                            INNER JOIN cdm.source_to_concept_map stcm1
                                ON src_m.value_ppi_code = stcm1.source_code
                                AND stcm1.priority = 1              -- priority 1
                                AND stcm1.source_vocabulary_id = 'ppi-sex'
                            LEFT JOIN voc.concept vc1
                                ON stcm1.target_concept_id = vc1.concept_id
                                AND vc1.standard_concept = 'S'
                                AND vc1.invalid_reason IS NULL
                            GROUP BY src_m.participant_id
                            HAVING
                                COUNT(distinct src_m.value_ppi_code) = 1
                                """)
        # -- Map many non-standard races from src_mapped to allowed
        # -- by cdm standards by 'source_to_concept_map' relation.
        # -- priority = 1 means more detailed racial
        # -- information over priority = 2. So if patient provides
        # -- detailed answer about his/her race, we firstly
        # -- use it.
        session.execute("""INSERT INTO cdm.src_race
                            SELECT DISTINCT
                                src_m.participant_id                    AS person_id,
                                MIN(stcm1.source_code)                  AS ppi_code,
                                MIN(stcm1.source_concept_id)            AS race_source_concept_id,
                                MIN(COALESCE(vc1.concept_id, 0))        AS race_target_concept_id
                            FROM cdm.src_mapped src_m
                            INNER JOIN cdm.source_to_concept_map stcm1
                                ON src_m.value_ppi_code = stcm1.source_code
                                AND stcm1.priority = 1              -- priority 1
                                AND stcm1.source_vocabulary_id = 'ppi-race'
                            LEFT JOIN voc.concept vc1
                                ON stcm1.target_concept_id = vc1.concept_id
                                AND vc1.standard_concept = 'S'
                                AND vc1.invalid_reason IS NULL
                            GROUP BY src_m.participant_id
                            HAVING
                                COUNT(distinct src_m.value_ppi_code) = 1
                                """)
        # -- Then we find and insert priority-2 (more common)
        # -- race info, if priority-1 info was not already
        # -- provided.
        session.execute("""INSERT INTO cdm.src_race
                            SELECT DISTINCT
                                src_m.participant_id                    AS person_id,
                                MIN(stcm1.source_code)                  AS ppi_code,
                                MIN(stcm1.source_concept_id)            AS race_source_concept_id,
                                MIN(COALESCE(vc1.concept_id, 0))        AS race_target_concept_id
                            FROM cdm.src_mapped src_m
                            INNER JOIN cdm.source_to_concept_map stcm1
                                ON src_m.value_ppi_code = stcm1.source_code
                                AND stcm1.priority = 2              -- priority 2
                                AND stcm1.source_vocabulary_id = 'ppi-race'
                            LEFT JOIN voc.concept vc1
                                ON stcm1.target_concept_id = vc1.concept_id
                                AND vc1.standard_concept = 'S'
                                AND vc1.invalid_reason IS NULL
                            WHERE
                                NOT EXISTS (SELECT * FROM cdm.src_race g
                                            WHERE src_m.participant_id = g.person_id)
                            GROUP BY src_m.participant_id
                            HAVING
                                COUNT(distinct src_m.value_ppi_code) = 1
                                """)
        # -- Map many non-standard ethnicities from src_mapped to allowed
        # -- by cdm standards by 'source_to_concept_map' relation.
        # -- priority = 1 means more detailed ethnic
        # -- information over priority = 2. So if patient provides
        # -- detailed answer about his/her ethnicity, we firstly
        # -- use it.
        session.execute("""INSERT INTO cdm.src_ethnicity
                            SELECT DISTINCT
                                src_m.participant_id                    AS person_id,
                                MIN(stcm1.source_code)                  AS ppi_code,
                                MIN(stcm1.source_concept_id)            AS ethnicity_source_concept_id,
                                MIN(COALESCE(vc1.concept_id, 0))        AS ethnicity_target_concept_id
                            FROM cdm.src_mapped src_m
                            INNER JOIN cdm.source_to_concept_map stcm1
                                ON src_m.value_ppi_code = stcm1.source_code
                                AND stcm1.priority = 1              -- priority 1
                                AND stcm1.source_vocabulary_id = 'ppi-ethnicity'
                            LEFT JOIN voc.concept vc1
                                ON stcm1.target_concept_id = vc1.concept_id
                                AND vc1.standard_concept = 'S'
                                AND vc1.invalid_reason IS NULL
                            GROUP BY src_m.participant_id
                            HAVING
                                COUNT(distinct src_m.value_ppi_code) = 1
                                """)
        # -- Then we find and insert priority-2 (more common)
        # -- ethnicity info, if priority-1 info was not already
        # -- provided.
        session.execute("""INSERT INTO cdm.src_ethnicity
                            SELECT DISTINCT
                                src_m.participant_id                    AS person_id,
                                MIN(stcm1.source_code)                  AS ppi_code,
                                MIN(stcm1.source_concept_id)            AS ethnicity_source_concept_id,
                                MIN(COALESCE(vc1.concept_id, 0))        AS ethnicity_target_concept_id
                            FROM cdm.src_mapped src_m
                            INNER JOIN cdm.source_to_concept_map stcm1
                                ON src_m.value_ppi_code = stcm1.source_code
                                AND stcm1.priority = 2              -- priority 2
                                AND stcm1.source_vocabulary_id = 'ppi-ethnicity'
                            LEFT JOIN voc.concept vc1
                                ON stcm1.target_concept_id = vc1.concept_id
                                AND vc1.standard_concept = 'S'
                                AND vc1.invalid_reason IS NULL
                            WHERE
                                NOT EXISTS (SELECT * FROM cdm.src_ethnicity g
                                            WHERE src_m.participant_id = g.person_id)
                            GROUP BY src_m.participant_id
                            HAVING
                                COUNT(distinct src_m.value_ppi_code) = 1
                                """)
        # Assembles person's birthday, gender, racial, ethnicity and location information altogether from 'src_mapped',
        # 'src_gender', 'src_race', 'src_ethnicity', 'src_person_location' relations.
        session.execute("""
        DROP TABLE IF EXISTS cdm.tmp_person;
        CREATE TABLE cdm.tmp_person LIKE cdm.person;
        ALTER TABLE cdm.tmp_person DROP COLUMN id;

        INSERT INTO cdm.tmp_person
                            SELECT DISTINCT
                                src_m.participant_id                        AS person_id,
                                COALESCE(g.gender_target_concept_id, 0)     AS gender_concept_id,
                                YEAR(b.date_of_birth)                       AS year_of_birth,
                                MONTH(b.date_of_birth)                      AS month_of_birth,
                                DAY(b.date_of_birth)                        AS day_of_birth,
                                TIMESTAMP(b.date_of_birth)                  AS birth_datetime,
                                COALESCE(r.race_target_concept_id, 0)       AS race_concept_id,
                                COALESCE(e.ethnicity_target_concept_id, 0)  AS ethnicity_concept_id,
                                person_loc.location_id                      AS location_id,
                                NULL                                        AS provider_id,
                                NULL                                        AS care_site_id,
                                src_m.participant_id                        AS person_source_value,
                                g.ppi_code                                  AS gender_source_value,
                                COALESCE(g.gender_source_concept_id, 0)     AS gender_source_concept_id,
                                r.ppi_code                                  AS race_source_value,
                                COALESCE(r.race_source_concept_id, 0)       AS race_source_concept_id,
                                e.ppi_code                                  AS ethnicity_source_value,
                                COALESCE(e.ethnicity_source_concept_id, 0) AS ethnicity_source_concept_id,
                                'person'                                    AS unit_id,
                                b.src_id                                    AS src_id
                            FROM cdm.src_mapped src_m
                            INNER JOIN cdm.src_participant b
                                ON src_m.participant_id = b.participant_id
                            LEFT JOIN cdm.src_gender g
                                ON src_m.participant_id = g.person_id
                            LEFT JOIN cdm.src_race r
                                ON src_m.participant_id = r.person_id
                            LEFT JOIN cdm.src_ethnicity e
                                ON src_m.participant_id = e.person_id
                            LEFT JOIN cdm.src_person_location person_loc
                                ON src_m.participant_id = person_loc.participant_id;
                            ;
                            """)
        session.execute("""SET @row_number = 0;
                            INSERT INTO cdm.person
                            SELECT
                              (@row_number:=@row_number + 1)              AS id,
                              cdm.tmp_person.*
                            FROM cdm.tmp_person;

                            DROP TABLE cdm.tmp_person;
                            """)

    @staticmethod
    def _populate_measurements(session, cutoff_date: Union[None, datetime], include_onsite: bool = True,
                               include_remote: bool = True):
        cutoff_filter = ''
        if cutoff_date:
            cutoff_filter = f"AND pm.finalized < '{cutoff_date.strftime('%Y-%m-%d')}'"

        if include_onsite and include_remote:
            collect_type_filter = ""
        elif include_onsite:
            collect_type_filter = "AND (pm.collect_type <> 2 OR pm.collect_type IS NULL)"
        elif include_remote:
            collect_type_filter = "AND pm.collect_type = 2"

        session.execute(f"""INSERT INTO cdm.src_meas
                            SELECT
                                0                               AS id,
                                pm.participant_id               AS participant_id,
                                pm.finalized_site_id            AS finalized_site_id,
                                meas.code_value                 AS code_value,
                                meas.measurement_time           AS measurement_time,
                                meas.value_decimal              AS value_decimal,
                                meas.value_unit                 AS value_unit,
                                meas.value_code_value           AS value_code_value,
                                LEFT(meas.value_string, 1024)   AS value_string,
                                meas.measurement_id             AS measurement_id,
                                pm.physical_measurements_id     AS physical_measurements_id,
                                meas.parent_id                  AS parent_id,
                                pm.origin                       AS src_id,
                                pm.collect_type                 AS collect_type
                            FROM rdr.measurement meas
                            INNER JOIN rdr.physical_measurements pm
                                ON meas.physical_measurements_id = pm.physical_measurements_id
                                AND pm.final = 1
                                {collect_type_filter}
                                AND (pm.status <> 2 OR pm.status IS NULL) {cutoff_filter}
                            INNER JOIN cdm.person pe
                                ON pe.person_id = pm.participant_id
                            ;
                            """)
        session.execute("""ALTER TABLE cdm.src_meas ADD KEY (code_value);
                            ALTER TABLE cdm.src_meas ADD KEY (physical_measurements_id);
                            """)
        session.execute("""INSERT INTO cdm.tmp_cv_concept_lk
                            SELECT DISTINCT
                                meas.code_value                                 AS code_value,
                                vc1.concept_id                                  AS cv_source_concept_id,
                                vc2.concept_id                                  AS cv_concept_id,
                                COALESCE(vc2.domain_id, vc1.domain_id)          AS cv_domain_id
                            FROM cdm.src_meas meas
                            LEFT JOIN voc.concept vc1
                                ON meas.code_value = vc1.concept_code
                                AND vc1.vocabulary_id = 'PPI'
                            LEFT JOIN voc.concept_relationship vcr1
                                ON vc1.concept_id = vcr1.concept_id_1
                                AND vcr1.relationship_id = 'Maps to'
                                AND vcr1.invalid_reason IS NULL
                            LEFT JOIN voc.concept vc2
                                ON vc2.concept_id = vcr1.concept_id_2
                                AND vc2.standard_concept = 'S'
                                AND vc2.invalid_reason IS NULL
                            WHERE
                                meas.code_value IS NOT NULL
                                    """)

        session.execute("""INSERT INTO cdm.tmp_vcv_concept_lk
                            SELECT DISTINCT
                                meas.value_code_value                           AS value_code_value,
                                vcv1.concept_id                                 AS vcv_source_concept_id,
                                vcv2.concept_id                                 AS vcv_concept_id,
                                COALESCE(vcv2.domain_id, vcv2.domain_id)        AS vcv_domain_id
                            FROM cdm.src_meas meas
                            LEFT JOIN voc.concept vcv1
                                ON meas.value_code_value = vcv1.concept_code
                                AND vcv1.vocabulary_id = 'PPI'
                            LEFT JOIN voc.concept_relationship vcrv1
                                ON vcv1.concept_id = vcrv1.concept_id_1
                                AND vcrv1.relationship_id = 'Maps to'
                                AND vcrv1.invalid_reason IS NULL
                            LEFT JOIN voc.concept vcv2
                                ON vcv2.concept_id = vcrv1.concept_id_2
                                AND vcv2.standard_concept = 'S'
                                AND vcv2.invalid_reason IS NULL
                            WHERE
                                meas.value_code_value IS NOT NULL
                                    """)
        session.execute(f"""INSERT INTO cdm.src_meas_mapped
                            SELECT
                                0                                           AS id,
                                meas.participant_id                         AS participant_id,
                                meas.finalized_site_id                      AS finalized_site_id,
                                meas.code_value                             AS code_value,
                                COALESCE(tmp1.cv_source_concept_id, 0)      AS cv_source_concept_id,
                                COALESCE(tmp1.cv_concept_id, 0)             AS cv_concept_id,
                                tmp1.cv_domain_id                           AS cv_domain_id,
                                meas.measurement_time                       AS measurement_time,
                                meas.value_decimal                          AS value_decimal,
                                meas.value_unit                             AS value_unit,
                                COALESCE(vc1.concept_id, 0)                 AS vu_concept_id,
                                meas.value_code_value                       AS value_code_value,
                                COALESCE(tmp2.vcv_source_concept_id, 0)     AS vcv_source_concept_id,
                                COALESCE(tmp2.vcv_concept_id, 0)            AS vcv_concept_id,
                                meas.measurement_id                         AS measurement_id,
                                meas.physical_measurements_id               AS physical_measurements_id,
                                meas.parent_id                              AS parent_id,
                                meas.src_id                                 AS src_id,
                                meas.collect_type                           AS collect_type
                            FROM cdm.src_meas meas
                            LEFT JOIN cdm.tmp_cv_concept_lk tmp1
                                ON meas.code_value = tmp1.code_value
                            LEFT JOIN voc.concept vc1           -- here we map units of measurements to standard concepts
                                ON meas.value_unit = vc1.concept_code
                                AND vc1.vocabulary_id = 'UCUM'
                                AND vc1.standard_concept = 'S'
                                AND vc1.invalid_reason IS NULL
                            LEFT JOIN cdm.tmp_vcv_concept_lk tmp2
                                ON meas.value_code_value = tmp2.value_code_value
                            WHERE
                                meas.code_value <> 'notes'
                                    """)
        session.execute("""alter table cdm.src_meas_mapped add key (physical_measurements_id);
                           alter table cdm.src_meas_mapped add key (measurement_id);
                           CREATE INDEX src_meas_pm_ids ON cdm.src_meas_mapped
                                        (physical_measurements_id, measurement_id);
                        """)

        session.execute("""DROP TABLE IF EXISTS cdm.tmp_care_site;
                            CREATE TABLE cdm.tmp_care_site LIKE cdm.care_site;
                            ALTER TABLE cdm.tmp_care_site DROP COLUMN id;
                        """)
        session.execute("""INSERT INTO cdm.tmp_care_site
                            SELECT DISTINCT
                                site.site_id                            AS care_site_id,
                                site.site_name                          AS care_site_name,
                                0                                       AS place_of_service_concept_id,
                                NULL                                    AS location_id,
                                site.site_id                            AS care_site_source_value,
                                NULL                                    AS place_of_service_source_value,
                                'care_site'                             AS unit_id,
                                ''                                      AS src_id
                            FROM rdr.site site
                            """)
        session.execute("""SET @row_number = 0;
                            INSERT INTO cdm.care_site
                            SELECT
                            (@row_number:=@row_number + 1)              AS id,
                              cdm.tmp_care_site.*
                            FROM cdm.tmp_care_site;
                        """)
        session.execute("""DROP TABLE IF EXISTS cdm.tmp_care_site""")
        session.execute("""INSERT INTO cdm.tmp_visits_src
                            SELECT
                                src_meas.physical_measurements_id       AS visit_occurrence_id,
                                src_meas.participant_id                 AS person_id,
                                MIN(src_meas.measurement_time)          AS visit_start_datetime,
                                MAX(src_meas.measurement_time)          AS visit_end_datetime,
                                src_meas.finalized_site_id              AS care_site_id,
                                src_meas.src_id                         AS src_id
                            FROM cdm.src_meas src_meas
                            GROUP BY
                                src_meas.physical_measurements_id,
                                src_meas.participant_id,
                                src_meas.finalized_site_id,
                                src_meas.src_id
                        """)
        session.execute("""SET @row_number = 0;
                            INSERT INTO cdm.visit_occurrence
                            SELECT
                                (@row_number:=@row_number + 1)          AS id,
                                src.visit_occurrence_id                 AS visit_occurrence_id,
                                src.person_id                           AS person_id,
                                9202                                    AS visit_concept_id, -- 9202 - 'Outpatient Visit'
                                DATE(src.visit_start_datetime)          AS visit_start_date,
                                src.visit_start_datetime                AS visit_start_datetime,
                                DATE(src.visit_end_datetime)            AS visit_end_date,
                                src.visit_end_datetime                  AS visit_end_datetime,
                                44818519                                AS visit_type_concept_id, -- 44818519 - 'Clinical Study Visit'
                                NULL                                    AS provider_id,
                                src.care_site_id                        AS care_site_id,
                                src.visit_occurrence_id                 AS visit_source_value,
                                0                                       AS visit_source_concept_id,
                                0                                       AS admitting_source_concept_id,
                                NULL                                    AS admitting_source_value,
                                0                                       AS discharge_to_concept_id,
                                NULL                                    AS discharge_to_source_value,
                                NULL                                    AS preceding_visit_occurrence_id,
                                'vis.meas'                              AS unit_id,
                                src.src_id                              AS src_id
                            FROM cdm.tmp_visits_src src
                        """)

        # unit: observ.meas - observations from measurement table
        session.execute("""
        INSERT INTO cdm.observation
                            SELECT
                                NULL                                    AS observation_id,
                                meas.participant_id                     AS person_id,
                                meas.cv_concept_id                      AS observation_concept_id,
                                DATE(meas.measurement_time)             AS observation_date,
                                meas.measurement_time                   AS observation_datetime,
                                581413                                  AS observation_type_concept_id,   -- 581413, Observation from Measurement
                                NULL                                    AS value_as_number,
                                NULL                                    AS value_as_string,
                                meas.vcv_concept_id                     AS value_as_concept_id,
                                0                                       AS qualifier_concept_id,
                                meas.vu_concept_id                      AS unit_concept_id,
                                NULL                                    AS provider_id,
                                meas.physical_measurements_id           AS visit_occurrence_id,
                                NULL                                    AS visit_detail_id,
                                meas.code_value                         AS observation_source_value,
                                meas.cv_source_concept_id               AS observation_source_concept_id,
                                meas.value_unit                         AS unit_source_value,
                                NULL                                    AS qualifier_source_value,
                                meas.vcv_source_concept_id              AS value_source_concept_id,
                                meas.value_code_value                   AS value_source_value,
                                NULL                                    AS questionnaire_response_id,
                                meas.measurement_id                     AS meas_id,
                                'observ.meas'                           AS unit_id,
                                meas.src_id                             AS src_id
                            FROM cdm.src_meas_mapped meas
                            WHERE
                                meas.cv_domain_id = 'Observation'
                                    """)
        session.execute("""ALTER TABLE cdm.observation ADD KEY (meas_id)""")
        # -- unit: meas.dec   - measurements represented as decimal values
        # -- unit: meas.value - measurements represented as value_code_value
        # -- unit: meas.empty - measurements with empty value_decimal and value_code_value fields
        # -- 'measurement' table is filled from src_meas_mapped table only.
        session.execute("""SET @row_number = 0;
                                  INSERT INTO cdm.measurement
                                  SELECT
                                      (@row_number:=@row_number + 1)          AS id,
                                      meas.measurement_id                     AS measurement_id,
                                      meas.participant_id                     AS person_id,
                                      meas.cv_concept_id                      AS measurement_concept_id,
                                      DATE(meas.measurement_time)             AS measurement_date,
                                      meas.measurement_time                   AS measurement_datetime,
                                      NULL                                    AS measurement_time,
                                      IF(meas.collect_type <> 2 OR meas.collect_type IS NULL, 44818701, 32865)
                                                                              AS measurement_type_concept_id, -- 44818701, From physical examination. 32865, Patient self-report
                                      0                                       AS operator_concept_id,
                                      meas.value_decimal                      AS value_as_number,
                                      meas.vcv_concept_id                     AS value_as_concept_id,
                                      meas.vu_concept_id                      AS unit_concept_id,
                                      NULL                                    AS range_low,
                                      NULL                                    AS range_high,
                                      NULL                                    AS provider_id,
                                      meas.physical_measurements_id           AS visit_occurrence_id,
                                      NULL                                    AS visit_detail_id,
                                      meas.code_value                         AS measurement_source_value,
                                      meas.cv_source_concept_id               AS measurement_source_concept_id,
                                      meas.value_unit                         AS unit_source_value,
                                      CASE
                                          WHEN meas.value_decimal IS NOT NULL OR meas.value_unit IS NOT NULL
                                              THEN CONCAT(COALESCE(meas.value_decimal, ''), ' ',
                                                  COALESCE(meas.value_unit, ''))     -- 'meas.dec'
                                          WHEN meas.value_code_value IS NOT NULL
                                              THEN meas.value_code_value             -- 'meas.value'
                                          ELSE NULL                                  -- 'meas.empty'
                                      END                                     AS value_source_value,
                                      meas.parent_id                          AS parent_id,
                                      CASE
                                          WHEN meas.value_decimal IS NOT NULL OR meas.value_unit IS NOT NULL
                                              THEN 'meas.dec'
                                          WHEN meas.value_code_value IS NOT NULL
                                              THEN 'meas.value'
                                          ELSE 'meas.empty'
                                      END                                     AS unit_id,
                                      meas.src_id                             AS src_id
                                  FROM cdm.src_meas_mapped meas
                                  WHERE
                                      meas.cv_domain_id = 'Measurement' OR meas.cv_domain_id IS NULL
                                          """)
        session.execute("""CREATE INDEX measurement_idx
                                  ON cdm.measurement (person_id, measurement_date, measurement_datetime, parent_id)
                              """)
        session.execute("""SET @row_number = 0;
                                  INSERT INTO cdm.note
                                  SELECT
                                      (@row_number:=@row_number + 1)          AS id,
                                      NULL                                    AS note_id,
                                      meas.participant_id                     AS person_id,
                                      DATE(meas.measurement_time)             AS note_date,
                                      meas.measurement_time                   AS note_datetime,
                                      44814645                                AS note_type_concept_id,    -- 44814645 - 'Note'
                                      0                                       AS note_class_concept_id,
                                      NULL                                    AS note_title,
                                      COALESCE(meas.value_string, '')         AS note_text,
                                      0                                       AS encoding_concept_id,
                                      4180186                                 AS language_concept_id,     -- 4180186 - 'English language'
                                      NULL                                    AS provider_id,
                                      NULL                                    AS visit_detail_id,
                                      meas.code_value                         AS note_source_value,
                                      meas.physical_measurements_id           AS visit_occurrence_id,
                                      'note'                                  AS unit_id
                                  FROM cdm.src_meas meas
                                  WHERE
                                      meas.code_value = 'notes'
                                          """)
        # Insert to fact_relationships measurement-to-observation relations
        session.execute("""SET @row_number = 0;
                             INSERT INTO cdm.fact_relationship
                             SELECT
                                 (@row_number:=@row_number + 1)  AS id,
                                 21                              AS domain_concept_id_1,     -- Measurement
                                 mtq.measurement_id              AS fact_id_1,
                                 27                              AS domain_concept_id_2,     -- Observation
                                 cdm_obs.observation_id          AS fact_id_2,
                                 581411                          AS relationship_concept_id, -- Measurement to Observation
                                 'observ.meas1'                  AS unit_id,
                                 cdm_obs.src_id                  AS src_id
                             FROM cdm.observation cdm_obs
                             INNER JOIN rdr.measurement_to_qualifier mtq
                                 ON mtq.qualifier_id = cdm_obs.meas_id
                                     """)
        session.execute("""INSERT INTO cdm.fact_relationship
                             SELECT
                                 (@row_number:=@row_number + 1)  AS id,
                                 27                              AS domain_concept_id_1,     -- Observation
                                 cdm_obs.observation_id          AS fact_id_1,
                                 21                              AS domain_concept_id_2,     -- Measurement
                                 mtq.measurement_id              AS fact_id_2,
                                 581410                          AS relationship_concept_id, -- Observation to Measurement
                                 'observ.meas2'                  AS unit_id,
                                 cdm_obs.src_id                  AS src_id
                             FROM cdm.observation cdm_obs
                             INNER JOIN rdr.measurement_to_qualifier mtq
                                 ON mtq.qualifier_id = cdm_obs.meas_id
                         """)

        # temporary table for populating cdm_fact_relationship table from systolic and
        # diastolic blood pressure measurements
        session.execute("""INSERT INTO cdm.tmp_fact_rel_sd
                             SELECT
                                 0                                                           AS id,
                                 m.measurement_id                                            AS measurement_id,
                                 CASE
                                     WHEN m.measurement_source_value = 'blood-pressure-systolic-1'     THEN 1
                                     WHEN m.measurement_source_value = 'blood-pressure-systolic-2'     THEN 2
                                     WHEN m.measurement_source_value = 'blood-pressure-systolic-3'     THEN 3
                                     WHEN m.measurement_source_value = 'blood-pressure-systolic-mean'  THEN 4
                                     ELSE 0
                                 END                                                         AS systolic_blood_pressure_ind,
                                 CASE
                                     WHEN m.measurement_source_value = 'blood-pressure-diastolic-1'    THEN 1
                                     WHEN m.measurement_source_value = 'blood-pressure-diastolic-2'    THEN 2
                                     WHEN m.measurement_source_value = 'blood-pressure-diastolic-3'    THEN 3
                                     WHEN m.measurement_source_value = 'blood-pressure-diastolic-mean' THEN 4
                                     ELSE 0
                                 END                                                         AS diastolic_blood_pressure_ind,
                                 m.person_id                                                 AS person_id,
                                 m.parent_id                                                 AS parent_id,
                                 m.src_id                                                    AS src_id

                             FROM cdm.measurement m
                             WHERE
                                 m.measurement_source_value IN (
                                     'blood-pressure-systolic-1', 'blood-pressure-systolic-2',
                                     'blood-pressure-systolic-3', 'blood-pressure-systolic-mean',
                                     'blood-pressure-diastolic-1', 'blood-pressure-diastolic-2',
                                     'blood-pressure-diastolic-3', 'blood-pressure-diastolic-mean'
                                 )
                                 AND m.parent_id IS NOT NULL
                                     """)
        session.execute("""ALTER TABLE cdm.tmp_fact_rel_sd ADD KEY (person_id, parent_id)""")

        # -- unit: syst.diast.*[1,2] - to link systolic and diastolic blood pressure
        # -- Insert into fact_relationship table systolic to disatolic blood pressure
        # -- measurements relations
        session.execute("""INSERT INTO cdm.fact_relationship
                             SELECT
                                 (@row_number:=@row_number + 1)  AS id,
                                 21                          AS domain_concept_id_1,     -- Measurement
                                 tmp1.measurement_id         AS fact_id_1,               -- measurement_id of the first/second/third/mean systolic blood pressure
                                 21                          AS domain_concept_id_2,     -- Measurement
                                 tmp2.measurement_id         AS fact_id_2,               -- measurement_id of the first/second/third/mean diastolic blood pressure
                                 46233683                    AS relationship_concept_id, -- Systolic to diastolic blood pressure measurement
                                 CASE
                                   WHEN tmp1.systolic_blood_pressure_ind = 1 THEN 'syst.diast.first1'
                                   WHEN tmp1.systolic_blood_pressure_ind = 2 THEN 'syst.diast.second1'
                                   WHEN tmp1.systolic_blood_pressure_ind = 3 THEN 'syst.diast.third1'
                                   WHEN tmp1.systolic_blood_pressure_ind = 4 THEN 'syst.diast.mean1'
                                 END                         AS unit_id,
                                 tmp1.src_id                 AS src_id
                             FROM cdm.tmp_fact_rel_sd tmp1
                             INNER JOIN cdm.tmp_fact_rel_sd tmp2
                                 ON tmp1.person_id = tmp2.person_id
                                 AND tmp1.parent_id = tmp2.parent_id
                                 AND tmp1.systolic_blood_pressure_ind = tmp2.diastolic_blood_pressure_ind   -- get the same index to refer between
                                                                                                            -- first, second, third and mean blood pressure measurements
                             WHERE tmp1.systolic_blood_pressure_ind != 0              -- take only systolic blood pressure measurements
                                 AND tmp2.diastolic_blood_pressure_ind != 0             -- take only diastolic blood pressure measurements
                                     """)
        # -- Insert into fact_relationship diastolic to systolic blood pressure
        # -- measurements relation
        session.execute("""INSERT INTO cdm.fact_relationship
                             SELECT
                                 (@row_number:=@row_number + 1)  AS id,
                                 21                          AS domain_concept_id_1,     -- Measurement
                                 tmp2.measurement_id         AS fact_id_1,               -- measurement_id of the first/second/third/mean diastolic blood pressure
                                 21                          AS domain_concept_id_2,     -- Measurement
                                 tmp1.measurement_id         AS fact_id_2,               -- measurement_id of the first/second/third/mean systolic blood pressure
                                 46233682                    AS relationship_concept_id, -- Diastolic to systolic blood pressure measurement
                                 CASE
                                   WHEN tmp1.systolic_blood_pressure_ind = 1 THEN 'syst.diast.first2'
                                   WHEN tmp1.systolic_blood_pressure_ind = 2 THEN 'syst.diast.second2'
                                   WHEN tmp1.systolic_blood_pressure_ind = 3 THEN 'syst.diast.third2'
                                   WHEN tmp1.systolic_blood_pressure_ind = 4 THEN 'syst.diast.mean2'
                                 END                         AS unit_id,
                                 tmp1.src_id                 AS src_id
                             FROM cdm.tmp_fact_rel_sd tmp1
                             INNER JOIN cdm.tmp_fact_rel_sd tmp2
                                 ON tmp1.person_id = tmp2.person_id
                                 AND tmp1.parent_id = tmp2.parent_id
                                 AND tmp1.systolic_blood_pressure_ind = tmp2.diastolic_blood_pressure_ind   -- get the same index to refer between
                                                                                                            -- first, second, third and mean blood pressurre measurements
                             WHERE tmp1.systolic_blood_pressure_ind != 0              -- take only systolic blood pressure measurements
                                 AND tmp2.diastolic_blood_pressure_ind != 0             -- take only diastolic blood pressure measurements
                                     """)

        # Insert into fact_relationship child-to-parent measurements relations
        session.execute("""INSERT INTO cdm.fact_relationship
                             SELECT
                                 (@row_number:=@row_number + 1)  AS id,
                                 21                              AS domain_concept_id_1,     -- Measurement
                                 cdm_meas.measurement_id         AS fact_id_1,
                                 21                              AS domain_concept_id_2,     -- Measurement
                                 cdm_meas.parent_id              AS fact_id_2,
                                 581437                          AS relationship_concept_id, -- 581437, Child to Parent Measurement
                                 'meas.meas1'                    AS unit_id,
                                 cdm_meas.src_id                 AS src_id
                             FROM cdm.measurement cdm_meas
                             WHERE cdm_meas.parent_id IS NOT NULL
                                     """)
        session.execute("""INSERT INTO cdm.fact_relationship
                             SELECT
                                 (@row_number:=@row_number + 1)  AS id,
                                 21                              AS domain_concept_id_1,     -- Measurement
                                 cdm_meas.parent_id              AS fact_id_1,
                                 21                              AS domain_concept_id_2,     -- Measurement
                                 cdm_meas.measurement_id         AS fact_id_2,
                                 581436                          AS relationship_concept_id, -- 581436, Parent to Child Measurement
                                 'meas.meas2'                    AS unit_id,
                                 cdm_meas.src_id                 AS src_id
                             FROM cdm.measurement cdm_meas
                             WHERE cdm_meas.parent_id IS NOT NULL
                                     """)

    def _populate_observation_surveys(self, session, pid_list:List[int]):
        # -- units: observ.code, observ.str, observ.num, observ.bool
        # -- 'observation' table consists of 2 parts:
        # -- 1) patient's questionnaires
        # -- 2) patient's observations from measurements
        # -- First part we fill from 'src_mapped', second -
        # -- from 'src_meas_mapped'

        session.execute(f"""INSERT INTO cdm.observation
                            SELECT
                                NULL                                        AS observation_id,
                                src_m.participant_id                        AS person_id,
                                src_m.question_concept_id                   AS observation_concept_id,
                                DATE(src_m.date_of_survey)                  AS observation_date,
                                src_m.date_of_survey                        AS observation_datetime,
                                45905771                                    AS observation_type_concept_id, -- 45905771, Observation Recorded from a Survey
                                src_m.value_number                          AS value_as_number,
                                CASE
                                    WHEN src_m.value_ppi_code IS NOT NULL
                                         AND  src_m.value_concept_id = 0    THEN src_m.value_string
                                    WHEN src_m.value_string IS NOT NULL
                                         AND src_m.value_ppi_code IS NULL   THEN src_m.value_string
                                    ELSE NULL
                                END                                         AS value_as_string,
                                CASE
                                    WHEN src_m.value_ppi_code IS NOT NULL THEN src_m.value_concept_id
                                    WHEN src_m.value_boolean IS NOT NULL THEN src_m.value_boolean_concept_id
                                    ELSE 0
                                END                                         AS value_as_concept_id,
                                0                                           AS qualifier_concept_id,
                                0                                           AS unit_concept_id,
                                NULL                                        AS provider_id,
                                NULL                                        AS visit_occurrence_id,
                                NULL                                        AS visit_detail_id,
                                src_m.question_ppi_code                     AS observation_source_value,
                                src_m.question_source_concept_id            AS observation_source_concept_id,
                                NULL                                        AS unit_source_value,
                                NULL                                        AS qualifier_source_value,
                                src_m.value_source_concept_id               AS value_source_concept_id,
                                src_m.value_ppi_code                        AS value_source_value,
                                src_m.questionnaire_response_id             AS questionnaire_response_id,
                                NULL                                        AS meas_id,
                                CASE
                                    WHEN src_m.value_ppi_code IS NOT NULL       THEN 'observ.code'
                                    WHEN src_m.value_ppi_code IS NULL
                                         AND src_m.value_string IS NOT NULL     THEN 'observ.str'
                                    WHEN src_m.value_number IS NOT NULL         THEN 'observ.num'
                                    WHEN src_m.value_boolean IS NOT NULL        THEN 'observ.bool'
                                    WHEN src_m.is_invalid                       THEN 'observ.invalid'
                                END                                         AS unit_id,
                                src_m.src_id                                AS src_id
                            FROM cdm.src_mapped src_m
                            WHERE src_m.question_ppi_code is not null
                            AND src_m.participant_id IN ({",".join([str(pid) for pid in pid_list])})
                                    """)

        # remove special character
        session.execute("""update cdm.observation set value_as_string = replace(value_as_string, '\0', '')
                           where value_as_string like '%\0%'""")

    @staticmethod
    def _populate_questionnaire_response_additional_info(session):
        # Preventing locks on questionnaire_response table when reading data
        session.execute("""SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED""")

        session.execute("""INSERT INTO cdm.questionnaire_response_additional_info SELECT DISTINCT
                            0 AS id,
                            qr.questionnaire_response_id, 'NON_PARTICIPANT_AUTHOR_INDICATOR' as type, qr.non_participant_author as value,
                            p.participant_origin src_id
                            from rdr.questionnaire_response qr
                            JOIN (SELECT DISTINCT questionnaire_response_id from cdm.src_clean) as qri ON qr.questionnaire_response_id = qri.questionnaire_response_id
                            JOIN rdr.participant p ON qr.participant_id = p.participant_id
                            where qr.non_participant_author is not null and qr.questionnaire_response_id=qri.questionnaire_response_id
                                    """)
        session.execute("""INSERT INTO cdm.questionnaire_response_additional_info SELECT DISTINCT
                            0 AS id,
                            qr.questionnaire_response_id, 'LANGUAGE' as type, qr.language as value, p.participant_origin src_id
                            from rdr.questionnaire_response qr
                            JOIN (SELECT DISTINCT questionnaire_response_id from cdm.src_clean) as qri ON qr.questionnaire_response_id = qri.questionnaire_response_id
                            JOIN rdr.participant p ON qr.participant_id = p.participant_id
                            where qr.language is not null and qr.questionnaire_response_id=qri.questionnaire_response_id
                                    """)
        session.execute("""INSERT INTO cdm.questionnaire_response_additional_info SELECT DISTINCT
                            0 AS id,
                            qr.questionnaire_response_id, 'CODE' as type, c.value as value, p.participant_origin src_id
                            from rdr.questionnaire_response qr
                            JOIN rdr.questionnaire_concept qc ON qr.questionnaire_id = qc.questionnaire_id AND qr.questionnaire_version = qc.questionnaire_version
                            JOIN rdr.code c ON qc.code_id = c.code_id
                            JOIN (SELECT DISTINCT questionnaire_response_id from cdm.src_clean) as qri ON qr.questionnaire_response_id = qri.questionnaire_response_id
                            JOIN rdr.participant p ON qr.participant_id = p.participant_id
                            where qr.questionnaire_id=qc.questionnaire_id
                            and qc.code_id=c.code_id
                            and qr.questionnaire_response_id=qri.questionnaire_response_id
                                    """)
        # Reset ISOLATION level to previous setting (assuming here that it was MySql's default)
        session.execute("""SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ""")


def add_additional_arguments(parser):
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa

    subparsers = parser.add_subparsers(dest='command')

    export_parser = subparsers.add_parser('export')
    export_parser.add_argument("--export-path", help="Bucket path to export to", required=True, type=str)  # noqa
    export_parser.add_argument("--table", help="Export a specific table", type=str, default=None)  # noqa

    cdm_parser = subparsers.add_parser('cdm-data')
    cdm_parser.add_argument("--cutoff", help="populate cdm with cut off date, example: 2022-04-01",
                            type=str, default=None)  # noqa
    cdm_parser.add_argument("--vocabulary", help="the path of the vocabulary of this run, "
                                                 "example: gs://curation-vocabulary/aou_vocab_20220201/",
                            type=str, default=None)  # noqa
    cdm_parser.add_argument("--participant-origin",
                            help="Participant origin for run, accepts vibrent, careevolution, or all",
                            type=str, default=None)
    cdm_parser.add_argument("--participant-list-file",
                            help="Path to a file containing a list of PIDs to run the ETL process with",
                            type=str, default=None)
    cdm_parser.add_argument("--include-surveys", help="Path to a file containing list of survey names to include",
                            type=str, default=None)
    cdm_parser.add_argument("--exclude-surveys", help="Path to a file containing list of survey names to exclude",
                            type=str, default=None)
    cdm_parser.add_argument("--omit-surveys", help="Observation table won't include survey data",
                            action="store_true", default=False)
    cdm_parser.add_argument("--omit-measurements", help="Observation table won't include physical measurements",
                            action="store_true", default=False)
    cdm_parser.add_argument("--exclude-participants", help="Path to a file containing a list of PIDs to exclude",
                            type=str, default=None)
    cdm_parser.add_argument("--exclude-in-person-pm", help="Excludes in-person physical measurements",
                            action="store_true", default=False)
    cdm_parser.add_argument("--exclude-remote-pm", help="Excludes remote physical measurements",
                            action="store_true", default=False)

    manage_code_parser = subparsers.add_parser('exclude-code')
    manage_code_parser.add_argument("--operation", help="operation type for exclude code command: add or remove",
                                    type=str, default=None)  # noqa
    manage_code_parser.add_argument("--code-value", help="code values, split by comma", type=str, default=None)  # noqa
    manage_code_parser.add_argument("--code-type", help="code type: module, question or answer",
                                    type=str, default=None)  # noqa


def run():
    cli_run(tool_cmd, tool_desc, CurationExportClass, add_additional_arguments)
