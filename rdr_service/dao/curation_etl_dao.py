from sqlalchemy import insert, case
from sqlalchemy.sql import literal
from rdr_service import clock
from rdr_service.dao.base_dao import UpdatableDao, BaseDao
from rdr_service.model.curation_etl import CdrEtlRunHistory, CdrEtlSurveyHistory, CdrExcludedCode
from rdr_service.dao.code_dao import CodeDao
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.participant_enums import CdrEtlCodeType, CdrEtlSurveyStatus
from rdr_service.etl.model.src_clean import SrcClean


class CdrEtlRunHistoryDao(UpdatableDao):
    def __init__(self):
        super(CdrEtlRunHistoryDao, self).__init__(CdrEtlRunHistory)

    def create_etl_history_record(self, session, cutoff, vocabulary):
        now = clock.CLOCK.now()
        cdr_etl_run_history = CdrEtlRunHistory(
            startTime=now,
            vocabularyPath=vocabulary,
            cutoffDate=cutoff
        )
        return self.insert_with_session(session, cdr_etl_run_history)

    def update_etl_end_time(self, session, etl_history_id):
        now = clock.CLOCK.now()
        record = self.get_for_update(session, etl_history_id)
        record.endTime = now

    def get_last_etl_run_info(self, session, is_sql=False):
        query = session.query(
            CdrEtlRunHistory.id,
            CdrEtlRunHistory.vocabularyPath,
            CdrEtlRunHistory.cutoffDate,
            CdrEtlRunHistory.startTime,
            CdrEtlRunHistory.endTime
        ).order_by(CdrEtlRunHistory.id.desc()).limit(1)

        if is_sql:
            sql = self.literal_sql_from_query(query)
            return sql

        return query.first()


class CdrExcludedCodeDao(BaseDao):
    def __init__(self):
        super(CdrExcludedCodeDao, self).__init__(CdrExcludedCode)

    def is_exist_exclude_code(self, session, code_value, code_type):
        exist_item = session.query(CdrExcludedCode).filter(CdrExcludedCode.codeValue == code_value,
                                                           CdrExcludedCode.codeType == code_type).all()
        return True if exist_item else False

    def add_excluded_code(self, session, code_value, code_type):
        if code_type not in [item for item in CdrEtlCodeType]:
            raise TypeError(f'unrecognized code type: {str(code_type)}')
        code_dao = CodeDao()
        code = code_dao.get_code_with_session(session, PPI_SYSTEM, code_value)
        if not code:
            raise ValueError(f'invalid code value: {str(code_value)}')
        if not self.is_exist_exclude_code(session, code_value, code_type):
            cdr_excluded_code = CdrExcludedCode(
                codeId=code.codeId,
                codeValue=code_value,
                codeType=code_type
            )
            return self.insert_with_session(session, cdr_excluded_code)

    def remove_excluded_code(self, session, code_value, code_type):
        code_dao = CodeDao()
        code = code_dao.get_code_with_session(session, PPI_SYSTEM, code_value)
        if not code:
            raise ValueError(f'invalid code value: {str(code_value)}')
        if self.is_exist_exclude_code(session, code_value, code_type):
            remove_list = session.query(CdrExcludedCode).filter(CdrExcludedCode.codeId == code.codeId,
                                                                CdrExcludedCode.codeType == code_type).all()
            for item in remove_list:
                session.delete(item)


class CdrEtlSurveyHistoryDao(BaseDao):
    def __init__(self):
        super(CdrEtlSurveyHistoryDao, self).__init__(CdrEtlSurveyHistory)

    def get_last_etl_run_code_history(self, session, is_sql=False):
        run_history_dao = CdrEtlRunHistoryDao()
        etl_run_info = run_history_dao.get_last_etl_run_info(session)
        query = session.query(
            CdrEtlSurveyHistory.etlRunId,
            case(
                [
                    (CdrEtlSurveyHistory.status == CdrEtlSurveyStatus.INCLUDE, 'INCLUDE'),
                    (CdrEtlSurveyHistory.status == CdrEtlSurveyStatus.EXCLUDE, 'EXCLUDE')
                ], ).label('status'),
            CdrEtlSurveyHistory.codeValue,
            case(
                [
                    (CdrEtlSurveyHistory.codeType == CdrEtlCodeType.MODULE, 'MODULE'),
                    (CdrEtlSurveyHistory.codeType == CdrEtlCodeType.QUESTION, 'QUESTION'),
                    (CdrEtlSurveyHistory.codeType == CdrEtlCodeType.ANSWER, 'ANSWER')
                ], ).label('codeType'),
        ).filter(CdrEtlSurveyHistory.etlRunId == etl_run_info.id)

        if is_sql:
            sql = self.literal_sql_from_query(query)
            return sql

        return query.all()

    def save_include_exclude_code_history_for_etl_run(self, session, run_id):
        now = clock.CLOCK.now()
        # excluded codes
        column_map = {
            CdrEtlSurveyHistory.created: literal(now),
            CdrEtlSurveyHistory.modified: literal(now),
            CdrEtlSurveyHistory.etlRunId: literal(run_id),
            CdrEtlSurveyHistory.codeId: CdrExcludedCode.codeId,
            CdrEtlSurveyHistory.codeValue: CdrExcludedCode.codeValue,
            CdrEtlSurveyHistory.codeType: CdrExcludedCode.codeType,
            CdrEtlSurveyHistory.status: literal(int(CdrEtlSurveyStatus.EXCLUDE))
        }
        select_excluded_query = session.query(*column_map.values())
        insert_query = insert(CdrEtlSurveyHistory).from_select(column_map.keys(), select_excluded_query)
        session.execute(insert_query)

        # included module codes
        SrcClean.__table__.schema = 'cdm'
        column_map = {
            CdrEtlSurveyHistory.created: literal(now),
            CdrEtlSurveyHistory.modified: literal(now),
            CdrEtlSurveyHistory.etlRunId: literal(run_id),
            CdrEtlSurveyHistory.codeId: literal(None),
            CdrEtlSurveyHistory.codeValue: SrcClean.survey_name,
            CdrEtlSurveyHistory.codeType: literal(int(CdrEtlCodeType.MODULE)),
            CdrEtlSurveyHistory.status: literal(int(CdrEtlSurveyStatus.INCLUDE))
        }

        select_module_code_query = session.query(*column_map.values()).distinct()
        insert_query = insert(CdrEtlSurveyHistory).from_select(column_map.keys(), select_module_code_query)
        session.execute(insert_query)
        # included question codes
        column_map = {
            CdrEtlSurveyHistory.created: literal(now),
            CdrEtlSurveyHistory.modified: literal(now),
            CdrEtlSurveyHistory.etlRunId: literal(run_id),
            CdrEtlSurveyHistory.codeId: SrcClean.question_code_id,
            CdrEtlSurveyHistory.codeValue: SrcClean.question_ppi_code,
            CdrEtlSurveyHistory.codeType: literal(int(CdrEtlCodeType.QUESTION)),
            CdrEtlSurveyHistory.status: literal(int(CdrEtlSurveyStatus.INCLUDE))
        }

        select_question_code_query = session.query(*column_map.values()).distinct()
        insert_query = insert(CdrEtlSurveyHistory).from_select(column_map.keys(), select_question_code_query)
        session.execute(insert_query)
        # included answer codes
        column_map = {
            CdrEtlSurveyHistory.created: literal(now),
            CdrEtlSurveyHistory.modified: literal(now),
            CdrEtlSurveyHistory.etlRunId: literal(run_id),
            CdrEtlSurveyHistory.codeId: SrcClean.value_code_id,
            CdrEtlSurveyHistory.codeValue: SrcClean.value_ppi_code,
            CdrEtlSurveyHistory.codeType: literal(int(CdrEtlCodeType.ANSWER)),
            CdrEtlSurveyHistory.status: literal(int(CdrEtlSurveyStatus.INCLUDE))
        }

        select_answer_code_query = session.query(*column_map.values()).distinct()
        insert_query = insert(CdrEtlSurveyHistory).from_select(column_map.keys(), select_answer_code_query)
        session.execute(insert_query)


