from datetime import datetime
import json

from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase, logger

tool_cmd = 'answer-hash-backfill'
tool_desc = 'Backfill the answer digest for responses'


class DigestBackfillTool(ToolBase):
    def run(self):
        super(DigestBackfillTool, self).run()

        latest_id = -10
        with self.get_session() as session:
            found_responses = True
            while found_responses:
                found_responses = False
                questionnaire_response_query = session.query(
                    QuestionnaireResponse
                ).filter(
                    QuestionnaireResponse.questionnaireResponseId > latest_id,
                    QuestionnaireResponse.answerHash.is_(None)
                ).order_by(QuestionnaireResponse.questionnaireResponseId).limit(2500)

                for response in questionnaire_response_query:
                    found_responses = True

                    answer_hash = QuestionnaireResponseDao.calculate_answer_hash(json.loads(response.resource))
                    response.answerHash = answer_hash

                    latest_id = response.questionnaireResponseId

                if found_responses:
                    logger.info(f'got to {latest_id}')
                    logger.info(datetime.now())
                    logger.info('committing')
                    session.commit()


def run():
    return cli_run(tool_cmd, tool_desc, DigestBackfillTool)
