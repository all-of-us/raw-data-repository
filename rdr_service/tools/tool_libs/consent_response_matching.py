import logging

from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.consent_file import ConsentFile, ConsentType
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'consent-match'
tool_desc = 'find the response for consents that do not have a response set'


class ConsentMatchScript(ToolBase):
    def run(self):
        super(ConsentMatchScript, self).run()

        with self.get_session() as session:
            unmatched_wear_consents = session.query(ConsentFile).filter(
                ConsentFile.type == ConsentType.WEAR,
                ConsentFile.consent_response_id.is_(None)
            ).all()

            wear_responses = QuestionnaireResponseDao.get_responses_to_surveys(
                session=session,
                survey_codes=['wear_consent'],
                participant_ids=[file.participant_id for file in unmatched_wear_consents]
            )

            for file in unmatched_wear_consents:
                if file.participant_id not in wear_responses:
                    logging.error(f'response for {file.participant_id} not found')
                else:
                    response_list = wear_responses[file.participant_id].responses
                    if len(response_list) != 1:
                        logging.warning(f'{len(response_list)} responses found for {file.participant_id}')
                    else:
                        response = response_list[0]
                        file.response = response


def run():
    return cli_run(tool_cmd, tool_desc, ConsentMatchScript)
