import json

from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.lib_fhir.fhirclient_1_0_6.models import questionnaireresponse as fhir_questionnaireresponse
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseExtension
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'response-extension-backfill'
tool_desc = 'Backfill the extensions table for responses'


class ExtensionBackfillTool(ToolBase):
    def run(self):
        super(ExtensionBackfillTool, self).run()

        with self.get_session() as session:
            found_responses = True
            while found_responses:
                questionnaire_response_query = session.query(
                    QuestionnaireResponse.questionnaireResponseId,
                    QuestionnaireResponse.created,
                    QuestionnaireResponse.resource
                ).join(
                    QuestionnaireResponseExtension,
                    isouter=True
                ).filter(
                    QuestionnaireResponseExtension.id.is_(None)
                ).order_by(QuestionnaireResponse.created).limit(500)

                found_responses = False
                latest_date = None
                for response_data in questionnaire_response_query:
                    found_responses = True

                    fhir_qr = fhir_questionnaireresponse.QuestionnaireResponse(json.loads(response_data.resource))
                    extensions = QuestionnaireResponseDao.extension_models_from_fhir_objects(fhir_qr.extension)
                    map(session.add, extensions)

                    latest_date = response_data.created

                if found_responses:
                    print(f'got to {latest_date}')
                    print('committing')
                    session.commit()


def run():
    return cli_run(tool_cmd, tool_desc, ExtensionBackfillTool)
