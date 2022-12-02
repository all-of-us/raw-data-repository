
from rdr_service.lib_fhir.fhirclient_4_0_0.models.questionnaire import Questionnaire as FhirQuestionnaire

from rdr_service.clock import CLOCK
from rdr_service.domain_model.etm import EtmQuestionnaire
from rdr_service.repository import etm as etm_repository

ETM_OUTCOMES_EXT_URL = 'https://research.joinallofus.org/fhir/outcomes'
ETM_EMOTIONAL_RECOGNITION_URL = 'https://research.joinallofus.org/fhir/emorecog'


class EtmApi:
    @classmethod
    def post_questionnaire(cls, questionnaire_json):
        # fhir_questionnaire = FhirQuestionnaire(questionnaire_json)
        # TODO: FHIR validation currently failing

        questionnaire_obj = EtmQuestionnaire(
            created=CLOCK.now(),
            modified=CLOCK.now(),
            questionnaire_type=questionnaire_json.get('url'),
            semantic_version=questionnaire_json.get('version'),
            name=questionnaire_json.get('name'),
            title=questionnaire_json.get('title'),
            resource_json=questionnaire_json
        )

        repository = etm_repository.EtmQuestionnaireRepository()
        repository.store_questionnaire(questionnaire_obj)

        return {
            'id': questionnaire_obj.id
        }

    @classmethod
    def post_questionnaire_response(cls, questionnaire_response_json):
        return 1
