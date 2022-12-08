import json
import logging
from typing import List

from rdr_service.lib_fhir.fhirclient_1_0_6.models.questionnaire import Questionnaire as FhirQuestionnaire
from rdr_service.lib_fhir.fhirclient_1_0_6.models.questionnaireresponse import \
    QuestionnaireResponse as FhirQuestionnaireResponse
from werkzeug.exceptions import BadRequest

from rdr_service.clock import CLOCK
from rdr_service.domain_model import etm as models
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.repository import etm as etm_repository
from rdr_service.services.response_validation.etm_validation import EtmValidation

ETM_OUTCOMES_EXT_URL = 'https://research.joinallofus.org/fhir/outcomes'
ETM_EMOTIONAL_RECOGNITION_URL = 'https://research.joinallofus.org/fhir/emorecog'

OUTCOMES_EXTENSION_URL = 'https://research.joinallofus.org/fhir/emorecog/outcomes'
METADATA_EXTENSION_URL = 'https://research.joinallofus.org/fhir/emorecog/metadata'


class EtmApi:
    @classmethod
    def post_questionnaire(cls, questionnaire_json):
        fhir_questionnaire = FhirQuestionnaire(questionnaire_json)

        questionnaire_obj = models.EtmQuestionnaire(
            created=CLOCK.now(),
            modified=CLOCK.now(),
            questionnaire_type=fhir_questionnaire.id,
            semantic_version=fhir_questionnaire.version,
            title=fhir_questionnaire.text.div,
            resource_json=questionnaire_json
        )

        repository = etm_repository.EtmQuestionnaireRepository()
        repository.store_questionnaire(questionnaire_obj)

        return {
            'id': questionnaire_obj.id
        }

    @classmethod
    def post_questionnaire_response(cls, questionnaire_response_json):
        response_obj = cls._parse_response(questionnaire_response_json)

        questionnaire_repository = etm_repository.EtmQuestionnaireRepository()
        questionnaire = questionnaire_repository.latest_questionnaire_for_type(response_obj.questionnaire_type)
        validation_result = EtmValidation.validate_response(
            response=response_obj,
            questionnaire=questionnaire
        )
        response_obj.version = questionnaire.version

        if validation_result.success:
            response_repository = etm_repository.EtmResponseRepository()
            response_repository.store_response(response_obj)

            return {
                'id': response_obj.id
            }
        else:
            validation_errors = ','.join(validation_result.errors)
            logging.warning(f'Validation failed: {validation_errors}')
            raise BadRequest(validation_errors)

    @classmethod
    def _parse_response(cls, questionnaire_response_json) -> models.EtmResponse:
        fhir_response = FhirQuestionnaireResponse(questionnaire_response_json)

        response_obj = models.EtmResponse(
            created=CLOCK.now(),
            modified=CLOCK.now(),
            authored=fhir_response.authored.date,
            questionnaire_type=fhir_response.questionnaire.reference.split('/')[-1],
            status=QuestionnaireStatus.SUBMITTED,
            participant_id=cls._participant_id_from_patient_ref(fhir_response.subject.reference),
            resource_json=questionnaire_response_json
        )

        response_obj.metadata_list = cls._parse_extension_json(cls._find_extension(
            extension_list=fhir_response.extension,
            url_str=METADATA_EXTENSION_URL
        ))
        response_obj.outcome_list = cls._parse_extension_json(cls._find_extension(
            extension_list=fhir_response.extension,
            url_str=OUTCOMES_EXTENSION_URL
        ))
        response_obj.answer_list = cls._parse_answers(fhir_response.group.question)

        return response_obj

    @classmethod
    def _parse_extension_json(cls, metadata_extension) -> List[models.EtmResponseExtension]:
        metadata_json = json.loads(metadata_extension.valueString)
        result = []
        for name, value in metadata_json.items():
            extension_object = models.EtmResponseExtension(key=name)
            if isinstance(value, int):
                extension_object.value_int = value
            elif isinstance(value, float):
                extension_object.value_decimal = value
            else:
                extension_object.value_string = value

            result.append(extension_object)

        return result

    @classmethod
    def _parse_answers(cls, question_list) -> List[models.EtmResponseAnswer]:
        result = []
        for question_answer in question_list:
            if len(question_answer.answer) > 0:
                fhir_answer = question_answer.answer[0]

                answer_value = fhir_answer.valueCoding.code
                result.append(
                    models.EtmResponseAnswer(
                        link_id=question_answer.linkId,
                        answer=answer_value
                    )
                )

        return result

    @classmethod
    def _participant_id_from_patient_ref(cls, patient_ref_str):
        return patient_ref_str[-9:]

    @classmethod
    def _find_extension(cls, extension_list, url_str):
        for extension in extension_list:
            if extension.url == url_str:
                return extension
