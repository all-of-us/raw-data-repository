import datetime
import json
import logging
import pytz
from typing import List

from rdr_service.api_util import dispatch_task
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg
from rdr_service.lib_fhir.fhirclient_1_0_6.models.questionnaire import Questionnaire as FhirQuestionnaire
from rdr_service.lib_fhir.fhirclient_1_0_6.models.questionnaireresponse import \
    QuestionnaireResponse as FhirQuestionnaireResponse
from werkzeug.exceptions import BadRequest

from rdr_service.clock import CLOCK
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.domain_model import etm as models
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.repository import etm as etm_repository
from rdr_service.services.response_validation.etm_validation import EtmValidation


class EtmApi:
    MODULE_IDENTIFIER_SYSTEM = 'https://research.joinallofus.org/fhir/module_identifier'

    @classmethod
    def post_questionnaire(cls, questionnaire_json):
        fhir_questionnaire = FhirQuestionnaire(questionnaire_json)

        questionnaire_type = None
        for identifier in fhir_questionnaire.identifier:
            if identifier.system == cls.MODULE_IDENTIFIER_SYSTEM:
                module_name = identifier.value.split('_')[0].lower()  # Remove version suffix from module name
                questionnaire_type = module_name
        if questionnaire_type is None:
            raise BadRequest('Unable to identify EtM module')

        questionnaire_obj = models.EtmQuestionnaire(
            created=CLOCK.now(),
            modified=CLOCK.now(),
            questionnaire_type=questionnaire_type,
            semantic_version=fhir_questionnaire.identifier[0].value,
            title=fhir_questionnaire.text.div,
            resource_json=questionnaire_json
        )

        repository = etm_repository.EtmQuestionnaireRepository()
        repository.store_questionnaire(questionnaire_obj)

        # Support RDR to PDR pipeline.  All we need is just the pubsub message for the newly stored
        # etm_questionnaire record (don't need submit_pipeline_pubsub_msg_from_model())
        submit_pipeline_pubsub_msg(database='rdr', table='etm_questionnaire', action='insert',
                                   pk_columns=['etm_questionnaire_id'], pk_values=[questionnaire_obj.id])

        return {
            **questionnaire_json
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
        response_obj.questionnaire_id = questionnaire.id

        if validation_result.success:
            response_repository = etm_repository.EtmResponseRepository()
            response_repository.store_response(response_obj)
            resp_authored = response_obj.authored
            # If authored is a datetime and has tzinfo, convert to utc and remove tzinfo.
            # The authored timestamps in the participant summary will already be in utc, but lack tzinfo.
            if resp_authored and isinstance(resp_authored, datetime.datetime) and resp_authored.tzinfo:
                resp_authored = resp_authored.astimezone(pytz.utc).replace(tzinfo=None)
            ps_dao = ParticipantSummaryDao()
            ps_obj = ps_dao.get_by_participant_id(response_obj.participant_id)
            if not ps_obj:
                logging.error(
                    f'Unable to store EtM task activity in participant_summary for {response_obj.participant_id}'
                )
            elif (ps_obj.latestEtMTaskAuthored is None or
                  ps_obj.latestEtMTaskAuthored < resp_authored):
                ps_obj.latestEtMTaskTime = response_obj.created
                ps_obj.latestEtMTaskAuthored = resp_authored
                ps_obj.lastModified = CLOCK.now()
                ps_dao.update(ps_obj)

            # Support RDR to PDR pipeline.  All we need is just the pubsub message for the newly stored
            # etm_questionnaire_response record (don't need submit_pipeline_pubsub_msg_from_model())
            submit_pipeline_pubsub_msg(database='rdr', table='etm_questionnaire_response', action='insert',
                                       pk_columns=['etm_questionnaire_response_id'], pk_values=[response_obj.id])

            dispatch_task(endpoint='update_retention_status', payload={'participant_id': response_obj.participant_id})

            return {
                'id': str(response_obj.id),
                **questionnaire_response_json
            }
        else:
            validation_errors = ','.join(validation_result.errors)
            logging.warning(f'Validation failed: {validation_errors}')
            raise BadRequest(validation_errors)

    @classmethod
    def is_etm_payload(cls, payload_json):
        return (
            'extension' in payload_json
            and any(
                extension.get('url') and 'outcomes' in extension.get('url')
                for extension in payload_json['extension']
            )
        )

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
            url_str_fragment='metadata'
        ))
        response_obj.outcome_list = cls._parse_extension_json(cls._find_extension(
            extension_list=fhir_response.extension,
            url_str_fragment='outcomes'
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
            for fhir_answer in question_answer.answer:
                # try to load answer from value coding (original expectation)
                # otherwise load it from the metadata
                if fhir_answer.valueCoding:
                    answer_value = fhir_answer.valueCoding.code
                else:
                    response_extension = None
                    for extension in fhir_answer.extension:
                        if extension.url.endswith('response'):
                            response_extension = extension
                            break
                    answer_value = response_extension.valueString if response_extension else None

                trial_id = None
                for extension in fhir_answer.extension:
                    if extension.url.endswith('trial_id'):
                        trial_id = extension.valueString
                        break

                answer_obj = models.EtmResponseAnswer(
                    trial_id=trial_id,
                    answer=answer_value
                )
                for extension in fhir_answer.extension:
                    answer_obj.metadata_list.append(
                        models.EtmAnswerMetadata(
                            url=extension.url,
                            value=cls._first_non_none_value(extension)
                        )
                    )

                result.append(answer_obj)

        return result

    @classmethod
    def _participant_id_from_patient_ref(cls, patient_ref_str):
        return patient_ref_str[-9:]

    @classmethod
    def _find_extension(cls, extension_list, url_str_fragment):
        for extension in extension_list:
            if url_str_fragment in extension.url:
                return extension

    @classmethod
    def _first_non_none_value(cls, fhir_value_container):
        for attr_name in [
            'valueBoolean',
            'valueDateTime',
            'valueDecimal',
            'valueInteger',
            'valueString'
        ]:
            value = getattr(fhir_value_container, attr_name)
            if value is not None:
                return value
