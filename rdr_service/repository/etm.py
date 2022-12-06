
from sqlalchemy.orm import Query

from rdr_service.dao import database_factory
from rdr_service.domain_model import etm as domain_model
from rdr_service.model import etm as schema_model
from rdr_service.repository import BaseRepository


class EtmQuestionnaireRepository(BaseRepository):
    def store_questionnaire(self, questionnaire: domain_model.EtmQuestionnaire):
        schema_object = schema_model.EtmQuestionnaire()
        schema_object.created = questionnaire.created
        schema_object.modified = questionnaire.modified
        schema_object.questionnaire_type = questionnaire.questionnaire_type
        schema_object.semantic_version = questionnaire.semantic_version
        schema_object.name = questionnaire.name
        schema_object.title = questionnaire.title
        schema_object.resource = questionnaire.resource_json
        schema_object.version = self._get_next_version_number(questionnaire.questionnaire_type)

        self._add_to_session(schema_object)
        questionnaire.id = schema_object.etm_questionnaire_id

    def _get_next_version_number(self, questionnaire_url):
        previous_version_query = (
            Query(schema_model.EtmQuestionnaire)
            .filter(
                schema_model.EtmQuestionnaire.questionnaire_type == questionnaire_url
            ).order_by(schema_model.EtmQuestionnaire.version.desc())
            .limit(1)
        )

        if self._session is None:
            with database_factory.get_database().session() as session:
                previous_version_query.session = session
                previous_questionnaire = previous_version_query.one_or_none()
        else:
            previous_version_query.session = self._session
            previous_questionnaire = previous_version_query.one_or_none()

        if previous_questionnaire:
            return previous_questionnaire.version + 1
        else:
            return 1


class EtmResponseRepository(BaseRepository):

    def store_response(self, response_obj: domain_model.EtmResponse):
        schema_object = schema_model.EtmQuestionnaireResponse()
        schema_object.created = response_obj.created
        schema_object.modified = response_obj.modified
        schema_object.authored = response_obj.authored
        schema_object.questionnaire_type = response_obj.questionnaire_type
        schema_object.status = response_obj.status
        schema_object.participant_id = response_obj.participant_id
        schema_object.resource = response_obj.resource_json
        schema_object.version = response_obj.version

        for metadata in response_obj.metadata_list:
            schema_object.extension_list.append(
                schema_model.EtmQuestionnaireResponseMetadata(
                    extension_type=schema_model.ExtensionType.METADATA,
                    key=metadata.key,
                    value_string=metadata.value_string,
                    value_int=metadata.value_int,
                    value_decimal=metadata.value_decimal
                )
            )
        for outcome in response_obj.outcome_list:
            schema_object.extension_list.append(
                schema_model.EtmQuestionnaireResponseMetadata(
                    extension_type=schema_model.ExtensionType.OUTCOME,
                    key=outcome.key,
                    value_string=outcome.value_string,
                    value_int=outcome.value_int,
                    value_decimal=outcome.value_decimal
                )
            )

        for answer in response_obj.answer_list:
            schema_object.answer_list.append(
                schema_model.EtmQuestionnaireResponseAnswer(
                    link_id=answer.link_id,
                    answer_value=answer.answer
                )
            )

        self._add_to_session(schema_object)
        response_obj.id = schema_object.etm_questionnaire_response_id
