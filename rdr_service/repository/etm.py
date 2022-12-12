
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
        schema_object.title = questionnaire.title
        schema_object.resource = questionnaire.resource_json
        schema_object.version = self._get_next_version_number(questionnaire.questionnaire_type)

        self._add_to_session(schema_object)
        questionnaire.id = schema_object.etm_questionnaire_id

    def latest_questionnaire_for_type(self, questionnaire_url) -> domain_model.EtmQuestionnaire:
        db_object = self._latest_db_object_for_version(questionnaire_url)

        result = domain_model.EtmQuestionnaire(
            id=db_object.etm_questionnaire_id,
            version=db_object.version,
            created=db_object.created,
            modified=db_object.modified,
            questionnaire_type=db_object.questionnaire_type,
            semantic_version=db_object.semantic_version,
            title=db_object.title,
            resource_json=db_object.resource
        )

        if 'extension' in result.resource_json:
            for extension in result.resource_json['extension']:
                is_metadata = 'metadata' in extension['url']
                is_outcomes = 'outcomes' in extension['url']
                if is_metadata or is_outcomes:
                    name_list = [
                        code_object['code']
                        for code_object in extension['valueCodeableConcept']['coding']
                        if 'code' in code_object
                    ]

                    if is_metadata:
                        result.metadata_name_list = name_list
                    else:
                        result.outcome_name_list = name_list

        if (
            'group' in result.resource_json
            and 'question' in result.resource_json['group']
        ):
            question_list = result.resource_json['group']['question']
            result.question_list = [
                domain_model.EtmQuestion(
                    link_id=question_obj['linkId'],
                    required=question_obj['required']
                )
                for question_obj in question_list
            ]

        return result


    def _latest_db_object_for_version(self, questionnaire_url) -> schema_model.EtmQuestionnaire:
        latest_version_query = (
            Query(schema_model.EtmQuestionnaire)
            .filter(
                schema_model.EtmQuestionnaire.questionnaire_type == questionnaire_url
            ).order_by(schema_model.EtmQuestionnaire.version.desc())
            .limit(1)
        )

        if self._session is None:
            with database_factory.get_database().session() as session:
                latest_version_query.session = session
                previous_questionnaire = latest_version_query.one_or_none()
        else:
            latest_version_query.session = self._session
            previous_questionnaire = latest_version_query.one_or_none()

        return previous_questionnaire

    def _get_next_version_number(self, questionnaire_url):
        previous_questionnaire = self._latest_db_object_for_version(questionnaire_url)

        if previous_questionnaire:
            return previous_questionnaire.version + 1
        else:
            return 1


class EtmResponseRepository(BaseRepository):

    def store_response(self, response_obj: domain_model.EtmResponse):
        schema_response = schema_model.EtmQuestionnaireResponse()
        schema_response.created = response_obj.created
        schema_response.modified = response_obj.modified
        schema_response.authored = response_obj.authored
        schema_response.questionnaire_type = response_obj.questionnaire_type
        schema_response.status = response_obj.status
        schema_response.participant_id = response_obj.participant_id
        schema_response.resource = response_obj.resource_json
        schema_response.version = response_obj.version

        for metadata in response_obj.metadata_list:
            schema_response.extension_list.append(
                schema_model.EtmQuestionnaireResponseMetadata(
                    extension_type=schema_model.ExtensionType.METADATA,
                    key=metadata.key,
                    value_string=metadata.value_string,
                    value_int=metadata.value_int,
                    value_decimal=metadata.value_decimal
                )
            )
        for outcome in response_obj.outcome_list:
            schema_response.extension_list.append(
                schema_model.EtmQuestionnaireResponseMetadata(
                    extension_type=schema_model.ExtensionType.OUTCOME,
                    key=outcome.key,
                    value_string=outcome.value_string,
                    value_int=outcome.value_int,
                    value_decimal=outcome.value_decimal
                )
            )

        for domain_answer in response_obj.answer_list:
            schema_answer = schema_model.EtmQuestionnaireResponseAnswer(
                link_id=domain_answer.link_id,
                answer_value=domain_answer.answer
            )
            schema_response.answer_list.append(schema_answer)

            for domain_metadata in domain_answer.metadata_list:
                schema_answer.metadata_list.append(
                    schema_model.EtmAnswerMetadata(
                        url=domain_metadata.url,
                        value=domain_metadata.value
                    )
                )

        self._add_to_session(schema_response)
        response_obj.id = schema_response.etm_questionnaire_response_id
