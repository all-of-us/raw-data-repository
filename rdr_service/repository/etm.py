
from sqlalchemy.orm import Session

from rdr_service.dao import database_factory
from rdr_service.domain_model import etm as domain_model
from rdr_service.model import etm as schema_model
from rdr_service.repository import BaseRepository
# from rdr_service.model.pr


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

        self._add_to_session(schema_object)
        questionnaire.id = schema_object.etm_questionnaire_id


        # TODO: figure out version number


class EtmResponseRepository:
    def __init__(self, session: Session = None):
        self._session = session

    # def store_
