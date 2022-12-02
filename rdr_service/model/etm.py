
from protorpc import messages
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import EnumZeroBased, UTCDateTime
from rdr_service.participant_enums import QuestionnaireResponseStatus


class ExtensionType(messages.Enum):
    """Given status of a questionnaire response"""

    METADATA = 0
    OUTCOME = 1


class EtmQuestionnaire(Base):
    __tablename__ = 'etm_questionnaire'
    etm_questionnaire_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    created = sa.Column(UTCDateTime)
    modified = sa.Column(UTCDateTime)
    questionnaire_type = sa.Column(sa.String(100))
    version = sa.Column(sa.Integer)   # TODO: increments with newer values when another questionnaire of the same questionnaire_type is sent
    semantic_version = sa.Column(sa.String(100))
    name = sa.Column(sa.String(100))
    title = sa.Column(sa.String(100))
    resource = sa.Column(sa.JSON)


class EtmQuestionnaireResponse(Base):
    __tablename__ = 'etm_questionnaire_response'
    etm_questionnaire_response_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    created = sa.Column(UTCDateTime)
    modified = sa.Column(UTCDateTime)
    authored = sa.Column(UTCDateTime)
    questionnaire_id = sa.Column(sa.String(50))
    questionnaire_type = sa.Column(sa.String(100))
    status = sa.Column(EnumZeroBased(QuestionnaireResponseStatus))
    participant_id = sa.Column(sa.Integer, sa.ForeignKey("participant.participant_id"))
    resource = sa.Column(sa.JSON)
    version = sa.Column(sa.Integer)


class EtmQuestionnaireResponseMetadata(Base):
    __tablename__ = 'etm_questionnaire_response_metadata'
    etm_questionnaire_response_metadata_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    etm_questionnaire_response_id = sa.Column(
        sa.Integer, sa.ForeignKey(EtmQuestionnaireResponse.etm_questionnaire_response_id)
    )
    extension_type = sa.Column(EnumZeroBased(ExtensionType))
    key = sa.Column(sa.String(100))
    value_string = sa.Column(MEDIUMTEXT)
    value_int = sa.Column(sa.Integer)
    value_decimal = sa.Column(sa.Float)


class EtmQuestionnaireResponseAnswers:
    __tablename__ = 'etm_questionnaire_response_answer'
    etm_questionnaire_response_answer_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    etm_questionnaire_response_id = sa.Column(
        sa.Integer, sa.ForeignKey(EtmQuestionnaireResponse.etm_questionnaire_response_id)
    )
    link_id = sa.Column(sa.String(40))
    answer = sa.Column(sa.String(100))
    value_string = sa.Column(MEDIUMTEXT)
    value_integer = sa.Column(sa.Integer)
    value_decimal = sa.Column(sa.Float)


sa.event.listen(EtmQuestionnaire, 'before_insert', model_insert_listener)
sa.event.listen(EtmQuestionnaire, 'before_update', model_update_listener)
sa.event.listen(EtmQuestionnaireResponse, 'before_insert', model_insert_listener)
sa.event.listen(EtmQuestionnaireResponse, 'before_update', model_update_listener)
