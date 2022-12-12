
from protorpc import messages
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import EnumZeroBased, UTCDateTime
from rdr_service.participant_enums import QuestionnaireStatus


class ExtensionType(messages.Enum):
    METADATA = 0
    OUTCOME = 1


class EtmQuestionnaire(Base):
    __tablename__ = 'etm_questionnaire'
    etm_questionnaire_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    created = sa.Column(UTCDateTime)
    modified = sa.Column(UTCDateTime)
    questionnaire_type = sa.Column(sa.String(100))
    version = sa.Column(sa.Integer)
    semantic_version = sa.Column(sa.String(100))
    title = sa.Column(sa.String(100))
    resource = sa.Column(sa.JSON)


class EtmQuestionnaireResponse(Base):
    __tablename__ = 'etm_questionnaire_response'
    etm_questionnaire_response_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    created = sa.Column(UTCDateTime)
    modified = sa.Column(UTCDateTime)
    authored = sa.Column(UTCDateTime)
    questionnaire_type = sa.Column(sa.String(100))
    status = sa.Column(EnumZeroBased(QuestionnaireStatus))
    participant_id = sa.Column(sa.Integer, sa.ForeignKey("participant.participant_id"))
    resource = sa.Column(sa.JSON)
    version = sa.Column(sa.Integer)

    extension_list = sa.orm.relationship(
        'EtmQuestionnaireResponseMetadata',
        backref='response'
    )
    answer_list = sa.orm.relationship(
        'EtmQuestionnaireResponseAnswer',
        backref='response'
    )


class EtmQuestionnaireResponseMetadata(Base):
    __tablename__ = 'etm_questionnaire_response_metadata'
    etm_questionnaire_response_metadata_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    etm_questionnaire_response_id = sa.Column(
        sa.Integer, sa.ForeignKey(EtmQuestionnaireResponse.etm_questionnaire_response_id)
    )
    extension_type = sa.Column(EnumZeroBased(ExtensionType))
    key = sa.Column(sa.String(100))
    value_string = sa.Column(MEDIUMTEXT)
    value_int = sa.Column(sa.BigInteger)
    value_decimal = sa.Column(sa.Float)


class EtmQuestionnaireResponseAnswer(Base):
    __tablename__ = 'etm_questionnaire_response_answer'
    etm_questionnaire_response_answer_id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    etm_questionnaire_response_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(EtmQuestionnaireResponse.etm_questionnaire_response_id)
    )
    link_id = sa.Column(sa.String(40))
    answer_value = sa.Column(MEDIUMTEXT)

    metadata_list = sa.orm.relationship('EtmAnswerMetadata', backref='answer')


class EtmAnswerMetadata(Base):
    __tablename__ = 'etm_questionnaire_response_answer_metadata'
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    answer_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(EtmQuestionnaireResponseAnswer.etm_questionnaire_response_answer_id)
    )
    url = sa.Column(sa.String(128))
    value = sa.Column(sa.String(128))


sa.event.listen(EtmQuestionnaire, 'before_insert', model_insert_listener)
sa.event.listen(EtmQuestionnaire, 'before_update', model_update_listener)
sa.event.listen(EtmQuestionnaireResponse, 'before_insert', model_insert_listener)
sa.event.listen(EtmQuestionnaireResponse, 'before_update', model_update_listener)
