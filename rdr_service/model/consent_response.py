
import sqlalchemy as sa
from sqlalchemy.event import listen
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base, model_insert_listener
from rdr_service.model.consent_file import ConsentFile, ConsentType
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.model.utils import Enum, UTCDateTime


class ConsentResponse(Base):
    """
    Responsible for storing a link between QuestionnaireResponse and the ConsentFile records we should have for them.
    """
    __tablename__ = 'consent_response'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    """Primary key of the consent_response table"""
    created = sa.Column(UTCDateTime)
    """UTC timestamp of when the consent_response record was created"""
    questionnaire_response_id = sa.Column(sa.Integer, sa.ForeignKey(QuestionnaireResponse.questionnaireResponseId),
                                          nullable=False)
    """Id of the questionnaire response that is a consent for the participant."""
    consent_file_id = sa.Column(sa.Integer, sa.ForeignKey(ConsentFile.id), nullable=True)
    """
    Id of the consent file that has been checked for the consent response. Will be null until the consent validation
    process runs.
    """
    type = sa.Column(Enum(ConsentType))
    """The type of consent given by the response."""

    response = relationship(QuestionnaireResponse)


listen(ConsentResponse, 'before_insert', model_insert_listener)
