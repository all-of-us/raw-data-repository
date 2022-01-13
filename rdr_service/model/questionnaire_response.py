from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text
)
from sqlalchemy import BLOB  # pylint: disable=unused-import
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text
from typing import List

from rdr_service.model.base import Base
from rdr_service.model.utils import EnumZeroBased, UTCDateTime
from rdr_service.model.field_types import BlobUTF8
from rdr_service.participant_enums import QuestionnaireResponseStatus, QuestionnaireResponseClassificationType


class QuestionnaireResponse(Base):
    """"A response to a questionnaire for a participant. Contains answers to questions found in the
  questionnaire."""

    __tablename__ = "questionnaire_response"
    questionnaireResponseId = Column("questionnaire_response_id", Integer, primary_key=True, autoincrement=False)
    """RDR identifier for the response"""
    questionnaireId = Column("questionnaire_id", Integer, nullable=False)
    """RDR identifier for the questionnaire"""
    questionnaireVersion = Column("questionnaire_version", Integer, nullable=False)
    """RDR's version number of the questionnaire"""
    questionnaireSemanticVersion = Column('questionnaire_semantic_version', String(100))
    """PTSC's version of the questionnaire (does not necessarily match RDR version)"""
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    """Identifier for the participant that responded to the questionnaire"""
    nonParticipantAuthor = Column("non_participant_author", String(80), nullable=True)
    created = Column("created", UTCDateTime, nullable=False)
    """The date and time the RDR API received the questionnaire response"""
    authored = Column("authored", UTCDateTime, nullable=True)
    """The actual time the participant completed the questionnaire"""
    language = Column("language", String(2), nullable=True)
    """Language that the response was completed in"""

    # Can be used to indicate equality between sets of answers
    answerHash = Column('answer_hash', String(32), nullable=True)
    """@rdr_dictionary_internal_column"""

    externalId = Column('external_id', String(30), nullable=True)
    """@rdr_dictionary_internal_column"""

    classificationType = Column('classification_type',
      EnumZeroBased(QuestionnaireResponseClassificationType),
      nullable=False,
      default=QuestionnaireResponseClassificationType.COMPLETE,
      server_default=text(str(int(QuestionnaireResponseClassificationType.COMPLETE))))
    """ Classification of a response (e.g., COMPLETE or DUPLICATE) which can determine if it should be ignored """

    resource = Column("resource", BlobUTF8, nullable=False)
    status = Column(
        EnumZeroBased(QuestionnaireResponseStatus),
        nullable=False,
        default=QuestionnaireResponseStatus.COMPLETED,
        server_default=text(str(int(QuestionnaireResponseStatus.COMPLETED)))
    )
    answers: List['QuestionnaireResponseAnswer'] = relationship(
        "QuestionnaireResponseAnswer", cascade="all, delete-orphan"
    )
    extensions = relationship('QuestionnaireResponseExtension')

    __table_args__ = (
        ForeignKeyConstraint(
            ["questionnaire_id", "questionnaire_version"],
            ["questionnaire_history.questionnaire_id", "questionnaire_history.version"],
        ),
        Index('idx_response_identifier_answers', externalId, answerHash),
        Index('idx_created_q_id', questionnaireId, created)
    )


class QuestionnaireResponseAnswer(Base):
    """An answer found in a questionnaire response. Note that there could be multiple answers to
  the same question, if the questionnaire allows for multiple answers.

  An answer is given to a particular question which has a particular concept code. The answer is
  the current answer for a participant from the time period between its parent response's creation
  field and the endTime field (or now, if endTime is not set.)

  When an answer is given by a participant in a questionnaire response, the endTime of any previous
  answers to questions with the same concept codes that don't have endTime set yet should have
  endTime set to the current time.
  """

    # This is the maximum # bytes that can be stored in a MySQL TEXT field, which
    # our field valueString should resolve to.
    # This value has no real affect, under the hood we're changing it to LONGBLOB in alembic/env.py which is 4GB
    VALUE_STRING_MAXLEN = 65535

    __tablename__ = "questionnaire_response_answer"
    questionnaireResponseAnswerId = Column("questionnaire_response_answer_id", Integer, primary_key=True)
    """RDR identifier for this answer"""
    questionnaireResponseId = Column(
        "questionnaire_response_id",
        Integer,
        ForeignKey("questionnaire_response.questionnaire_response_id"),
        nullable=False,
    )
    """RDR identifier for the response this answer is a part of"""
    questionId = Column(
        "question_id", Integer, ForeignKey("questionnaire_question.questionnaire_question_id"), nullable=False
    )
    """Question that this answer is a response to"""
    endTime = Column("end_time", UTCDateTime)
    """The time at which the participant completed another response to the survey, making this answer obsolete"""
    valueSystem = Column("value_system", String(50))
    """The code system used for the response value"""
    valueCodeId = Column("value_code_id", Integer, ForeignKey("code.code_id"))
    """The code used for the response value"""
    valueBoolean = Column("value_boolean", Boolean)
    """When the response to the question is true or false"""
    valueDecimal = Column("value_decimal", Float)
    """When the response to the question is a rational number that has a decimal representation"""
    valueInteger = Column("value_integer", Integer)
    """When the response is a signed integer"""
    valueString = Column("value_string", Text)
    """When the response is a sequence of Unicode characters"""
    valueDate = Column("value_date", Date)
    """
    When the response is a date, or partial date (e.g. just year or year + month) as used in human communication.
    There SHALL be no time zone. Dates SHALL be valid dates.
    """
    valueDateTime = Column("value_datetime", UTCDateTime)
    """
    When the response is a date, date-time or partial date (e.g. just year or year + month)
    as used in human communication.
    """
    valueUri = Column("value_uri", String(1024))
    """
    When the response is a Uniform Resource Identifier Reference (RFC 3986 ).
    Note: URIs are case sensitive. For UUID (urn:uuid:53fefa32-fcbb-4ff8-8a92-55ee120877b7) use all lowercase
    """


class QuestionnaireResponseExtension(Base):
    """
    Extension object provided with a questionnaire response payload, fields derived from the FHIR 1.0.6 standard
    """
    __tablename__ = "questionnaire_response_extension"
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    questionnaireResponseId = Column(
        "questionnaire_response_id",
        Integer,
        ForeignKey(QuestionnaireResponse.questionnaireResponseId),
        nullable=False
    )

    url = Column(String(1024))
    valueCode = Column('value_code', String(512))
    valueString = Column('value_string', String(512))
