from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy import BLOB  # pylint: disable=unused-import
from sqlalchemy.orm import relationship
from typing import List

from rdr_service.model.base import Base
from rdr_service.model.code import Code
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import QuestionnaireDefinitionStatus
from rdr_service.model.field_types import BlobUTF8


class QuestionnaireBase(object):
    """Mixin containing columns for Questionnaire and QuestionnaireHistory"""

    questionnaireId = Column("questionnaire_id", Integer, primary_key=True)
    """RDR identifier for the questionnaire"""
    # Incrementing version, starts at 1 and is incremented on each update.
    version = Column("version", Integer, nullable=False)
    """RDR version of the questionnaire"""
    semanticVersion = Column('semantic_version', String(100))
    """PTSC's version of the questionnaire (does not necessarily match RDR version)"""
    semanticDesc = Column('semantic_desc', String(500))
    irbMapping = Column('irb_mapping', String(500))
    created = Column("created", UTCDateTime, nullable=False)
    """The date and time the questionnaire was created"""
    lastModified = Column("last_modified", UTCDateTime, nullable=False)
    """The date and time the questionnaire was last modified"""
    # The JSON representation of the questionnaire provided by the client.
    # Concepts and questions can be be parsed out of this for use in querying.
    resource = Column("resource", BlobUTF8, nullable=False)
    status = Column("status", Enum(QuestionnaireDefinitionStatus), default=QuestionnaireDefinitionStatus.VALID)

    externalId = Column('external_id', String(100))

    def asdict_with_children(self):
        return self.asdict(follow={"concepts": {}, "questions": {}})


class Questionnaire(QuestionnaireBase, Base):
    """A questionnaire containing questions to pose to participants."""

    __tablename__ = "questionnaire"
    concepts = relationship(
        "QuestionnaireConcept",
        cascade="expunge",
        cascade_backrefs=False,
        primaryjoin="Questionnaire.questionnaireId==" + "foreign(QuestionnaireConcept.questionnaireId)",
    )
    questions = relationship(
        "QuestionnaireQuestion",
        cascade="expunge",
        cascade_backrefs=False,
        primaryjoin="and_(Questionnaire.questionnaireId==" + "foreign(QuestionnaireQuestion.questionnaireId)," + \
        "Questionnaire.version==" + "foreign(QuestionnaireQuestion.questionnaireVersion))",
    )


class QuestionnaireHistory(QuestionnaireBase, Base):
    __tablename__ = "questionnaire_history"
    version = Column("version", Integer, primary_key=True)
    concepts: List['QuestionnaireConcept'] = relationship("QuestionnaireConcept", cascade="all, delete-orphan")
    questions: List['QuestionnaireQuestion'] = relationship("QuestionnaireQuestion", cascade="all, delete-orphan")


class QuestionnaireConcept(Base):
    """Concepts for the questionnaire as a whole.

  These should be copied whenever a new version of
  a questionnaire is created.
  """

    __tablename__ = "questionnaire_concept"
    questionnaireConceptId = Column("questionnaire_concept_id", Integer, primary_key=True)
    """An identifier to link the questionnaire to the CONCEPT table from OMOP"""
    questionnaireId = Column("questionnaire_id", Integer, nullable=False)
    """Questionnaire identifier for the concept"""
    questionnaireVersion = Column("questionnaire_version", Integer, nullable=False)
    """Questionnaire's RDR version for the concept"""
    codeId = Column("code_id", Integer, ForeignKey("code.code_id"), nullable=False)
    """The corresponding concept for this item"""
    __table_args__ = (
        ForeignKeyConstraint(
            ["questionnaire_id", "questionnaire_version"],
            ["questionnaire_history.questionnaire_id", "questionnaire_history.version"],
        ),
        UniqueConstraint("questionnaire_id", "questionnaire_version", "code_id"),
    )


class QuestionnaireQuestion(Base):
    """A question in a questionnaire.

  These should be copied whenever a new version of a
  questionnaire is created.

  Each question has a concept system and code defining what the question is
  about. Questions on
  different questionnaires can share the same concept code, but concept code is
  unique within a
  given questionnaire.
  """

    __tablename__ = "questionnaire_question"
    questionnaireQuestionId = Column("questionnaire_question_id", Integer, primary_key=True)
    """RDR identifier for the question"""
    questionnaireId = Column("questionnaire_id", Integer)
    """RDR identifier for the containing questionnaire"""
    questionnaireVersion = Column("questionnaire_version", Integer)
    """RDR version for the containing questionnaire"""
    linkId = Column("link_id", String(40))
    """The unique ID for the item in the questionnaire"""
    codeId = Column("code_id", Integer, ForeignKey("code.code_id"), nullable=False)
    """The corresponding concept for this question"""
    repeats = Column("repeats", Boolean, nullable=False)
    """Whether the item may repeat"""
    __table_args__ = (
        ForeignKeyConstraint(
            ["questionnaire_id", "questionnaire_version"],
            ["questionnaire_history.questionnaire_id", "questionnaire_history.version"],
        ),
        UniqueConstraint("questionnaire_id", "questionnaire_version", "link_id"),
    )

    code = relationship(Code)
