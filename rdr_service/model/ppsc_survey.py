from typing import List

from protorpc import messages
import sqlalchemy as sa
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.code import Code
from rdr_service.model.utils import EnumZeroBased, UTCDateTime


class PpscSurveyDataDictionary(Base):
    __tablename__ = 'ppsc_survey_data_dictionary'

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, server_default=sa.text('CURRENT_TIMESTAMP'))

    ppsc_survey_id = sa.Column(sa.Integer)
    version_number = sa.Column(sa.Integer, nullable=False)

    questions: List['PpscSurveyQuestionDefinition'] = relationship(
        'PpscSurveyQuestionDefinition',
        back_populates='data_dictionary'
    )


class PpscQuestionDataType(messages.Enum):
    TEXT = 0
    INTEGER = 1
    FLOAT = 2
    SELECTION = 3


class PpscSurveyQuestionDefinition(Base):
    __tablename__ = 'ppsc_survey_question_definition'

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)

    code_id = sa.Column(sa.Integer, sa.ForeignKey(Code.codeId))

    data_type = sa.Column(EnumZeroBased(PpscQuestionDataType))
    branching_logic = sa.Column(sa.JSON)
    answer_format_pattern = sa.Column(sa.String(512))

    min_value = sa.Column(sa.String(256))
    max_value = sa.Column(sa.String(256))
    multiple_of = sa.Column(sa.String(256))

    data_dictionary_id = sa.Column('dictionary_id', sa.Integer, sa.ForeignKey(PpscSurveyDataDictionary.id))
    data_dictionary = relationship(PpscSurveyDataDictionary, back_populates='questions')

    options: List['PpscSurveyQuestionOption'] = relationship('PpscSurveyQuestionOption', back_populates='question')


class PpscSurveyQuestionOption(Base):
    __tablename__ = 'ppsc_survey_question_option'

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)

    question_id = sa.Column('question_id', sa.Integer, sa.ForeignKey(PpscSurveyQuestionDefinition.id))
    question = relationship(PpscSurveyQuestionDefinition, back_populates='options')

    code_id = sa.Column(sa.Integer, sa.ForeignKey(Code.codeId))
    is_exclusive = sa.Column(sa.Boolean)
