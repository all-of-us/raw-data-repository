from typing import List

import sqlalchemy as sa

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class PpiValidationResults(Base):
    __tablename__ = 'ppi_validation_results'

    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False)
    modified = sa.Column(UTCDateTime, nullable=True)
    questionnaire_response_id = sa.Column(
        sa.Integer,
        sa.ForeignKey('questionnaire_response.questionnaire_response_id'),
        nullable=False
    )
    survey_id = sa.Column(sa.Integer, sa.ForeignKey('survey.id'), nullable=False)
    obsoletion_timestamp = sa.Column(UTCDateTime, nullable=True)
    obsoletion_reason = sa.Column(sa.String(512), nullable=True)

    errors: List['PpiValidationErrors'] = sa.orm.relationship('PpiValidationErrors', back_populates='result')


sa.event.listen(PpiValidationResults, "before_insert", model_insert_listener)
sa.event.listen(PpiValidationResults, "before_update", model_update_listener)
