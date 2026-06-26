
import sqlalchemy as sa

from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class PrsConsentSubmission(Base):
    __tablename__ = 'prs_consent_submission'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, server_default=sa.text('CURRENT_TIMESTAMP'))
    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'))
    signed_date = sa.Column(sa.Date)
    consent_type = sa.Column(sa.String, nullable=False, index=True)


class PrsConsentValidationResult(Base):
    __tablename__ = 'prs_consent_validation_result'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, server_default=sa.text('CURRENT_TIMESTAMP'))
    prs_consent_submission_id = sa.Column(sa.Integer, sa.ForeignKey(PrsConsentSubmission.id))
    is_valid = sa.Column(sa.Boolean)
    ignore = sa.Column(sa.Boolean, server_default=0)


class PrsConsentValidationError(Base):
    __tablename__ = 'prs_consent_validation_error'
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True, nullable=False)
    prs_consent_validation_result_id = sa.Column(sa.Integer, sa.ForeignKey(PrsConsentValidationResult.id))
    error_message = sa.Column(sa.String)
    expected_data = sa.Column(sa.String)
    pdf_data = sa.Column(sa.String)
