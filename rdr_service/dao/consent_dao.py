from datetime import datetime
from typing import Collection

from sqlalchemy import or_
from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary


class ConsentDao(BaseDao):
    def __init__(self):
        super(ConsentDao, self).__init__(ConsentFile)

    @classmethod
    def get_participants_with_consents_in_range(cls, session, start_date,
                                                end_date=None) -> Collection[ParticipantSummary]:
        query = session.query(
            ParticipantSummary
        ).join(
            Participant,
            Participant.participantId == ParticipantSummary.participantId
        ).filter(
            ParticipantSummary.participantOrigin == 'vibrent',
            Participant.isGhostId.isnot(True),
            Participant.isTestParticipant.isnot(True),
            or_(
                ParticipantSummary.email.is_(None),
                ParticipantSummary.email.notlike('%@example.com')
            )
        )
        if end_date is None:
            query = query.filter(
                or_(
                    ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored >= start_date,
                    ParticipantSummary.consentForCABoRAuthored >= start_date,
                    ParticipantSummary.consentForElectronicHealthRecordsAuthored >= start_date,
                    ParticipantSummary.consentForGenomicsRORAuthored >= start_date
                )
            )
        else:
            query = query.filter(
                or_(
                    ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored.between(start_date, end_date),
                    ParticipantSummary.consentForCABoRAuthored.between(start_date, end_date),
                    ParticipantSummary.consentForElectronicHealthRecordsAuthored.between(start_date, end_date),
                    ParticipantSummary.consentForGenomicsRORAuthored.between(start_date, end_date)
                )
            )
        summaries = query.all()
        return summaries

    @classmethod
    def get_files_needing_correction(cls, session, min_modified_datetime: datetime = None) -> Collection[ConsentFile]:
        query = session.query(ConsentFile).filter(
            ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING
        )
        if min_modified_datetime:
            query = query.filter(ConsentFile.modified >= min_modified_datetime)
        return query.all()

    @classmethod
    def batch_update_consent_files(cls, session, consent_files: Collection[ConsentFile]):
        for file_record in consent_files:
            session.merge(file_record)

    @classmethod
    def get_validation_results_for_participants(cls, session,
                                                participant_ids: Collection[int]) -> Collection[ConsentFile]:
        return session.query(ConsentFile).filter(
            ConsentFile.participant_id.in_(participant_ids)
        ).all()

    @classmethod
    def _get_ready_to_sync_with_session(cls, session: Session, org_names=None):
        query = (
            session.query(ConsentFile)
            .join(Participant)
            .join(Organization)
            .filter(ConsentFile.sync_status == ConsentSyncStatus.READY_FOR_SYNC)
        )
        if org_names is not None:
            query = query.filter(Organization.externalId.in_(org_names))
        return query.all()

    def get_files_ready_to_sync(self, org_names=None, session: Session = None) -> Collection[ConsentFile]:
        if session is None:
            with self.session() as dao_session:
                return self._get_ready_to_sync_with_session(session=dao_session, org_names=org_names)
        else:
            return self._get_ready_to_sync_with_session(session=session, org_names=org_names)

    @classmethod
    def set_previously_synced_files_as_ready(cls, session: Session, participant_id: int):
        session.query(
            ConsentFile
        ).filter(
            ConsentFile.sync_status == ConsentSyncStatus.SYNC_COMPLETE,
            ConsentFile.participant_id == participant_id
        ).update({
            ConsentFile.sync_status: ConsentSyncStatus.READY_FOR_SYNC,
            ConsentFile.sync_time: None
        })
