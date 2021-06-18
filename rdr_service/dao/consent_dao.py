from typing import List

from sqlalchemy import or_

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary


class ConsentDao(BaseDao):
    def __init__(self):
        super(ConsentDao, self).__init__(ConsentFile)

    def get_participants_with_consents_in_range(self, start_date=None, end_date=None) -> List[ParticipantSummary]:
        with self.session() as session:
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
                ),
                Participant.hpoId == 15
            )
            if start_date is not None:
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

    def get_files_needing_correction(self) -> List[ConsentFile]:
        with self.session() as session:
            return session.query(ConsentFile).filter(
                ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING
            ).all()

    def batch_update_consent_files(self, consent_files: List[ConsentFile]):
        with self.session() as session:
            for file_record in consent_files:
                session.merge(file_record)

    def get_validation_results_for_participants(self, participant_ids: List[int]) -> List[ConsentFile]:
        with self.session() as session:
            return session.query(ConsentFile).filter(
                ConsentFile.participant_id.in_(participant_ids)
            ).all()

    def get_files_ready_to_sync(self) -> List[ConsentFile]:
        with self.session() as session:
            return session.query(ConsentFile).filter(
                ConsentFile.sync_status == ConsentSyncStatus.READY_FOR_SYNC
            ).all()
