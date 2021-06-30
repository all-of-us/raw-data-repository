from datetime import datetime
from typing import Collection, List

from sqlalchemy import or_

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary


class ConsentDao(BaseDao):
    def __init__(self):
        super(ConsentDao, self).__init__(ConsentFile)

    def get_all_ehr_for_revalidation(self, session):
        # TODO: should have only gotten records that weren't obsolete
        return session.query(
            ConsentFile
        ).filter(
            ConsentFile.type == ConsentType.EHR
        ).order_by(ConsentFile.participant_id).all()

    def get_ce_participants_with_consents(self, session, start_date, end_date=None) -> List[ParticipantSummary]:
        query = session.query(
            ParticipantSummary
        ).join(
            Participant,
            Participant.participantId == ParticipantSummary.participantId
        ).filter(
            ParticipantSummary.participantOrigin == 'careevolution',
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

    def get_org_change_summaries(self, session) -> List[ParticipantSummary]:
        return session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId.in_([

            ])
        )

    def get_participants_with_consents_in_range(self, start_date, end_date=None) -> List[ParticipantSummary]:
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

    def get_files_needing_correction(self, session, min_modified_datetime: datetime = None) -> List[ConsentFile]:
        query = session.query(ConsentFile).filter(
            ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING
        ).order_by(ConsentFile.participant_id)
        if min_modified_datetime:
            query = query.filter(ConsentFile.modified >= min_modified_datetime)
        return query.all()

    def batch_update_consent_files(self, session, consent_files: List[ConsentFile]):
        for file_record in consent_files:
            session.merge(file_record)

    def get_validation_results_for_participants(self, session, participant_ids: Collection[int]) -> List[ConsentFile]:
        return session.query(ConsentFile).filter(
            ConsentFile.participant_id.in_(participant_ids)
        ).all()

    def get_files_ready_to_sync(self) -> List[ConsentFile]:
        with self.session() as session:
            return session.query(ConsentFile).filter(
                ConsentFile.sync_status == ConsentSyncStatus.READY_FOR_SYNC
            ).all()
