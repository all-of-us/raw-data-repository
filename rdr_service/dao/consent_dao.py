
from sqlalchemy import or_

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.participant_summary import ParticipantSummary


class ConsentDao(BaseDao):
    def get_participants_with_consents_in_range(self, start_date, end_date=None):
        with self.session() as session:
            query = session.query(ParticipantSummary)
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
