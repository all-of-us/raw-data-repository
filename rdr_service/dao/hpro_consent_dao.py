
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.hpro_consent_files import HealthProConsentFile


class HealthProConsentDao(UpdatableDao):
    validate_version_match = False

    def __init__(self):
        super(HealthProConsentDao, self).__init__(
            HealthProConsentFile, order_by_ending=['id'])

    def get_needed_consents_for_transfer(self, limit=None):
        with self.session() as session:
            needed_consents = session.query(
                ConsentFile
            ).outerjoin(
                HealthProConsentFile,
                HealthProConsentFile.consent_file_id == ConsentFile.id
            ).filter(
                ConsentFile.file_exists == 1,
                ConsentFile.file_path.isnot(None),
                HealthProConsentFile.id.is_(None)
            )

            if limit:
                needed_consents = needed_consents.limit(limit)

            return needed_consents.all()

    def get_by_participant(self, participant_id):
        with self.session() as session:
            return session.query(
                HealthProConsentFile.file_path.label('file_path'),
                HealthProConsentFile.participant_id.label('participant_id'),
                ConsentFile.type.label('consent_type')
            ).join(
                ConsentFile,
                ConsentFile.id == HealthProConsentFile.consent_file_id
            ).filter(
                HealthProConsentFile.participant_id == participant_id,
                HealthProConsentFile.file_path.isnot(None)
            ).all()
