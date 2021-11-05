import logging

from datetime import datetime

from rdr_service import config
from rdr_service.dao.hpro_consent_dao import HealthProConsentDao
from rdr_service.storage import GoogleCloudStorageProvider


class HealthProConsentFile:
    """
    Service used for transferring Consent records
    from RDR to HealthPro consent bucket
    """
    def __init__(
        self,
        logger=None,
        store_failures=False
    ):
        self.dao = HealthProConsentDao()
        self.storage_provider = GoogleCloudStorageProvider()
        self.consents_for_transfer = None
        self.hpro_bucket = config.getSetting(config.HEALTHPRO_CONSENT_BUCKET)
        self.transfer_limit = None
        self.transfer_count = 0
        self.transfer_failures = []
        self.logger = logger or logging
        self.store_failures = store_failures

    def initialize_consent_transfer(self):
        self.get_consents_for_transfer()

        if not self.consents_for_transfer:
            self.logger.info('No consents ready for transfer')
            return

        self.cp_consent_files()

    def get_consents_for_transfer(self):
        self.consents_for_transfer = self.dao.get_needed_consents_for_transfer(self.transfer_limit)

    def cp_consent_files(self):
        self.logger.info(f'Ready to transfer {len(self.consents_for_transfer)} consent(s) to {self.hpro_bucket} bucket')

        for consent in self.consents_for_transfer:
            src = consent.file_path
            dest = self.create_path_destination(src)
            obj = self.make_object(consent, dest)

            try:
                self.storage_provider.copy_blob(src, dest)
                self.transfer_count += 1
                self.dao.insert(obj)

            # pylint: disable=broad-except
            except Exception:
                if self.store_failures:
                    self.transfer_failures.append({
                        'original': consent.file_path,
                        'destination': dest,
                    })
                self.logger.warning(f'Healthpro consent {src} failed to transfer to {dest}', exc_info=True)

        self.logger.info(f'Healthpro consent(s) {self.transfer_count} transferred to {self.hpro_bucket} bucket')

    def create_path_destination(self, file_path):
        dest_base = "/".join(file_path.strip("/").split('/')[1:])
        dest = f'{self.hpro_bucket}/{dest_base}'
        return dest

    def make_object(self, obj, dest):
        obj = self.dao.model_type(
            participant_id=obj.participant_id,
            consent_file_id=obj.id,
            file_path=dest,
            file_upload_time=datetime.utcnow()
        )
        return obj




