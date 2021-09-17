import logging

from datetime import datetime

from rdr_service import config
from rdr_service.dao.hpro_consent_dao import HealthProConsentDao
from rdr_service.services.gcp_utils import gcp_cp


class HealthProConsentFile:
    """
    Service used for transferring Consent records
    from RDR to HealthPro consent bucket
    """
    def __init__(self):
        self.dao = HealthProConsentDao()
        self.consents_for_transfer = None
        self.hpro_bucket = config.getSetting(config.HEALTHPRO_CONSENT_BUCKET)
        self.transfer_limit = None
        self.transfer_count = 0

    def initialize_consent_transfer(self):
        self.get_consents_for_transfer()

        if not self.consents_for_transfer:
            logging.info('No consents ready for transfer')
            return

        self.cp_consent_files()

    def get_consents_for_transfer(self):
        self.consents_for_transfer = self.dao.get_needed_consents_for_transfer(self.transfer_limit)

    def cp_consent_files(self):
        logging.info(f'Ready to transfer {len(self.consents_for_transfer)} consent(s) to {self.hpro_bucket}')

        for consent in self.consents_for_transfer:
            src = f'gs://{consent.file_path}'
            dest = self.create_path_destination(consent.file_path)
            obj = self.make_object(consent, dest)

            transfer = gcp_cp(src, dest)
            if transfer:
                self.transfer_count += 1
                self.dao.insert(obj)

        logging.info(f'{self.transfer_count} consent(s) transferred to {self.hpro_bucket}')

    def create_path_destination(self, file_path):
        dest_base = "/".join(file_path.strip("/").split('/')[1:])
        dest = f'gs://{self.hpro_bucket}/{dest_base}'
        return dest

    def make_object(self, obj, dest):
        obj = self.dao.model_type(
            participant_id=obj.participant_id,
            consent_file_id=obj.id,
            file_path=dest.split('gs://')[1] if 'gs://' in dest else dest,
            file_upload_time=datetime.utcnow()
        )
        return obj




