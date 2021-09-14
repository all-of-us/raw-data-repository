
from rdr_service.dao.hpro_consent_dao import HProConsentDao


class HproConsentFile:
    """
    """
    def __init__(self):
        self.dao = HProConsentDao()
        self.consents_for_transfer = None

    @staticmethod
    def valid_message_data(message):
        pass

    def cp_consent_files(self):
        if self.consents_for_transfer:
            pass


