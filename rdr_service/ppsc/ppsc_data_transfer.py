from abc import ABC


class PPSCTransferOauth:

    def __init__(self):
        self.dao = ''

    def generate_token(self):
        ...


class BaseDataTransfer(ABC):

    def __init__(self):
        self.auth_token = None

    def run_data_transfer(self):
        ...

    def check_oauth_token(self):
        ...

    def get_endpoint_for_transfer(self):
        ...

    def send_item(self):
        ...


class PPSCDataTransferCore(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = ''


class PPSCDataTransferEHR(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = ''


class PPSCDataTransferBiobank(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = ''


class PPSCDataTransferHealthData(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = ''


