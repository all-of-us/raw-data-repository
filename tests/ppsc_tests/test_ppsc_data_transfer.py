from rdr_service.dao.ppsc_dao import PPSCDataTransferAuthDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin


class PPSCDataTransferTest(GenomicDataGenMixin):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.oauth_dao = PPSCDataTransferAuthDao()

    def set_data_elements(self) -> None:
        ...

    def build_oauth_data(self):
        oauth = {}
        self.oauth_dao.insert(self.oauth_dao.model_type(**oauth))

    def build_endpoint_data(self) -> None:
        ...

    def test_ouath_token_generation(self) -> None:
        ...

    def send_core_items_for_transfer(self) -> None:
        ...

    def send_ehr_items_for_transfer(self) -> None:
        ...

    def send_health_data_items_for_transfer(self) -> None:
        ...

    def send_biobank_sample_items_for_transfer(self) -> None:
        ...

    def tearDown(self):
        super().tearDown()
