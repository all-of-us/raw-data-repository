from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin


class PPSCDataTransferTest(GenomicDataGenMixin):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()

    def set_data_elements(self) -> None:
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
        # self.clear_table_after_test("ppsc.activity")
        # self.clear_table_after_test("ppsc.participant")
        # self.clear_table_after_test("ppsc.participant_event_activity")
        # self.clear_table_after_test("ppsc.enrollment_event_type")
        # self.clear_table_after_test("ppsc.enrollment_event")
