# import http.client
# import random
# from copy import deepcopy
#
# from rdr_service import config
# from rdr_service.api_util import PPSC, RDR, HEALTHPRO
# from rdr_service.dao.participant_dao import ParticipantDao as LegacyParticipantDao
# from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
# from rdr_service.dao.ppsc_dao import ParticipantDao, PPSCDefaultBaseDao
# from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
# from rdr_service.model.ppsc import ParticipantEventActivity, EnrollmentEvent
# from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
#
#
# class PPSCDataTransferTest(GenomicDataGenMixin):
#     def setUp(self):
#         super().setUp()
#         self.ppsc_data_gen = PPSCDataGenerator()
#
#
#     def set_data_elements(self, el_type: str) -> None:
#         ...
#
#     def test_ouath_token_generation(self):
#         ...
#
#     def tearDown(self):
#         super().tearDown()
#         # self.clear_table_after_test("ppsc.activity")
#         # self.clear_table_after_test("ppsc.participant")
#         # self.clear_table_after_test("ppsc.participant_event_activity")
#         # self.clear_table_after_test("ppsc.enrollment_event_type")
#         # self.clear_table_after_test("ppsc.enrollment_event")
