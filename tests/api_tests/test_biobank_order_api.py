import datetime
import http.client
import mock

from rdr_service.clock import FakeClock
from rdr_service.code_constants import CONSENT_PERMISSION_YES_CODE, RACE_NONE_OF_THESE_CODE
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_order import (
    BiobankOrderHistory,
    BiobankOrderIdentifierHistory,
    BiobankOrderedSampleHistory,
)
from rdr_service.model.participant import Participant
from rdr_service.model.utils import from_client_participant_id, to_client_participant_id
from rdr_service.participant_enums import OrderStatus, UNSET_HPO_ID
from tests.api_tests.test_participant_summary_api import _add_code_answer
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import load_biobank_order_json, load_measurement_json

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_5 = datetime.datetime(2016, 1, 5, 0, 1)
TIME_6 = datetime.datetime(2015, 1, 1)


class BiobankOrderApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.participant = Participant(participantId=123, biobankId=555)
        self.participant_dao = ParticipantDao()
        self.participant_dao.insert(self.participant)
        self.summary_dao = ParticipantSummaryDao()
        self.bio_dao = BiobankOrderDao()
        self.path = "Participant/%s/BiobankOrder" % to_client_participant_id(self.participant.participantId)
        self.mayolink_response = {
            "orders": {
                "order": {
                    "status": "Queued",
                    "reference_number": "somebarcodenumber",
                    "received": "2016-12-01T12:00:00-05:00",
                    "number": "WEB1ABCD1234",
                    "patient": {"medical_record_number": "PAT-123-456"},
                }
            }
        }

        mayolinkapi_patcher = mock.patch(
            "rdr_service.dao.biobank_order_dao.MayoLinkClient",
            **{"return_value.post.return_value": self.mayolink_response}
        )
        mayolinkapi_patcher.start()
        self.addCleanup(mayolinkapi_patcher.stop)

    @mock.patch('rdr_service.dao.biobank_order_dao.get_account_origin_id')
    def test_create_quest_order(self, quest_origin):
        quest_origin.return_value = 'careevolution'
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="quest_biobank_order_1.json")
        result = self.send_post(self.path, order_json)
        self.assertEqual(result['id'], 'WEB1ABCD1234')
        self.assertEqual(result['collectedInfo']['address'], {'city': 'Little Rock', 'line': ['address1', 'address2'],
                                                              'postalCode': '72205-5302', 'state': 'AR'})

    @mock.patch('rdr_service.dao.biobank_order_dao.get_account_origin_id')
    def test_update_biobank_order_from_different_origin(self, quest_origin):
        quest_origin.return_value = 'careevolution'
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="quest_biobank_order_1.json")
        result = self.send_post(self.path, order_json)
        self.assertEqual(result['id'], 'WEB1ABCD1234')

        quest_origin.return_value = 'hpro'
        update_path = self.path + "/" + 'WEB1ABCD1234'
        update_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        update_json['identifier'][1]['value'] = 'WEB1ABCD1234'
        self.send_put(update_path, request_data=update_json, headers={"If-Match": 'W/"1"'},
                      expected_status=http.client.BAD_REQUEST)


    @mock.patch('rdr_service.dao.biobank_order_dao.get_account_origin_id')
    def test_get_orders_by_participant_id(self, quest_origin):
        quest_origin.return_value = 'careevolution'
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="quest_biobank_order_1.json")
        self.send_post(self.path, order_json)

        quest_origin.return_value = 'hpro'
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        self.send_post(self.path, order_json)

        get_path = "Participant/%s/BiobankOrder" % to_client_participant_id(self.participant.participantId)
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 2)
        self.assertEqual(len(result['data']), 2)
        if result['data'][0]['origin'] == 'careevolution':
            self.assertEqual(result['data'][1]['origin'], 'hpro')
        else:
            self.assertEqual(result['data'][1]['origin'], 'careevolution')

    @mock.patch('rdr_service.dao.biobank_order_dao.get_account_origin_id')
    def test_get_orders_by_kit_id(self, quest_origin):
        quest_origin.return_value = 'careevolution'
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="quest_biobank_order_1.json")
        self.send_post(self.path, order_json)

        quest_origin.return_value = 'hpro'
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        self.send_post(self.path, order_json)

        get_path = "BiobankOrder?kitId=KIT-12345678"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 1)
        self.assertEqual(len(result['data']), 1)
        self.assertEqual(result['data'][0]['origin'], 'careevolution')

    @mock.patch('rdr_service.dao.biobank_order_dao.get_account_origin_id')
    def test_get_orders_by_time_range(self, quest_origin):
        quest_origin.return_value = 'careevolution'
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="quest_biobank_order_1.json")
        self.send_post(self.path, order_json)

        quest_origin.return_value = 'hpro'
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        self.send_post(self.path, order_json)

        p2 = Participant(participantId=456, biobankId=666)
        self.participant_dao.insert(p2)
        self.summary_dao.insert(self.participant_summary(p2))
        order_json = load_biobank_order_json(p2.participantId, filename="biobank_order_2.json")
        p2_path = "Participant/%s/BiobankOrder" % to_client_participant_id(p2.participantId)
        self.send_post(p2_path, order_json)

        p3 = Participant(participantId=789, biobankId=777)
        self.participant_dao.insert(p3)
        self.summary_dao.insert(self.participant_summary(p3))
        order_json = load_biobank_order_json(p3.participantId, filename="biobank_order_2.json")
        p3_path = "Participant/%s/BiobankOrder" % to_client_participant_id(p3.participantId)
        self.send_post(p3_path, order_json)

        p4 = Participant(participantId=1244, biobankId=888)
        self.participant_dao.insert(p4)
        self.summary_dao.insert(self.participant_summary(p4))
        order_json = load_biobank_order_json(p4.participantId, filename="biobank_order_4.json")
        p4_path = "Participant/%s/BiobankOrder" % to_client_participant_id(p4.participantId)
        self.send_post(p4_path, order_json)
        get_path = "BiobankOrder?origin=hpro&startDate=2016-01-04&endDate=2016-01-05&page=1&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 3)
        self.assertEqual(len(result['data']), 2)
        self.assertIn(result['data'][0]['biobankId'][1:], ('555', '666'))
        get_path = "BiobankOrder?origin=hpro&startDate=2016-01-03&endDate=2016-01-04&page=2&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 3)
        self.assertEqual(len(result['data']), 1)
        get_path = "BiobankOrder?origin=hpro&startDate=2016-01-03&endDate=2016-01-04&page=3&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 3)
        self.assertEqual(len(result['data']), 0)
        get_path = "BiobankOrder?origin=hpro&startDate=2019-12-03&endDate=2019-12-04&page=1&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 0)
        self.assertEqual(len(result['data']), 0)
        get_path = "BiobankOrder?origin=careevolution&startDate=2019-12-03&endDate=2019-12-04&page=1&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 1)
        self.assertEqual(len(result['data']), 1)
        get_path = "BiobankOrder?origin=careevolution&state=AR&page=1&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 1)
        self.assertEqual(len(result['data']), 1)
        get_path = "BiobankOrder?origin=careevolution&state=TN&page=1&pageSize=2"
        result = self.send_get(get_path)
        self.assertEqual(result['total'], 0)
        self.assertEqual(len(result['data']), 0)

    def test_cancel_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)
        full_order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_1.json")
        _strip_fields(result)
        _strip_fields(full_order_json)
        self.assertEqual(full_order_json, result)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all wrong",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }
        cancelled_order = self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"1"'})
        get_cancelled_order = self.send_get(path)
        get_summary = self.summary_dao.get(self.participant.participantId)

        self.assertEqual(get_summary.biospecimenSourceSiteId, None)
        self.assertEqual(get_summary.biospecimenCollectedSiteId, None)
        self.assertEqual(get_summary.biospecimenOrderTime, None)
        self.assertEqual(get_summary.biospecimenStatus, None)
        self.assertEqual(get_summary.biospecimenFinalizedSiteId, None)
        self.assertEqual(get_summary.biospecimenProcessedSiteId, None)
        self.assertEqual(get_summary.sampleOrderStatus2ED10, None)
        self.assertEqual(get_summary.sampleOrderStatus2ED10Time, None)
        self.assertEqual(get_summary.sampleStatus2ED10, None)
        self.assertEqual(get_summary.sampleStatus2ED10Time, None)
        self.assertEqual(get_summary.sampleOrderStatus1PST8, None)
        self.assertEqual(get_summary.sampleOrderStatus1PST8Time, None)
        self.assertEqual(get_summary.sampleStatus1PST8, None)
        self.assertEqual(get_summary.sampleStatus1PST8Time, None)
        self.assertEqual(get_summary.sampleOrderStatus1PS08, None)
        self.assertEqual(get_summary.sampleOrderStatus1PS08Time, None)
        self.assertEqual(get_summary.sampleStatus1PS08, None)
        self.assertEqual(get_summary.sampleStatus1PS08Time, None)
        self.assertEqual(get_summary.sampleOrderStatus2PST8, None)
        self.assertEqual(get_summary.sampleOrderStatus2PST8Time, None)
        self.assertEqual(get_summary.sampleStatus2PST8, None)
        self.assertEqual(get_summary.sampleStatus2PST8Time, None)
        self.assertEqual(get_summary.sampleOrderStatus1PXR2, None)
        self.assertEqual(get_summary.sampleOrderStatus1PXR2Time, None)
        self.assertEqual(get_summary.sampleStatus1PXR2, None)
        self.assertEqual(get_summary.sampleStatus1PXR2Time, None)
        self.assertEqual(get_summary.sampleOrderStatus1CFD9, None)
        self.assertEqual(get_summary.sampleOrderStatus1CFD9Time, None)
        self.assertEqual(get_summary.sampleStatus1CFD9, None)
        self.assertEqual(get_summary.sampleStatus1CFD9Time, None)
        self.assertEqual(get_summary.sampleOrderStatus1ED02, None)
        self.assertEqual(get_summary.sampleOrderStatus1ED02Time, None)
        self.assertEqual(get_summary.sampleStatus1ED02, None)
        self.assertEqual(get_summary.sampleStatus1ED02Time, None)
        self.assertEqual(cancelled_order, get_cancelled_order)
        self.assertEqual(get_cancelled_order["status"], "CANCELLED")
        self.assertEqual(get_cancelled_order["amendedReason"], "Its all wrong")
        self.assertEqual(get_cancelled_order["cancelledInfo"]["author"]["value"], "fred@pmi-ops.org")
        self.assertEqual(get_cancelled_order["cancelledInfo"]["site"]["value"], "hpo-site-monroeville")

    def test_you_can_not_cancel_a_cancelled_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all wrong",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }
        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"1"'})

        self.send_patch(
            path, request_data=request_data, headers={"If-Match": 'W/"2"'}, expected_status=http.client.BAD_REQUEST
        )

    def test_cancel_one_order_with_another_good_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_1.json")
        order_json2 = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        order_json2['identifier'][0]['value'] = 'healthpro-order-id-1231234'
        order_json2['identifier'][1]['value'] = 'WEB1YLHV1234'
        result = self.send_post(self.path, order_json)
        self.send_post(self.path, order_json2)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all wrong",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }
        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"1"'})

        self.send_patch(
            path, request_data=request_data, headers={"If-Match": 'W/"2"'}, expected_status=http.client.BAD_REQUEST
        )

        get_summary = self.summary_dao.get(self.participant.participantId)

        self.assertEqual(get_summary.biospecimenSourceSiteId, 1)
        self.assertEqual(get_summary.biospecimenCollectedSiteId, 1)
        self.assertEqual(get_summary.biospecimenFinalizedSiteId, 2)

    def test_you_can_not_restore_a_not_cancelled_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all wrong",
            "restoredInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "restored",
        }
        self.send_patch(
            path, request_data=request_data, headers={"If-Match": 'W/"1"'}, expected_status=http.client.BAD_REQUEST
        )

    def test_restore_an_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)
        full_order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_1.json")
        _strip_fields(result)
        _strip_fields(full_order_json)
        self.assertEqual(full_order_json, result)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all wrong",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }
        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"1"'})

        request_data = {
            "amendedReason": "I didnt mean to cancel",
            "restoredInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "restored",
        }

        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"2"'})
        restored_order = self.send_get(path)
        get_summary = self.summary_dao.get(self.participant.participantId)
        self.assertEqual(get_summary.sampleOrderStatus1SST8, OrderStatus.CREATED)
        self.assertEqual(get_summary.sampleOrderStatus2ED10, OrderStatus.CREATED)
        self.assertEqual(get_summary.sampleOrderStatus1SAL, OrderStatus.CREATED)
        self.assertEqual(get_summary.sampleOrderStatus1UR10, OrderStatus.CREATED)
        self.assertEqual(get_summary.sampleOrderStatus1CFD9, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.sampleOrderStatus1ED02, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.sampleOrderStatus2SST8, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.sampleOrderStatus2PST8, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.biospecimenFinalizedSiteId, 2)
        self.assertEqual(restored_order["status"], "UNSET")
        self.assertEqual(restored_order["restoredInfo"]["author"]["value"], "fred@pmi-ops.org")
        self.assertEqual(restored_order["restoredInfo"]["site"]["value"], "hpo-site-monroeville")
        self.assertEqual(restored_order["amendedReason"], "I didnt mean to cancel")

    def test_amending_an_order(self):
        # pylint: disable=unused-variable
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all better",
            "amendedInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-bannerphoenix"},
            },
        }

        biobank_order_identifiers = {
            "created": "2018-02-21T16:25:12",
            "createdInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "nobody@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-clinic-phoenix"},
            },
        }
        get_order = self.send_get(path)
        full_order = get_order.copy()
        full_order.update(request_data)
        full_order.update(biobank_order_identifiers)

        self.assertEqual(len(full_order["samples"]), 16)
        del full_order["samples"][0]

        self.send_put(path, request_data=full_order, headers={"If-Match": 'W/"1"'})

        get_amended_order = self.send_get(path)
        get_summary = self.summary_dao.get(self.participant.participantId)
        self.assertEqual(get_summary.biospecimenProcessedSiteId, 1)
        self.assertEqual(get_summary.biospecimenFinalizedSiteId, 2)
        self.assertEqual(get_summary.biospecimenCollectedSiteId, 1)
        self.assertEqual(get_summary.sampleOrderStatus2PST8, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.sampleOrderStatus1PS08, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.sampleOrderStatus1PST8, OrderStatus.FINALIZED)
        self.assertEqual(get_summary.sampleOrderStatus1SST8, OrderStatus.CREATED)
        self.assertEqual(get_summary.sampleOrderStatus2ED10, OrderStatus.CREATED)
        self.assertEqual(len(get_amended_order["samples"]), 15)
        self.assertEqual(get_amended_order["meta"], {"versionId": 'W/"2"'})
        self.assertEqual(get_amended_order["amendedReason"], "Its all better")
        self.assertEqual(get_amended_order["amendedInfo"]["author"]["value"], "fred@pmi-ops.org")
        self.assertEqual(get_amended_order["amendedInfo"]["site"]["value"], "hpo-site-bannerphoenix")
        self.assertEqual(get_amended_order["createdInfo"]["site"]["value"], "hpo-site-clinic-phoenix")
        self.assertEqual(get_amended_order["createdInfo"]["author"]["value"], "nobody@pmi-ops.org")
        self.assertEqual(get_amended_order["created"], "2018-02-21T16:25:12")
        self.assertEqual(get_amended_order["status"], "AMENDED")

    def test_amend_a_restored_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)
        full_order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_1.json")
        _strip_fields(result)
        _strip_fields(full_order_json)

        biobank_order_id = result["identifier"][1]["value"]
        path = self.path + "/" + biobank_order_id
        request_data = {
            "amendedReason": "Its all wrong",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }
        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"1"'})
        self.send_get(path)
        request_data = {
            "amendedReason": "I didnt mean to cancel",
            "restoredInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "restored",
        }

        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"2"'})

        request_data = {
            "amendedReason": "Its all better",
            "samples": [
                {
                    "test": "1ED10",
                    "description": "EDTA 10 mL (1)",
                    "processingRequired": False,
                    "collected": "2016-01-04T09:45:49Z",
                    "finalized": "2016-01-04T10:55:41Z",
                },
                {
                    "test": "1PST8",
                    "description": "Plasma Separator 8 mL",
                    "collected": "2016-01-04T09:45:49Z",
                    "processingRequired": True,
                    "processed": "2016-01-04T10:28:50Z",
                    "finalized": "2016-01-04T10:55:41Z",
                },
            ],
            "amendedInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "mike@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
        }
        get_order = self.send_get(path)
        full_order = get_order.copy()
        full_order.update(request_data)
        self.send_put(path, request_data=full_order, headers={"If-Match": 'W/"3"'})

        get_amended_order = self.send_get(path)
        self.assertEqual(len(get_amended_order["samples"]), 2)
        self.assertEqual(get_amended_order["amendedInfo"]["author"]["value"], "mike@pmi-ops.org")
        self.assertEqual(get_amended_order["status"], "AMENDED")
        self.assertEqual(get_amended_order.get("restoredSiteId"), None)
        self.assertEqual(get_amended_order.get("restoredUsername"), None)
        self.assertEqual(get_amended_order.get("restoredTime"), None)
        self.assertEqual(get_amended_order["meta"], {"versionId": 'W/"4"'})

    def test_insert_and_refetch(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        self.create_and_verify_created_obj(self.path, load_biobank_order_json(self.participant.participantId))

    def test_insert_new_order(self):
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)
        full_order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_1.json")
        _strip_fields(result)
        _strip_fields(full_order_json)
        self.assertEqual(full_order_json, result)

        # check order origin
        biobank_order_id = result["identifier"][1]["value"]
        get_path = "Participant/{}/BiobankOrder/{}".format(to_client_participant_id(123), biobank_order_id)
        get_result = self.send_get(get_path)
        self.assertEqual(get_result["origin"], "example")

    def test_biobank_history_on_insert(self):
        with self.bio_dao.session() as session:
            self.summary_dao.insert(self.participant_summary(self.participant))
            order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
            result = self.send_post(self.path, order_json)
            load_biobank_order_json(self.participant.participantId, filename="biobank_order_1.json")
            order_history = session.query(BiobankOrderHistory).first()
            identifier_history = session.query(BiobankOrderIdentifierHistory).first()
            sample_history = session.query(BiobankOrderedSampleHistory).first()
            all_samples_history = session.query(BiobankOrderedSampleHistory).all()

            self.assertEqual(result["id"], order_history.biobankOrderId)
            self.assertEqual(identifier_history.biobankOrderId, result["id"])
            self.assertEqual(sample_history.biobankOrderId, result["id"])
            self.assertEqual(result["meta"]["versionId"], 'W/"1"')
            self.assertEqual(order_history.version, 1)
            self.assertEqual(len(all_samples_history), 16)

            # Test history on updates...
            biobank_order_id = result["identifier"][1]["value"]
            path = self.path + "/" + biobank_order_id
            request_data = {
                "amendedReason": "Its all better",
                "amendedInfo": {
                    "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                    "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-bannerphoenix"},
                },
            }

            biobank_order_identifiers = {
                "created": "2018-02-21T16:25:12",
                "createdInfo": {
                    "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "nobody@pmi-ops.org"},
                    "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-clinic-phoenix"},
                },
            }
            get_order = self.send_get(path)
            full_order = get_order.copy()
            full_order.update(request_data)
            full_order.update(biobank_order_identifiers)

            self.assertEqual(len(full_order["samples"]), 16)
            del full_order["samples"][0]

            self.send_put(path, request_data=full_order, headers={"If-Match": 'W/"1"'})

            with self.bio_dao.session() as session:
                amended_order = self.send_get(path)
                second_order_history = session.query(BiobankOrderHistory).filter_by(version=2).first()
                second_order_samples = session.query(BiobankOrderedSampleHistory).filter_by(version=2).first()
                second_order_identifier = session.query(BiobankOrderIdentifierHistory).filter_by(version=2).first()
                self.assertEqual(second_order_history.biobankOrderId, amended_order["id"])
                self.assertEqual(second_order_identifier.biobankOrderId, amended_order["id"])
                self.assertEqual(second_order_samples.biobankOrderId, amended_order["id"])

                # Check that original order hasn't changed in history
                original = session.query(BiobankOrderHistory).filter_by(version=1).first()
                self.assertEqual(original.asdict(), order_history.asdict())

    def test_error_no_summary(self):
        order_json = load_biobank_order_json(self.participant.participantId)
        self.send_post(self.path, order_json, expected_status=http.client.BAD_REQUEST)

    def test_error_missing_required_fields(self):
        order_json = load_biobank_order_json(self.participant.participantId)
        del order_json["identifier"]
        self.send_post(self.path, order_json, expected_status=http.client.BAD_REQUEST)

    def test_no_duplicate_test_within_order(self):
        order_json = load_biobank_order_json(self.participant.participantId)
        order_json["samples"].extend(list(order_json["samples"]))
        self.send_post(self.path, order_json, expected_status=http.client.BAD_REQUEST)

    def test_auto_pair_updates_participant_and_summary(self):
        self.summary_dao.insert(self.participant_summary(self.participant))

        # Sanity check: No HPO yet.
        p_unpaired = self.participant_dao.get(self.participant.participantId)
        self.assertEqual(p_unpaired.hpoId, UNSET_HPO_ID)
        self.assertIsNone(p_unpaired.providerLink)
        s_unpaired = self.summary_dao.get(self.participant.participantId)
        self.assertEqual(s_unpaired.hpoId, UNSET_HPO_ID)

        self.send_post(self.path, load_biobank_order_json(self.participant.participantId))

        # Some HPO has been set. (ParticipantDao tests cover more detailed cases / specific values.)
        p_paired = self.participant_dao.get(self.participant.participantId)
        self.assertNotEqual(p_paired.hpoId, UNSET_HPO_ID)
        self.assertIsNotNone(p_paired.providerLink)

        s_paired = self.summary_dao.get(self.participant.participantId)

        self.assertNotEqual(s_paired.hpoId, UNSET_HPO_ID)
        self.assertEqual(s_paired.biospecimenCollectedSiteId, s_paired.siteId)
        self.assertNotEqual(s_paired.biospecimenCollectedSiteId, s_paired.biospecimenFinalizedSiteId)

        self.assertNotEqual(s_paired.siteId, s_paired.physicalMeasurementsCreatedSiteId)
        self.assertNotEqual(s_paired.siteId, s_paired.physicalMeasurementsFinalizedSiteId)

    def test_not_pairing_at_pm_when_has_bio(self):
        self.participant_id = self.create_participant()
        _id = int(self.participant_id[1:])
        self.path = "Participant/%s/BiobankOrder" % to_client_participant_id(_id)
        pid_numeric = from_client_participant_id(self.participant_id)
        self.send_consent(self.participant_id)
        self.send_post(self.path, load_biobank_order_json(pid_numeric))
        participant_paired = self.summary_dao.get(pid_numeric)

        self.assertEqual(participant_paired.siteId, participant_paired.biospecimenCollectedSiteId)
        self.path = "Participant/%s/PhysicalMeasurements" % to_client_participant_id(pid_numeric)
        self._insert_measurements(datetime.datetime.utcnow().isoformat())
        self.assertNotEqual(participant_paired.siteId, participant_paired.physicalMeasurementsFinalizedSiteId)

    def test_bio_after_cancelled_pm(self):
        self.participant_id = self.create_participant()
        self.send_consent(self.participant_id)
        measurement = load_measurement_json(self.participant_id)
        measurement2 = load_measurement_json(self.participant_id)

        # send both PM's
        pm_path = "Participant/%s/PhysicalMeasurements" % self.participant_id
        response = self.send_post(pm_path, measurement)
        self.send_post(pm_path, measurement2)

        # cancel the 1st PM
        pm_path = pm_path + "/" + response["id"]
        cancel_info = self.get_restore_or_cancel_info()
        self.send_patch(pm_path, cancel_info)

        # set up questionnaires to hit the calculate_max_core_sample_time in participant summary
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        self._submit_consent_questionnaire_response(
            self.participant_id, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        self.submit_questionnaire_response(
            self.participant_id, questionnaire_id, RACE_NONE_OF_THESE_CODE, None, None, datetime.date(1978, 10, 10)
        )

        self._submit_empty_questionnaire_response(self.participant_id, questionnaire_id_2)

        # send a biobank order
        _id = int(self.participant_id[1:])
        self.path = "Participant/%s/BiobankOrder" % to_client_participant_id(_id)
        pid_numeric = from_client_participant_id(self.participant_id)
        self.send_post(self.path, load_biobank_order_json(pid_numeric))

        # fetch participant summary
        ps = self.send_get("ParticipantSummary?participantId=%s" % _id)

        self.assertTrue(ps["entry"][0]["resource"]["physicalMeasurementsFinalizedTime"])
        self.assertEqual(ps["entry"][0]["resource"]["physicalMeasurementsFinalizedSite"], "hpo-site-bannerphoenix")
        self.assertIsNotNone("biobankId", ps["entry"][0]["resource"])

    def _insert_measurements(self, now=None):
        measurements_1 = load_measurement_json(self.participant_id, now)
        path_1 = "Participant/%s/PhysicalMeasurements" % self.participant_id
        self.send_post(path_1, measurements_1)

    def _submit_consent_questionnaire_response(
        self, participant_id, questionnaire_id, ehr_consent_answer, time=TIME_1
    ):
        code_answers = []
        _add_code_answer(code_answers, "ehrConsent", ehr_consent_answer)
        qr = self.make_questionnaire_response_json(participant_id, questionnaire_id, code_answers=code_answers)
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def _submit_empty_questionnaire_response(self, participant_id, questionnaire_id, time=TIME_1):
        qr = self.make_questionnaire_response_json(participant_id, questionnaire_id)
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)


def _strip_fields(order_json):
    if order_json.get("created"):
        del order_json["created"]
    if order_json.get("id"):
        del order_json["id"]
    if order_json.get("origin"):
        del order_json["origin"]
    if order_json.get("version"):
        del order_json["version"]
    for sample in order_json["samples"]:
        if sample.get("collected"):
            del sample["collected"]
        if sample.get("processed"):
            del sample["processed"]
        if sample.get("finalized"):
            del sample["finalized"]
