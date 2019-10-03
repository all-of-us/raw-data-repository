import http.client
import json

import mock
from werkzeug.exceptions import ServiceUnavailable

from rdr_service.api_util import VIBRENT_FHIR_URL, parse_date, get_code_id
from rdr_service.dao.dv_order_dao import DvOrderDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.biobank_dv_order import BiobankDVOrder
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.fhir_utils import SimpleFhirR4Reader
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import OrderShipmentStatus, OrderShipmentTrackingStatus
from tests.test_data import load_test_data_json
from tests.helpers.unittest_base import BaseTestCase

from collections import namedtuple

class DvOrderDaoTestBase(BaseTestCase):
    def setUp(self):
        super().setUp()

        self.post_delivery = load_test_data_json("dv_order_api_post_supply_delivery.json")
        self.put_delivery = load_test_data_json("dv_order_api_put_supply_delivery.json")
        self.post_request = load_test_data_json("dv_order_api_post_supply_request.json")
        self.put_request = load_test_data_json("dv_order_api_put_supply_request.json")
        self.dao = DvOrderDao()

        self.code_dao = CodeDao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

        self.participant = Participant(participantId=123456789, biobankId=7)
        self.participant_dao.insert(self.participant)
        self.summary = self.participant_summary(self.participant)
        self.summary_dao.insert(self.summary)
        self.mayolink_response = {
            "orders": {
                "order": {
                    "status": "finished",
                    "reference_number": "barcode",
                    "received": "2019-04-05 12:00:00",
                    "number": "12345",
                    "patient": {"medical_record_number": "WEB1ABCD1234"},
                }
            }
        }

        mayolinkapi_patcher = mock.patch(
            "rdr_service.dao.dv_order_dao.MayoLinkApi", **{"return_value.post.return_value": self.mayolink_response}
        )
        mayolinkapi_patcher.start()
        self.addCleanup(mayolinkapi_patcher.stop)

    def test_insert_biobank_order(self):
        payload = self.send_post("SupplyRequest", request_data=self.post_request, expected_status=http.client.CREATED)
        request_response = json.loads(payload.response[0])
        location = payload.location.rsplit("/", 1)[-1]
        put_response = self.send_put("SupplyRequest/{}".format(location), request_data=self.put_request)
        payload = self.send_post(
            "SupplyDelivery", request_data=self.post_delivery, expected_status=http.client.CREATED
        )
        post_response = json.loads(payload.response[0])
        location = payload.location.rsplit("/", 1)[-1]
        put_response = self.send_put("SupplyDelivery/{}".format(location), request_data=self.put_delivery)
        self.assertEqual(request_response["version"], 1)
        self.assertEqual(post_response["version"], 3)
        self.assertEqual(post_response["meta"]["versionId"].strip("W/"), '"3"')
        self.assertEqual(put_response["version"], 4)
        self.assertEqual(put_response["meta"]["versionId"].strip("W/"), '"4"')
        self.assertEqual(put_response["barcode"], "SABR90160121INA")
        # self.assertEqual(put_response["biobankOrderId"], "12345")
        self.assertEqual(put_response["biobankStatus"], "Delivered")
        self.assertEqual(put_response["order_id"], 999999)

    def test_enumerate_shipping_status(self):
        fhir_resource = SimpleFhirR4Reader(self.post_request)
        status = self.dao._enumerate_order_shipping_status(fhir_resource.status)
        self.assertEqual(status, OrderShipmentStatus.SHIPPED)

    def test_enumerate_tracking_status(self):
        fhir_resource = SimpleFhirR4Reader(self.post_delivery)
        status = self.dao._enumerate_order_tracking_status(
            fhir_resource.extension.get(url=VIBRENT_FHIR_URL + "tracking-status").valueString
        )
        self.assertEqual(status, OrderShipmentTrackingStatus.IN_TRANSIT)

    def test_from_client_json(self):
        self.make_supply_posts()

        expected_result = self.build_expected_data(self.post_delivery)

        result_from_dao = self.dao.from_client_json(self.post_delivery, participant_id=self.participant.participantId)

        # run tests against result_from_dao
        for i, test_field in enumerate(expected_result):
            self.assertEqual(test_field, getattr(result_from_dao, expected_result._fields[i]))

    def test_dv_order_post_inserted_correctly(self):

        expected_result = self.build_expected_data(self.post_delivery)

        # make posts to create
        self.make_supply_posts()

        # return a BiobankDVOrder object from database
        with self.dao.session() as session:
            dv_order_result = session.query(BiobankDVOrder).filter_by(participantId=self.participant.participantId).first()

        # run tests against dv_order_result
        for i, test_field in enumerate(expected_result):
            self.assertEqual(test_field, getattr(dv_order_result, expected_result._fields[i]))

    @mock.patch("rdr_service.dao.dv_order_dao.MayoLinkApi")
    def test_service_unavailable(self, mocked_api):
        # pylint: disable=unused-argument
        def raises(*args):
            raise ServiceUnavailable()

        with self.assertRaises(ServiceUnavailable):
            mocked_api.return_value.post.side_effect = raises
            self.dao.send_order(self.post_delivery, self.participant.participantId)

    def build_expected_data(self, json_data):
        """Helper function to build the data we are testing against from the test-data file."""
        fhir_resource = SimpleFhirR4Reader(json_data)

        # add fields to test
        test_fields = {
            'shipmentEstArrival': parse_date(fhir_resource.extension.get(url=VIBRENT_FHIR_URL + "expected-delivery-date").valueDateTime),
            'shipmentCarrier': fhir_resource.extension.get(url=VIBRENT_FHIR_URL + "carrier").valueString,
            'trackingId': fhir_resource.identifier.get(system=VIBRENT_FHIR_URL + "trackingId").value,
            'shipmentLastUpdate': parse_date(fhir_resource.occurrenceDateTime),
            'order_id': int(fhir_resource.basedOn[0].identifier.value),
        }

        # Address Handling
        fhir_address = fhir_resource.contained.get(resourceType="Location").get("address")
        address_fields = {
            "streetAddress1": fhir_address.line[0],
            "streetAddress2": fhir_address.line[1],
            "city": fhir_address.city,
            "stateId": get_code_id(fhir_address, self.code_dao, "state", "State_"),
            "zipCode": fhir_address.postalCode,
        }
        test_fields.update(address_fields)

        Supply = namedtuple('Supply', test_fields.keys())
        expected_data = Supply(**test_fields)

        return expected_data

    def make_supply_posts(self, supply_request=True, supply_delivery=True):
        """Helper function to make the POSTs for tests that depend on existing dv_orders"""
        if supply_request:
            self.send_post(
                "SupplyRequest",
                request_data=self.post_request,
                expected_status=http.client.CREATED,
            )

        if supply_delivery:
            self.send_post(
                "SupplyDelivery",
                request_data=self.post_delivery,
                expected_status=http.client.CREATED,
            )
