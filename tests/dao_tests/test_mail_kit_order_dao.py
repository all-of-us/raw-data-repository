import http.client
import json

import mock
from werkzeug.exceptions import ServiceUnavailable

from rdr_service.api_util import (
    DV_FHIR_URL,
    DV_FULFILLMENT_URL,
    DV_ORDER_URL,
    parse_date,
    get_code_id,
)
from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import BiobankOrder
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.fhir_utils import SimpleFhirR4Reader
from rdr_service.model.participant import Participant
from rdr_service.offline.biobank_samples_pipeline import _PMI_OPS_SYSTEM
from rdr_service.participant_enums import OrderShipmentStatus, OrderShipmentTrackingStatus
from tests.test_data import load_test_data_json
from tests.helpers.unittest_base import BaseTestCase

from collections import namedtuple


class MailKitOrderDaoTestBase(BaseTestCase):
    def setUp(self):
        super().setUp()

        self.post_delivery = load_test_data_json("dv_order_api_post_supply_delivery.json")
        self.put_delivery = load_test_data_json("dv_order_api_put_supply_delivery.json")
        self.post_request = load_test_data_json("dv_order_api_post_supply_request.json")
        self.put_request = load_test_data_json("dv_order_api_put_supply_request.json")

        self.dao = MailKitOrderDao()
        self.code_dao = CodeDao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

        self.participant = Participant(participantId=123456789, biobankId=7)
        self.participant_dao.insert(self.participant)
        self.summary = self.participant_summary(self.participant)
        self.summary_dao.insert(self.summary)
        self.mayolink_barcode = "barcode"
        self.mayolink_response = {
            "orders": {
                "order": {
                    "status": "Queued",
                    "reference_number": self.mayolink_barcode,
                    "received": "2019-04-05 12:00:00",
                    "number": "12345",
                    "patient": {
                        "medical_record_number": "WEB1ABCD1234"
                    },
                }
            }
        }

        mayolinkapi_patcher = mock.patch(
            "rdr_service.dao.mail_kit_order_dao.MayoLinkApi", **{"return_value.post.return_value": self.mayolink_response}
        )

        self.mock_mayolinkapi = mayolinkapi_patcher.start()
        self.addCleanup(mayolinkapi_patcher.stop)

    def test_insert_biobank_order(self):
        version_one_barcode = self.put_request['extension'][0]['valueString']
        payload = self.send_post(
            "SupplyRequest",
            request_data=self.post_request,
            expected_status=http.client.CREATED
        )
        request_response = json.loads(payload.response[0])
        location = payload.location.rsplit("/", 1)[-1]
        self.send_put(
            f"SupplyRequest/{location}",
            request_data=self.put_request
        )
        payload = self.send_post(
            "SupplyDelivery",
            request_data=self.post_delivery,
            expected_status=http.client.CREATED
        )
        post_response = json.loads(payload.response[0])
        location = payload.location.rsplit("/", 1)[-1]
        put_response = self.send_put(
            f"SupplyDelivery/{location}",
            request_data=self.put_delivery
        )

        self.assertEqual(request_response["version"], 1)
        self.assertEqual(post_response["version"], 3)
        self.assertEqual(post_response["meta"]["versionId"].strip("W/"), '"3"')
        self.assertEqual(put_response["version"], 4)
        self.assertEqual(put_response["meta"]["versionId"].strip("W/"), '"4"')
        self.assertEqual(put_response["barcode"], version_one_barcode)
        self.assertEqual(put_response["order_id"], 999999)

        mayo_order_payload = self.mock_mayolinkapi.return_value.post.call_args.args[0]
        mayo_order_payload = mayo_order_payload['order']
        mayo_payload_fields = ['collected', 'account', 'number', 'patient', 'physician', 'report_notes', 'tests', 'comments']

        self.assertEqual(mayo_order_payload['number'], version_one_barcode)
        self.assertTrue(all(key in mayo_order_payload.keys() for key in mayo_payload_fields))

    def test_insert_biobank_order_version_two_barcode(self):
        version_two_barcode = 'SABR9016012221IN'
        self.put_request['extension'][0]['valueString'] = version_two_barcode
        payload = self.send_post(
            "SupplyRequest",
            request_data=self.post_request,
            expected_status=http.client.CREATED
        )
        location = payload.location.rsplit("/", 1)[-1]
        self.send_put(
            f"SupplyRequest/{location}",
            request_data=self.put_request
        )
        payload = self.send_post(
            "SupplyDelivery",
            request_data=self.post_delivery,
            expected_status=http.client.CREATED
        )
        location = payload.location.rsplit("/", 1)[-1]
        put_response = self.send_put(
            f"SupplyDelivery/{location}",
            request_data=self.put_delivery
        )

        self.assertEqual(put_response["barcode"], version_two_barcode)

        mayo_order_payload = self.mock_mayolinkapi.return_value.post.call_args.args[0]
        mayo_order_payload = mayo_order_payload['order']

        mayo_request_test_data = mayo_order_payload['tests'][0]['test']
        self.assertEqual(mayo_request_test_data['client_passthrough_fields']['field1'], version_two_barcode)
        self.assertIsNone(mayo_request_test_data['client_passthrough_fields']['field2'])
        self.assertIsNone(mayo_request_test_data['client_passthrough_fields']['field3'])
        self.assertIsNone(mayo_request_test_data['client_passthrough_fields']['field4'])
        self.assertEqual(
            ['collected', 'account', 'number', 'patient', 'physician', 'report_notes', 'tests','comments'],
            list(mayo_order_payload.keys())
        )
        self.assertIsNone(mayo_order_payload['number'])  # An empty number field should be given for version two

        # Make sure the correct account is used for version two
        self.mock_mayolinkapi.assert_called_once_with(credentials_key='version_two')

    def test_biobank_bad_barcode(self):
        bad_barcode = 'SABR90-1601-2221IN'
        cleaned_barcode = 'SABR9016012221IN'

        self.put_request['extension'][0]['valueString'] = bad_barcode

        payload = self.send_post(
            "SupplyRequest",
            request_data=self.post_request,
            expected_status=http.client.CREATED
        )
        location = payload.location.rsplit("/", 1)[-1]

        self.send_put(
            f"SupplyRequest/{location}",
            request_data=self.put_request
        )
        payload = self.send_post(
            "SupplyDelivery",
            request_data=self.post_delivery,
            expected_status=http.client.CREATED
        )
        location = payload.location.rsplit("/", 1)[-1]

        put_response = self.send_put(
            f"SupplyDelivery/{location}",
            request_data=self.put_delivery
        )

        self.assertEqual(put_response["barcode"], cleaned_barcode)

        mayo_order_payload = self.mock_mayolinkapi.return_value.post.call_args.args[0]['order']['tests'][0]['test']
        self.assertEqual(mayo_order_payload['client_passthrough_fields']['field1'], cleaned_barcode)

    def test_biobank_order_finalized_and_identifier_created(self):
        self.send_post(
            "SupplyRequest",
            request_data=self.post_request,
            expected_status=http.client.CREATED
        )
        payload = self.send_post(
            "SupplyDelivery",
            request_data=load_test_data_json("dv_order_api_post_supply_delivery_alt.json"),
            expected_status=http.client.CREATED
        )
        post_response = json.loads(payload.response[0])
        biobank_order = self.session.query(BiobankOrder).filter(
            BiobankOrder.biobankOrderId == post_response['biobankOrderId']
        ).one()
        self.assertIsNotNone(biobank_order.finalizedTime,
                             'Salivary DV orders should create biobank orders and set them as finalized')
        self.assertTrue(any([identifier.system == _PMI_OPS_SYSTEM and identifier.value == self.mayolink_barcode
                             for identifier in biobank_order.identifiers]),
                        'BiobankOrderIdentifiers should be created with the barcode as a value')

    def test_enumerate_shipping_status(self):
        fhir_resource = SimpleFhirR4Reader(self.post_request)
        status = self.dao._enumerate_order_shipping_status(fhir_resource.status)
        self.assertEqual(status, OrderShipmentStatus.SHIPPED)

    def test_enumerate_tracking_status(self):
        fhir_resource = SimpleFhirR4Reader(self.post_delivery)
        status = self.dao._enumerate_order_tracking_status(
            fhir_resource.extension.get(url=DV_FHIR_URL + "tracking-status").valueString
        )
        self.assertEqual(status, OrderShipmentTrackingStatus.IN_TRANSIT)

    def test_from_client_json(self):
        self.make_supply_posts(self.post_request, self.post_delivery)

        expected_result = self.build_expected_resource_type_data(self.post_delivery)

        result_from_dao = self.dao.from_client_json(self.post_delivery, participant_id=self.participant.participantId)

        # run tests against result_from_dao
        for i, test_field in enumerate(expected_result):
            self.assertEqual(test_field, getattr(result_from_dao, expected_result._fields[i]))

    def test_dv_order_post_inserted_correctly(self):
        def run_db_test(expected_result):
            """ Runs the db test against the expected result"""

            # return a BiobankDVOrder object from database
            with self.dao.session() as session:
                dv_order_result = session.query(BiobankMailKitOrder).filter_by(
                    participantId=self.participant.participantId).first()

            # run tests against dv_order_result
            for i, test_field in enumerate(expected_result):
                self.assertEqual(test_field, getattr(dv_order_result, expected_result._fields[i]))

        # run DB test after each post
        test_data_payloads = [self.post_request, self.post_delivery]
        for test_case in test_data_payloads:
            expected_data = self.build_expected_resource_type_data(test_case)

            # make posts to create SupplyRequest and SupplyDelivery records
            self.make_supply_posts(test_case)
            run_db_test(expected_data)

    @mock.patch("rdr_service.dao.mail_kit_order_dao.MayoLinkApi")
    def test_service_unavailable(self, mocked_api):
        # pylint: disable=unused-argument
        def raises(*args):
            raise ServiceUnavailable()

        with self.assertRaises(ServiceUnavailable):
            mocked_api.return_value.post.side_effect = raises
            self.dao.send_order(self.post_delivery, self.participant.participantId)

    def build_expected_resource_type_data(self, resource_type):
        """Helper function to build the data we are expecting from the test-data file."""
        fhir_resource = SimpleFhirR4Reader(resource_type)

        test_fields = {}
        fhir_address = {}

        # fields to test with the same structure in both payloads
        fhir_device = fhir_resource.contained.get(resourceType="Device")
        test_fields.update({
            'itemName': fhir_device.deviceName.get(type="manufacturer-name").name,
            'orderType': fhir_resource.extension.get(url=DV_ORDER_URL).valueString
        })

        # add the fields to test for each resource type (SupplyRequest, SupplyDelivery)
        if resource_type == self.post_request:
            test_fields.update({
                'order_id': int(fhir_resource.identifier.get(system=DV_FHIR_URL + "orderId").value),
                'supplier': fhir_resource.contained.get(resourceType="Organization").id,
                'supplierStatus': fhir_resource.extension.get(url=DV_FULFILLMENT_URL).valueString,
                'itemQuantity': fhir_resource.quantity.value,
                'itemSKUCode': fhir_device.identifier.get(system=DV_FHIR_URL + "SKU").value,
            })
            # Address Handling
            fhir_address = fhir_resource.contained.get(resourceType="Patient").address[0]

        if resource_type == self.post_delivery:
            test_fields.update({
                'order_id': int(fhir_resource.basedOn[0].identifier.value),
                'shipmentEstArrival': parse_date(fhir_resource.extension.get(
                    url=DV_FHIR_URL + "expected-delivery-date").valueDateTime),
                'shipmentCarrier': fhir_resource.extension.get(url=DV_FHIR_URL + "carrier").valueString,
                'trackingId': fhir_resource.identifier.get(system=DV_FHIR_URL + "trackingId").value,
                'shipmentLastUpdate': parse_date(fhir_resource.occurrenceDateTime),
            })
            # Address Handling
            fhir_address = fhir_resource.contained.get(resourceType="Location").get("address")

        address_fields = {
            "streetAddress1": fhir_address.line[0],
            "streetAddress2": '',
            "city": fhir_address.city,
            "stateId": get_code_id(fhir_address, self.code_dao, "state", "State_"),
            "zipCode": fhir_address.postalCode,
        }

        # street address 2
        if len(list(fhir_address.line)) > 1:
            address_fields['streetAddress2'] = fhir_address.line[1]

        test_fields.update(address_fields)

        Supply = namedtuple('Supply', test_fields.keys())
        expected_data = Supply(**test_fields)

        return expected_data

    def make_supply_posts(self, *test_cases):
        """Helper function to make the POSTs for tests that depend on existing dv_orders"""
        if self.post_request in test_cases:
            self.send_post(
                "SupplyRequest",
                request_data=self.post_request,
                expected_status=http.client.CREATED,
            )

        if self.post_delivery in test_cases:
            self.send_post(
                "SupplyDelivery",
                request_data=self.post_delivery,
                expected_status=http.client.CREATED,
            )
