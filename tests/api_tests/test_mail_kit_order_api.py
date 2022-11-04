import copy
import http.client
import mock

from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import (
    BiobankOrderIdentifier,
    BiobankOrderedSample,
    BiobankOrder,
    BiobankOrderIdentifierHistory,
    BiobankOrderedSampleHistory,
    BiobankOrderHistory,
    MayolinkCreateOrderHistory
)
from rdr_service.model.code import Code, CodeType
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.offline.biobank_samples_pipeline import _PMI_OPS_SYSTEM
from rdr_service.participant_enums import WithdrawalStatus
from tests.test_data import load_test_data_json
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import load_biobank_order_json
from rdr_service.model.utils import to_client_participant_id


class MailKitOrderApiTestBase(BaseTestCase):
    mayolink_response = None

    def setUp(self, with_data=True):
        super().setUp(with_data=with_data)
        self.mail_kit_order_dao = MailKitOrderDao()
        self.hpo_dao = HPODao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.code_dao = CodeDao()

        # WARNING: the HPO looks like it gets set here, but is cleared during the insert
        # TODO: find out whot should be happening here
        self.hpo = self.hpo_dao.get_by_name("PITT")
        self.participant = Participant(hpoId=self.hpo.hpoId, participantId=123456789, biobankId=7)
        self.participant_dao.insert(self.participant)
        self.summary = self.participant_summary(self.participant)
        self.summary_dao.insert(self.summary)

        mayolinkapi_patcher = mock.patch(
            "rdr_service.dao.mail_kit_order_dao.MayoLinkClient", **{"return_value.post.return_value": self.mayolink_response}
        )
        self.mock_mayolink_api = mayolinkapi_patcher.start()
        self.addCleanup(mayolinkapi_patcher.stop)

    def get_payload(self, filename):
        return load_test_data_json(filename)

    def get_orders(self):
        with self.mail_kit_order_dao.session() as session:
            return list(session.query(BiobankMailKitOrder))


class MailKitOrderApiTestPostSupplyRequest(MailKitOrderApiTestBase):
    def test_order_created(self):
        self.assertEqual(0, len(self.get_orders()))
        response = self.send_post(
            "SupplyRequest",
            request_data=self.get_payload("dv_order_api_post_supply_request.json"),
            expected_status=http.client.CREATED,
        )
        self.assertTrue(response.location.endswith("/SupplyRequest/999999"))
        orders = self.get_orders()
        self.assertEqual(1, len(orders))

    def _build_supply_request_payload(self, participant: Participant):
        json = self.get_payload('dv_order_api_post_supply_request.json')

        # set the participant id
        json['contained'][0]['identifier'][0]['value'] = to_client_participant_id(participant.participantId)
        return json

    # The test setup creates a participant and appears to set an HPO on it, but the HPO is cleared
    # during the insertion process. So the following tests are created to work with their own participants
    def test_dv_order_created(self):
        unpaired_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=unpaired_participant)

        request_json = self._build_supply_request_payload(unpaired_participant)
        self.send_post("SupplyRequest", request_data=request_json, expected_status=http.client.CREATED)

        # Check that the order has no HPO set
        order: BiobankMailKitOrder = self.session.query(BiobankMailKitOrder).filter(
            BiobankMailKitOrder.participantId == unpaired_participant.participantId
        ).one()
        self.assertIsNone(order.associatedHpoId)

    def test_hpo_mail_kit_order_created(self):
        hpo = self.data_generator.create_database_hpo()
        paired_participant = self.data_generator.create_database_participant(hpoId=hpo.hpoId)
        self.data_generator.create_database_participant_summary(participant=paired_participant)

        request_json = self._build_supply_request_payload(paired_participant)
        self.send_post("SupplyRequest", request_data=request_json, expected_status=http.client.CREATED)

        # Check that the order has no HPO set
        order: BiobankMailKitOrder = self.session.query(BiobankMailKitOrder).filter(
            BiobankMailKitOrder.participantId == paired_participant.participantId
        ).one()
        self.assertEqual(hpo.hpoId, order.associatedHpoId)

class MailKitOrderApiTestPutSupplyRequest(MailKitOrderApiTestBase):
    mayolink_response = {
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

    def _set_mayo_address(self, data):
        """ set the address of a Supply Delivery json to the Mayo address """
        req = copy.deepcopy(data)

        for item in req['contained']:
            if item['resourceType'] == 'Location':
                item['address'] = {'city': "Rochester", 'state': "MN",
                                   'postalCode': "55901", 'line': ["3050 Superior Drive NW"], 'type': 'postal',
                                   'use': 'work'}
        # Mayo tracking ID
        req['identifier'] = \
            [{"system": "http://joinallofus.org/fhir/trackingId", "value": "98765432109876543210"}]
        # Participant Tracking ID
        req['partOf'] = \
            [{'identifier': {"system": "http://joinallofus.org/fhir/trackingId", "value": "P12435464423"}}]
        return req

    def test_order_updated(self):
        # create a regular biobank order first, make sure the salivary order will not overwrite
        # the participant summary specimen site info
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        path = "Participant/%s/BiobankOrder" % to_client_participant_id(self.participant.participantId)
        self.send_post(path, order_json)
        ps = self.summary_dao.get(self.participant.participantId)
        self.assertEqual(ps.biospecimenSourceSiteId, 1)
        self.assertEqual(ps.biospecimenCollectedSiteId, 1)
        self.assertEqual(ps.biospecimenProcessedSiteId, 1)
        self.assertEqual(ps.biospecimenFinalizedSiteId, 2)

        self.assertEqual(0, len(self.get_orders()))
        post_response = self.send_post(
            "SupplyRequest",
            request_data=self.get_payload("dv_order_api_post_supply_request.json"),
            expected_status=http.client.CREATED,
        )
        location_id = post_response.location.rsplit("/", 1)[-1]
        self.send_put(
            "SupplyRequest/{}".format(location_id),
            request_data=self.get_payload("dv_order_api_put_supply_request.json"),
        )
        post_response = self.send_post(
            "SupplyDelivery",
            request_data=self._set_mayo_address(self.get_payload('dv_order_api_post_supply_delivery.json')),
            expected_status=http.client.CREATED,
        )
        location_id = post_response.location.rsplit("/", 1)[-1]
        self.send_put(
            "SupplyDelivery/{}".format(location_id),
            request_data=self._set_mayo_address(self.get_payload('dv_order_api_put_supply_delivery.json')),
        )
        # make sure the participant summary specimen site info is not changed
        ps = self.summary_dao.get(self.participant.participantId)
        self.assertEqual(ps.biospecimenSourceSiteId, 1)
        self.assertEqual(ps.biospecimenCollectedSiteId, 1)
        self.assertEqual(ps.biospecimenProcessedSiteId, 1)
        self.assertEqual(ps.biospecimenFinalizedSiteId, 2)

        orders = self.get_orders()
        self.assertEqual(1, len(orders))
        for i in orders:
            self.assertEqual(i.barcode, "SABR90160121IN")
            self.assertEqual(i.id, int(1))
            self.assertEqual(i.order_id, int(999999))
            self.assertEqual(i.biobankOrderId, "WEB1ABCD1234")
            self.assertEqual(i.biobankStatus, "Queued")
            self.assertEqual(i.biobankTrackingId, "PAT-123-456")

        with self.mail_kit_order_dao.session() as session:
            # there should be three identifier records in the BiobankOrderIdentifier table
            identifiers = session.query(BiobankOrderIdentifier).all()
            self.assertEqual(5, len(identifiers))
            # there should be one ordered sample in the BiobankOrderedSample table
            samples = session.query(BiobankOrderedSample).all()
            self.assertEqual(17, len(samples))

            mayolink_history_records = session.query(MayolinkCreateOrderHistory).all()
            self.assertEqual(1, len(mayolink_history_records))

    def test_missing_authoredOn_works(self):
        """authoredOn may not be sent in payload."""
        request = self.get_payload("dv_order_api_post_supply_request.json")
        del request["authoredOn"]
        post_response = self.send_post("SupplyRequest", request_data=request, expected_status=http.client.CREATED)
        order = self.get_orders()
        self.assertEqual(1, len(order))
        self.assertEqual(post_response._status_code, 201)

    def test_set_system_identifier_by_user(self):
        system_from_user = {
            'vibrent-drc-prod@test-bed.fake': ('vibrent', 'http://vibrenthealth.com'),
            'careevolution@test-bed.fake': ('careevolution', 'http://carevolution.be'),
            'no-sys@test-bed.fake': (None, None),
            'example@example.com': ('example', 'system-test')
        }

        # duplicate the test for each user (Vibrent and CE)
        for user, expected_system_identifier in system_from_user.items():
            BaseTestCase.switch_auth_user(user, client_id=expected_system_identifier[0])

            # Make the series of API calls to create DV orders and associated Biobank records
            post_response = self.send_post(
                'SupplyRequest',
                request_data=self.get_payload('dv_order_api_post_supply_request.json'),
                expected_status=http.client.CREATED
            )
            location_id = post_response.location.rsplit('/', 1)[-1]
            self.send_put(
                'SupplyRequest/{}'.format(location_id),
                request_data=self.get_payload('dv_order_api_put_supply_request.json'),
            )

            # Check if there is a client ID
            if expected_system_identifier[0] is None:
                self.send_post(
                    'SupplyDelivery',
                    request_data=self._set_mayo_address(
                        self.get_payload('dv_order_api_post_supply_delivery.json')),
                    expected_status=400
                )
            else:
                self.send_post(
                    'SupplyDelivery',
                    request_data=self._set_mayo_address(
                        self.get_payload('dv_order_api_post_supply_delivery.json')),
                    expected_status=http.client.CREATED
                )

                # Compare the results in the DB with the system identifiers defined above
                with self.mail_kit_order_dao.session() as session:
                    test_order_id = self.mayolink_response['orders']['order']['number']
                    identifiers = session.query(BiobankOrderIdentifier).filter_by(
                        biobankOrderId=test_order_id
                    ).all()
                    for identifier in identifiers:
                        if identifier.system.endswith('/trackingId'):
                            self.assertEqual(identifier.system, expected_system_identifier[1] + "/trackingId")
                        elif identifier.system == _PMI_OPS_SYSTEM:
                            # Skip identifier that is created for each dv salivary order regardless of user
                            continue
                        else:
                            self.assertEqual(identifier.system, expected_system_identifier[1])
                        session.delete(identifier)

            self._intra_test_clean_up_db()

        # Resetting in case downstream tests require it
        BaseTestCase.switch_auth_user("example@example.com")

    def _intra_test_clean_up_db(self):
        """DB clean-up to avoid duplicate key errors"""
        test_order_id = self.mayolink_response['orders']['order']['number']

        with self.mail_kit_order_dao.session() as session:

            identifier_history = session.query(BiobankOrderIdentifierHistory).filter_by(
                biobankOrderId=test_order_id
            ).all()
            for record in identifier_history:
                session.delete(record)

            ordered_samples_history = session.query(BiobankOrderedSampleHistory).filter_by(
                biobankOrderId=test_order_id
            ).all()
            for record in ordered_samples_history:
                session.delete(record)

            dv_orders = session.query(BiobankMailKitOrder).filter_by(
                participantId=self.participant.participantId
            ).all()
            for dv_order in dv_orders:
                session.delete(dv_order)

            bb_order_history = session.query(BiobankOrderHistory).filter_by(
                biobankOrderId=test_order_id
            ).all()
            for record in bb_order_history:
                session.delete(record)

            bb_orders = session.query(BiobankOrder).filter_by(
                biobankOrderId=test_order_id
            ).all()
            for bb_order in bb_orders:
                session.delete(bb_order)

class MailKitOrderApiTestPostSupplyDelivery(MailKitOrderApiTestBase):
    mayolink_response = {
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

    def test_supply_delivery_fails_without_supply_request(self):
        self.send_post(
            "SupplyDelivery",
            request_data=self.get_payload("dv_order_api_post_supply_delivery.json"),
            expected_status=http.client.CONFLICT,
        )

    def test_delivery_pass_after_supply_request(self):
        self.send_post(
            "SupplyRequest",
            request_data=self.get_payload("dv_order_api_post_supply_request.json"),
            expected_status=http.client.CREATED,
        )

        self.send_post(
            "SupplyDelivery",
            request_data=self.get_payload("dv_order_api_post_supply_delivery.json"),
            expected_status=http.client.CREATED,
        )

        orders = self.get_orders()
        self.assertEqual(1, len(orders))

    @mock.patch("rdr_service.dao.mail_kit_order_dao.get_code_id")
    def test_biobank_address_received(self, patched_code_id):
        patched_code_id.return_value = 1

        code = Code(system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True)
        self.code_dao.insert(code)
        self.send_post(
            "SupplyRequest",
            request_data=self.get_payload("dv_order_api_post_supply_request.json"),
            expected_status=http.client.CREATED,
        )

        response = self.send_post(
            "SupplyDelivery",
            request_data=self.get_payload("dv_order_api_post_supply_delivery.json"),
            expected_status=http.client.CREATED,
        )

        request = self.get_payload("dv_order_api_put_supply_delivery.json")
        biobank_address = self.mail_kit_order_dao.biobank_address
        request["contained"][0]["address"] = biobank_address

        location_id = response.location.rsplit("/", 1)[-1]
        self.send_put("SupplyDelivery/{}".format(location_id), request_data=request)

        order = self.get_orders()
        self.assertEqual(order[0].biobankCity, "Rochester")
        self.assertEqual(order[0].city, "Fairfax")
        self.assertEqual(order[0].biobankStreetAddress1, "3050 Superior Drive NW")
        self.assertEqual(order[0].streetAddress1, "4114 Legato Rd")
        self.assertEqual(order[0].streetAddress2, "test line 2")
        self.assertEqual(order[0].biobankStateId, 1)
        self.assertEqual(order[0].stateId, 1)
        self.assertEqual(order[0].biobankZipCode, "55901")
        self.assertEqual(order[0].zipCode, "22033")

        self.assertTrue(response.location.endswith("/SupplyDelivery/999999"))
        self.assertEqual(1, len(order))
        for i in order:
            self.assertEqual(i.id, int(1))
            self.assertEqual(i.order_id, int(999999))

    @mock.patch("rdr_service.dao.mail_kit_order_dao.get_code_id")
    def test_biobank_address_received_alt_json(self, patched_code_id):
        patched_code_id.return_value = 1

        code = Code(system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True)
        self.code_dao.insert(code)
        self.send_post(
            "SupplyRequest",
            request_data=self.get_payload("dv_order_api_post_supply_request.json"),
            expected_status=http.client.CREATED,
        )

        response = self.send_post(
            "SupplyDelivery",
            request_data=self.get_payload("dv_order_api_post_supply_delivery_alt.json"),
            expected_status=http.client.CREATED,
        )

        request = self.get_payload("dv_order_api_put_supply_delivery.json")
        biobank_address = self.mail_kit_order_dao.biobank_address
        request["contained"][0]["address"] = biobank_address

        location_id = response.location.rsplit("/", 1)[-1]
        self.send_put("SupplyDelivery/{}".format(location_id), request_data=request)

        order = self.get_orders()
        self.assertEqual(order[0].biobankCity, "Rochester")
        self.assertEqual(order[0].biobankStreetAddress1, "3050 Superior Drive NW")
        self.assertEqual(order[0].biobankStateId, 1)
        self.assertEqual(order[0].biobankZipCode, "55901")

        self.assertTrue(response.location.endswith("/SupplyDelivery/999999"))
        self.assertEqual(1, len(order))
        for i in order:
            self.assertEqual(i.id, int(1))
            self.assertEqual(i.order_id, int(999999))

    def test_no_api_call_on_withdrawn(self):
        """
        No Mayolink API call should occur if the request fails validation because the participant is withdrawn
        """
        participant_id = self.participant.participantId

        # Create order for the SupplyDelivery POST to work with
        # (order number and participant id from dv_order_api_post_supply_delivery.json file)
        self.data_generator.create_database_biobank_mail_kit_order(
            participantId=participant_id,
            order_id=999999
        )

        # Set the participant as withdrawn
        participant = self.session.query(Participant).filter(
            Participant.participantId == participant_id
        ).one()
        participant.withdrawalStatus = WithdrawalStatus.NO_USE
        summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == participant_id
        ).one()
        summary.withdrawalStatus = WithdrawalStatus.NO_USE
        self.session.commit()

        # Send the SupplyDelivery POST, expecting it to fail validation
        order_json = self.get_payload("dv_order_api_post_supply_delivery.json")
        self.send_post("SupplyDelivery", request_data=order_json, expected_status=http.client.FORBIDDEN)

        # Ensure that the Mayolink API wasn't called
        self.mock_mayolink_api.return_value.post.assert_not_called()

    @mock.patch("rdr_service.dao.mail_kit_order_dao.get_code_id")
    def test_exam_one_order_delivery(self, patched_code_id):
        patched_code_id.return_value = 1
        code = Code(system="a", value="b", display="c", topic="d", codeType=CodeType.MODULE, mapped=True)
        self.code_dao.insert(code)

        # Send a request for an ExamOne order
        supply_request_json = self.get_payload("dv_order_api_post_supply_request.json")
        supply_request_json['extension'][1]['valueString'] = 'Exam One Order'
        self.send_post(
            "SupplyRequest",
            request_data=supply_request_json,
            expected_status=http.client.CREATED,
        )

        # Be sure that an error is raised if a delivery is sent for the ExamOne order
        self.send_post(
            "SupplyDelivery",
            request_data=self.get_payload("dv_order_api_post_supply_delivery.json"),
            expected_status=http.client.BAD_REQUEST,
        )


class MailKitOrderApiTestPutSupplyDelivery(MailKitOrderApiTestBase):
    mayolink_response = {
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

    def test_supply_delivery_put(self):
        self.send_post(
            "SupplyRequest",
            request_data=self.get_payload("dv_order_api_post_supply_request.json"),
            expected_status=http.client.CREATED,
        )

        response = self.send_post(
            "SupplyDelivery",
            request_data=self.get_payload("dv_order_api_post_supply_delivery.json"),
            expected_status=http.client.CREATED,
        )

        location_id = response.location.rsplit("/", 1)[-1]
        self.send_put(
            "SupplyDelivery/{}".format(location_id),
            request_data=self.get_payload("dv_order_api_put_supply_delivery.json"),
        )

        orders = self.get_orders()
        self.assertEqual(1, len(orders))
