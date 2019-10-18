import copy
import httplib

import mock
from dao.code_dao import CodeDao
from dao.dv_order_dao import DvOrderDao
from dao.hpo_dao import HPODao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_dv_order import BiobankDVOrder
from model.code import Code, CodeType
from model.participant import Participant
from model.biobank_order import (
  BiobankOrderIdentifier,
  BiobankOrderedSample,
  BiobankOrder,
  BiobankOrderIdentifierHistory,
  BiobankOrderedSampleHistory, BiobankOrderHistory)
from test_data import load_test_data_json
from unit_test_util import FlaskTestBase, NdbTestBase
import api_util

class DvOrderApiTestBase(FlaskTestBase):
  mayolink_response = None

  def setUp(self, use_mysql=True, with_data=True):
    super(DvOrderApiTestBase, self).setUp(use_mysql=use_mysql, with_data=with_data)
    self.dv_order_dao = DvOrderDao()
    self.hpo_dao = HPODao()
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.code_dao = CodeDao()

    self.hpo = self.hpo_dao.get_by_name('PITT')
    self.participant = Participant(
        hpoId=self.hpo.hpoId,
        participantId=123456789,
        biobankId=7
    )
    self.participant_dao.insert(self.participant)
    self.summary = self.participant_summary(self.participant)
    self.summary_dao.insert(self.summary)

    mayolinkapi_patcher = mock.patch(
        'dao.dv_order_dao.MayoLinkApi',
        **{'return_value.post.return_value': self.mayolink_response}
    )
    mayolinkapi_patcher.start()
    self.addCleanup(mayolinkapi_patcher.stop)

  def get_payload(self, filename):
    return load_test_data_json(filename)

  def get_orders(self):
    with self.dv_order_dao.session() as session:
      return list(session.query(BiobankDVOrder))


class DvOrderApiTestPostSupplyRequest(DvOrderApiTestBase):

  def test_order_created(self):
    self.assertEqual(0, len(self.get_orders()))
    response = self.send_post(
        'SupplyRequest',
        request_data=self.get_payload('dv_order_api_post_supply_request.json'),
        expected_status=httplib.CREATED
    )
    self.assertTrue(response.location.endswith('/SupplyRequest/999999'))
    orders = self.get_orders()
    self.assertEqual(1, len(orders))


class DvOrderApiTestPutSupplyRequest(DvOrderApiTestBase):
  mayolink_response = {
      'orders': {
          'order': {
              'status': 'Queued',
              'reference_number': 'somebarcodenumber',
              'received': '2016-12-01T12:00:00-05:00',
              'number': 'WEB1ABCD1234',
              'patient': {
                  'medical_record_number': 'PAT-123-456'
              }
          }
      }
  }

  def _set_mayo_address(self, data):
    """ set the address of a Supply Delivery json to the Mayo address """
    req = copy.deepcopy(data)

    for item in req['contained']:
      if item['resourceType'] == 'Location':
        item['address'] = {'city': "Rochester", 'state': "MN",
                            'postalCode': "55901", 'line': ["3050 Superior Drive NW"], 'type': 'postal', 'use': 'work'}
    # Mayo tracking ID
    req['identifier'] = \
      [{"system": "http://joinallofus.org/fhir/trackingId", "value": "98765432109876543210"}]
    # Participant Tracking ID
    req['partOf'] = \
      [{'identifier': {"system": "http://joinallofus.org/fhir/trackingId", "value": "P12435464423"}}]
    return req

  def test_order_updated(self):
    self.assertEqual(0, len(self.get_orders()))
    post_response = self.send_post(
        'SupplyRequest',
        request_data=self.get_payload('dv_order_api_post_supply_request.json'),
        expected_status=httplib.CREATED
    )
    location_id = post_response.location.rsplit('/', 1)[-1]
    self.send_put(
        'SupplyRequest/{}'.format(location_id),
        request_data=self.get_payload('dv_order_api_put_supply_request.json'),
    )
    post_response = self.send_post(
        'SupplyDelivery',
        request_data=self._set_mayo_address(self.get_payload('dv_order_api_post_supply_delivery.json')),
        expected_status=httplib.CREATED
    )
    location_id = post_response.location.rsplit('/', 1)[-1]
    self.send_put(
        'SupplyDelivery/{}'.format(location_id),
        request_data=self._set_mayo_address(self.get_payload('dv_order_api_put_supply_delivery.json')),
    )

    orders = self.get_orders()
    self.assertEqual(1, len(orders))
    for i in orders:
      self.assertEqual(i.barcode, 'SABR90160121INA')
      self.assertEqual(i.id, long(1))
      self.assertEqual(i.order_id, long(999999))
      self.assertEqual(i.biobankOrderId, 'WEB1ABCD1234')
      self.assertEqual(i.biobankStatus, 'Queued')
      self.assertEqual(i.biobankTrackingId, 'PAT-123-456')

    with self.dv_order_dao.session() as session:
      # there should be two identifier records in the BiobankOrderIdentifier table
      identifiers = session.query(BiobankOrderIdentifier).all()
      self.assertEqual(2, len(identifiers))
      # there should be one ordered sample in the BiobankOrderedSample table
      samples = session.query(BiobankOrderedSample).all()
      self.assertEqual(1, len(samples))

  def test_missing_authoredOn_works(self):
    """authoredOn may not be sent in payload."""
    request = self.get_payload('dv_order_api_post_supply_request.json')
    del request['authoredOn']
    post_response = self.send_post(
        'SupplyRequest',
        request_data=request,
        expected_status=httplib.CREATED
    )
    order = self.get_orders()
    self.assertEquals(1, len(order))
    self.assertEquals(post_response._status_code, 201)

  def test_set_system_identifier_by_user(self):

    system_from_user = {
      'vibrent-drc-prod@test-bed.fake': "http://vibrenthealth.com",
      'careevolution@test-bed.fake': "http://carevolution.be",
      'authorized@gservices.act': "system-test"
    }

    # duplicate the test for each user (Vibrent and CE)
    for user, expected_system_identifier in system_from_user.items():
      self._switch_auth_user(user)

      # Make the series of API calls to create DV orders and associated Biobank records
      post_response = self.send_post(
        'SupplyRequest',
        request_data=self.get_payload('dv_order_api_post_supply_request.json'),
        expected_status=httplib.CREATED
      )
      location_id = post_response.location.rsplit('/', 1)[-1]
      self.send_put(
        'SupplyRequest/{}'.format(location_id),
        request_data=self.get_payload('dv_order_api_put_supply_request.json'),
      )
      self.send_post(
        'SupplyDelivery',
        request_data=self._set_mayo_address(
          self.get_payload('dv_order_api_post_supply_delivery.json')),
        expected_status=httplib.CREATED
      )

      # Compare the results in the DB with the system identifiers defined above
      with self.dv_order_dao.session() as session:
        test_order_id = self.mayolink_response['orders']['order']['number']
        identifiers = session.query(BiobankOrderIdentifier).filter_by(
          biobankOrderId=test_order_id
        ).all()
        for identifier in identifiers:
          if identifier.system.endswith('/trackingId'):
            self.assertEqual(identifier.system, expected_system_identifier + "/trackingId")
          else:
            self.assertEqual(identifier.system, expected_system_identifier)
          session.delete(identifier)

      self._intra_test_clean_up_db()

    # Resetting in case downstream tests require it
    self._switch_auth_user("authorized@gservices.act")


  def _switch_auth_user(self, new_auth_user):
    """Helper function to switch the auth_user's account email for testing"""
    NdbTestBase._AUTH_USER = new_auth_user
    NdbTestBase._CONFIG_USER_INFO = {
      NdbTestBase._AUTH_USER: {
        'roles': api_util.ALL_ROLES,
      },
    }

    NdbTestBase.doSetUp(self)
    self.set_auth_user(NdbTestBase._AUTH_USER)

  def _intra_test_clean_up_db(self):
    """DB clean-up to avoid duplicate key errors"""
    test_order_id = self.mayolink_response['orders']['order']['number']

    with self.dv_order_dao.session() as session:

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

      dv_orders = session.query(BiobankDVOrder).filter_by(
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

class DvOrderApiTestPostSupplyDelivery(DvOrderApiTestBase):
  mayolink_response = {
    'orders': {
      'order': {
        'status': 'Queued',
        'reference_number': 'somebarcodenumber',
        'received': '2016-12-01T12:00:00-05:00',
        'number': 'WEB1ABCD1234',
        'patient': {
          'medical_record_number': 'PAT-123-456'
        }
      }
    }
  }

  def test_supply_delivery_fails_without_supply_request(self):
    self.send_post(
      'SupplyDelivery',
      request_data=self.get_payload('dv_order_api_post_supply_delivery.json'),
      expected_status=httplib.CONFLICT
    )

  def test_delivery_pass_after_supply_request(self):
    self.send_post(
      'SupplyRequest',
      request_data=self.get_payload('dv_order_api_post_supply_request.json'),
      expected_status=httplib.CREATED
    )

    self.send_post(
      'SupplyDelivery',
      request_data=self.get_payload('dv_order_api_post_supply_delivery.json'),
      expected_status=httplib.CREATED
    )

    orders = self.get_orders()
    self.assertEqual(1, len(orders))
  @mock.patch('dao.dv_order_dao.get_code_id')
  def test_biobank_address_received(self, patched_code_id):
    patched_code_id.return_value = 1

    code = Code(system="a", value="b", display=u"c", topic=u"d",
                codeType=CodeType.MODULE, mapped=True)
    self.code_dao.insert(code)
    self.send_post(
      'SupplyRequest',
      request_data=self.get_payload('dv_order_api_post_supply_request.json'),
      expected_status=httplib.CREATED
    )

    response = self.send_post(
      'SupplyDelivery',
      request_data=self.get_payload('dv_order_api_post_supply_delivery.json'),
      expected_status=httplib.CREATED
    )

    request = self.get_payload('dv_order_api_put_supply_delivery.json')
    biobank_address = self.dv_order_dao.biobank_address
    request['contained'][0]['address'] = biobank_address

    location_id = response.location.rsplit('/', 1)[-1]
    self.send_put(
      'SupplyDelivery/{}'.format(location_id),
      request_data=request
    )

    order = self.get_orders()
    self.assertEquals(order[0].biobankCity, 'Rochester')
    self.assertEquals(order[0].city, 'Fairfax')
    self.assertEquals(order[0].biobankStreetAddress1, '3050 Superior Drive NW')
    self.assertEquals(order[0].streetAddress1, '4114 Legato Rd')
    self.assertEquals(order[0].streetAddress2, 'test line 2')
    self.assertEquals(order[0].biobankStateId, 1)
    self.assertEquals(order[0].stateId, 1)
    self.assertEquals(order[0].biobankZipCode, '55901')
    self.assertEquals(order[0].zipCode, '22033')

    self.assertTrue(response.location.endswith('/SupplyDelivery/999999'))
    self.assertEqual(1, len(order))
    for i in order:
      self.assertEqual(i.id, long(1))
      self.assertEqual(i.order_id, long(999999))

  @mock.patch('dao.dv_order_dao.get_code_id')
  def test_biobank_address_received_alt_json(self, patched_code_id):
    patched_code_id.return_value = 1

    code = Code(system="a", value="b", display=u"c", topic=u"d",
                codeType=CodeType.MODULE, mapped=True)
    self.code_dao.insert(code)
    self.send_post(
      'SupplyRequest',
      request_data=self.get_payload('dv_order_api_post_supply_request.json'),
      expected_status=httplib.CREATED
    )

    response = self.send_post(
      'SupplyDelivery',
      request_data=self.get_payload('dv_order_api_post_supply_delivery_alt.json'),
      expected_status=httplib.CREATED
    )

    request = self.get_payload('dv_order_api_put_supply_delivery.json')
    biobank_address = self.dv_order_dao.biobank_address
    request['contained'][0]['address'] = biobank_address

    location_id = response.location.rsplit('/', 1)[-1]
    self.send_put(
      'SupplyDelivery/{}'.format(location_id),
      request_data=request
    )

    order = self.get_orders()
    self.assertEquals(order[0].biobankCity, 'Rochester')
    self.assertEquals(order[0].biobankStreetAddress1, '3050 Superior Drive NW')
    self.assertEquals(order[0].biobankStateId, 1)
    self.assertEquals(order[0].biobankZipCode, '55901')

    self.assertTrue(response.location.endswith('/SupplyDelivery/999999'))
    self.assertEqual(1, len(order))
    for i in order:
      self.assertEqual(i.id, long(1))
      self.assertEqual(i.order_id, long(999999))

class DvOrderApiTestPutSupplyDelivery(DvOrderApiTestBase):
  mayolink_response = {
      'orders': {
          'order': {
              'status': 'Queued',
        'reference_number': 'somebarcodenumber',
        'received': '2016-12-01T12:00:00-05:00',
        'number': 'WEB1ABCD1234',
        'patient': {
            'medical_record_number': 'PAT-123-456'
        }
      }
    }
  }
  def test_supply_delivery_put(self):
    self.send_post(
      'SupplyRequest',
      request_data=self.get_payload('dv_order_api_post_supply_request.json'),
      expected_status=httplib.CREATED
    )

    response = self.send_post(
      'SupplyDelivery',
      request_data=self.get_payload('dv_order_api_post_supply_delivery.json'),
      expected_status=httplib.CREATED
    )

    location_id = response.location.rsplit('/', 1)[-1]
    self.send_put(
      'SupplyDelivery/{}'.format(location_id),
      request_data=self.get_payload('dv_order_api_put_supply_delivery.json')
    )

    orders = self.get_orders()
    self.assertEqual(1, len(orders))
