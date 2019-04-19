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
from test_data import load_test_data_json
from unit_test_util import FlaskTestBase


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
    # TODO: confirm parsed correct information from payload


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
    orders = self.get_orders()
    self.assertEqual(1, len(orders))
    for i in orders:
      self.assertEqual(i.barcode, 'somebarcodenumber')
      self.assertEqual(i.id, long(1))
      self.assertEqual(i.order_id, long(999999))
      self.assertEqual(i.biobankOrderId, 'WEB1ABCD1234')
      self.assertEqual(i.biobankStatus, 'Delivered')

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

class DvOrderApiTestPostSupplyDelivery(DvOrderApiTestBase):

  def test_supply_delivery_fails_without_supply_request(self):
    self.send_post(
      'SupplyDelivery',
      request_data=self.get_payload('dv_order_api_post_supply_delivery.json'),
      expected_status=httplib.CONFLICT
    )

  def test_delivery_pass_after_supply_request(self):
    response = self.send_post(
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
    biobank_address['type'] = 'postal'
    biobank_address['use'] = 'home'
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
    self.assertEqual(1, len(order))
    for i in order:
      self.assertEqual(i.id, long(1))
      self.assertEqual(i.order_id, long(999999))

