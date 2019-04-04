import httplib
import json
import mock

from dao.dv_order_dao import DvOrderDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant import Participant
from test_data import load_test_data_json
from unit_test_util import FlaskTestBase


class DvOrderDaoTestBase(FlaskTestBase):

  def setUp(self):
    super(DvOrderDaoTestBase, self).setUp(use_mysql=True)
    self.post_delivery = load_test_data_json('dv_order_api_post_supply_delivery.json')
    self.post_request = load_test_data_json('dv_order_api_post_supply_request.json')
    self.put_request = load_test_data_json('dv_order_api_put_supply_request.json')
    self.dao = DvOrderDao()

    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()

    self.participant = Participant(
      participantId=123456789,
      biobankId=7
    )
    self.participant_dao.insert(self.participant)
    self.summary = self.participant_summary(self.participant)
    self.summary_dao.insert(self.summary)
    self.mayolink_response = {'orders': {'order': {
      'status': 'finished',
      'reference_number': 'barcode',
      'received': '2019-04-05 12:00:00',
      'number': '12345',
      'patient': {'medical_record_number': 'WEB1ABCD1234'}
    }}}

    mayolinkapi_patcher = mock.patch(
      'dao.dv_order_dao.MayoLinkApi',
      **{'return_value.post.return_value': self.mayolink_response}
    )
    mayolinkapi_patcher.start()
    self.addCleanup(mayolinkapi_patcher.stop)

  def test_insert_biobank_order(self):
    payload = self.send_post('SupplyRequest', request_data=self.post_request, expected_status=httplib.CREATED)
    post_response = json.loads(payload.response[0])
    location = payload.location.rsplit('/', 1)[-1]
    put_response = self.send_put('SupplyRequest/{}'.format(location), request_data=self.put_request)
    print post_response, '<<< post response', '\n'
    print put_response, '<<< put response', '\n'
    self.assertEquals(post_response['version'], 1)
    self.assertEquals(put_response['version'], 2)
    self.assertEquals(put_response['barcode'], 'barcode')
    self.assertEquals(put_response['biobankOrderId'], '12345')
    self.assertEquals(post_response['meta']['versionId'].strip('W/'), '"1"')
    self.assertEquals(put_response['meta']['versionId'].strip('W/'), '"2"')
