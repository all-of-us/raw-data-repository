import httplib

from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import load_biobank_order_json
from model.utils import to_client_participant_id
from model.participant import Participant
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao

class BiobankOrderApiTest(FlaskTestBase):
  def setUp(self):
    super(BiobankOrderApiTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.path = (
        'Participant/%s/BiobankOrder' % to_client_participant_id(self.participant.participantId))

  def test_insert_and_refetch(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.create_and_verify_created_obj(
        self.path, load_biobank_order_json(self.participant.participantId))

  def test_error_no_summary(self):
    order_json = load_biobank_order_json(self.participant.participantId)
    self.send_post(self.path, order_json, expected_status=httplib.BAD_REQUEST)

  def test_error_missing_required_fields(self):
    order_json = load_biobank_order_json(self.participant.participantId)
    del order_json['identifier']
    self.send_post(self.path, order_json, expected_status=httplib.BAD_REQUEST)

  def test_no_duplicate_test_within_order(self):
    order_json = load_biobank_order_json(self.participant.participantId)
    order_json['samples'].extend(list(order_json['samples']))
    self.send_post(self.path, order_json, expected_status=httplib.BAD_REQUEST)
