from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import load_biobank_order_json
from model.utils import to_client_participant_id
from model.participant import Participant
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from participant_enums import SampleStatus

class DataGenApiTest(FlaskTestBase):
  def setUp(self):
    super(DataGenApiTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.participant_id = to_client_participant_id(self.participant.participantId)
    self.order_path = ('Participant/%s/BiobankOrder' % self.participant_id)

  def test_generate_samples(self):
    ParticipantSummaryDao().insert(self.participant_summary(self.participant))
    self.create_and_verify_created_obj(
        self.order_path, load_biobank_order_json(self.participant.participantId))
    self.send_post('DataGen', { 'create_biobank_samples': self.participant_id})
    self.assertEquals(7, len(BiobankStoredSampleDao().get_all()))
    ps = ParticipantSummaryDao().get(self.participant.participantId)
    self.assertEquals(SampleStatus.RECEIVED, ps.samplesToIsolateDNA)
    self.assertEquals(6, ps.numBaselineSamplesArrived)