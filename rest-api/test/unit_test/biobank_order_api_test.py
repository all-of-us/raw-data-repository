import httplib

from dao.participant_dao import ParticipantDao
from model.participant import Participant
from test.unit_test.unit_test_util import FlaskTestBase
from test import test_data


_PARTICIPANT = 929
_URL_T = 'Participant/P%d/BiobankOrder/%d'


class TestBiobankOrder(FlaskTestBase):
  def test_insert_eval(self):
    ParticipantDao().insert(Participant(participantId=_PARTICIPANT, biobankId=4))

    order_id = 22334
    biobank_order = test_data.load_biobank_order(_PARTICIPANT)
    self.send_post(_URL_T % (_PARTICIPANT, order_id), biobank_order)

    # This should fail because the identifiers are already in use.
    resp = self.send_post(
        _URL_T % (_PARTICIPANT, order_id),
        biobank_order,
        expected_status=httplib.BAD_REQUEST)
    self.assertIn('already exists', resp['message'])


if __name__ == '__main__':
  unittest.main()
