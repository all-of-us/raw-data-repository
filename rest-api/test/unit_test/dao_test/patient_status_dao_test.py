import copy

from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.patient_status_dao import PatientStatusDao
from dateutil.parser import parse
from model.participant import Participant
from test.unit_test.unit_test_util import FlaskTestBase


class PatientStatusTestBase(FlaskTestBase):

  def setUp(self, use_mysql=True, with_data=True, with_consent_codes=False):
    super(PatientStatusTestBase, self).\
        setUp(use_mysql=use_mysql, with_data=with_data, with_consent_codes=with_consent_codes)

    self.test_data = {
      "subject": "Patient/P123456789",
      "awardee": "PITT",
      "organization": "PITT_BANNER_HEALTH",
      "patient_status": "YES",
      "user": "john.doe@pmi-ops.org",
      "site": "hpo-site-monroeville",
      "authored": "2019-04-26T12:11:41Z",
      "comment": "This is comment"
    }

    self.dao = PatientStatusDao()
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()

    self.participant = Participant(participantId=123456789, biobankId=7)
    self.participant_dao.insert(self.participant)
    self.summary = self.participant_summary(self.participant)
    self.summary_dao.insert(self.summary)


  def test_patient_status(self):

    data = copy.copy(self.test_data)
    model = self.dao.from_client_json(data, participant_id=self.participant.participantId)
    self.dao.insert(model)
    result = self.dao.get(self.participant.participantId, data['organization'])

    self.assertEqual(result['subject'], data['subject'])
    self.assertEqual(result['organization'], data['organization'])
    self.assertEqual(result['site'], data['site'])
    self.assertEqual(parse(result['authored']), parse(data['authored']).replace(tzinfo=None))
    self.assertEqual(result['comment'], data['comment'])

    # Test changing site
    data['authored'] = '2019-04-27T16:32:01Z'
    data['comment'] = 'saw patient at new site'
    data['site'] = 'hpo-site-bannerphoenix'
    model = self.dao.from_client_json(data, participant_id=self.participant.participantId)
    self.dao.update(model)
    result = self.dao.get(self.participant.participantId, data['organization'])

    self.assertEqual(result['subject'], data['subject'])
    self.assertEqual(result['organization'], data['organization'])
    self.assertEqual(result['site'], data['site'])
    self.assertEqual(parse(result['authored']), parse(data['authored']).replace(tzinfo=None))
    self.assertEqual(result['comment'], data['comment'])

  # TODO: When new style history tables and triggers have been added to unit tests, test dao.get_history().
