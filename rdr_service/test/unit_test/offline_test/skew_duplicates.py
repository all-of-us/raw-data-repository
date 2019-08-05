import datetime
from dateutil import parser

from concepts import Concept
from rdr_service.code_constants import PPI_SYSTEM, PMI_SKIP_CODE, RACE_WHITE_CODE
from rdr_service.model.code import CodeType
from clock import FakeClock
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao

from test.unit_test.unit_test_util import make_questionnaire_response_json, FlaskTestBase, \
                    SqlTestBase

from offline.participant_maint import skew_duplicate_last_modified

TIME_1 = datetime.datetime(2019, 1, 30, 10, 15, 00, 52525)

def _add_code_answer(code_answers, link_id, code):
  if code:
    code_answers.append((link_id, Concept(PPI_SYSTEM, code)))

class SkewDuplicatesTest(FlaskTestBase):
  """Tests setting a flag on participants as a ghost account with date added.
  """

  # Some link ids relevant to the demographics questionnaire
  code_link_ids = (
    'race', 'genderIdentity', 'state', 'sex', 'sexualOrientation', 'recontactMethod', 'language',
    'education', 'income'
  )
  string_link_ids = (
    'firstName', 'middleName', 'lastName', 'streetAddress', 'city', 'phoneNumber', 'zipCode'
  )

  provider_link = {
    "primary": True,
    "organization": {
      "display": None,
      "reference": "Organization/PITT",
    }
  }

  def setUp(self):
    super(SkewDuplicatesTest, self).setUp(use_mysql=True)
    self.participant_summary_dao = ParticipantSummaryDao()

  def create_demographics_questionnaire(self):
    """Uses the demographics test data questionnaire.  Returns the questionnaire id"""
    return self.create_questionnaire('questionnaire3.json')

  def post_demographics_questionnaire(self,
                                      participant_id,
                                      questionnaire_id,
                                      cabor_signature_string=False,
                                      time=TIME_1, **kwargs):
    """POSTs answers to the demographics questionnaire for the participant"""
    answers = {'code_answers': [],
               'string_answers': [],
               'date_answers': [('dateOfBirth', kwargs.get('dateOfBirth'))]}
    if cabor_signature_string:
      answers['string_answers'].append(('CABoRSignature', kwargs.get('CABoRSignature')))
    else:
      answers['uri_answers'] = [('CABoRSignature', kwargs.get('CABoRSignature'))]

    for link_id in self.code_link_ids:
      if link_id in kwargs:
        concept = Concept(PPI_SYSTEM, kwargs[link_id])
        answers['code_answers'].append((link_id, concept))

    for link_id in self.string_link_ids:
      code = kwargs.get(link_id)
      answers['string_answers'].append((link_id, code))

    response_data = make_questionnaire_response_json(participant_id, questionnaire_id, **answers)

    with FakeClock(time):
      url = 'Participant/%s/QuestionnaireResponse' % participant_id
      return self.send_post(url, request_data=response_data)

  def test_last_modified_sync(self):
    SqlTestBase.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)
    questionnaire_id = self.create_demographics_questionnaire()
    t1 = TIME_1

    total_dups = 15

    def setup_participant(when, providerLink=self.provider_link):
      # Set up participant, questionnaire, and consent
      with FakeClock(when):
        participant = self.send_post('Participant', {"providerLink": [providerLink]})
        participant_id = participant['participantId']
        self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
          'race': RACE_WHITE_CODE,
          'genderIdentity': PMI_SKIP_CODE,
          'firstName': self.fake.first_name(),
          'middleName': self.fake.first_name(),
          'lastName': self.fake.last_name(),
          'zipCode': '78751',
          'state': PMI_SKIP_CODE,
          'streetAddress': '1234 Main Street',
          'city': 'Austin',
          'sex': PMI_SKIP_CODE,
          'sexualOrientation': PMI_SKIP_CODE,
          'phoneNumber': '512-555-5555',
          'recontactMethod': PMI_SKIP_CODE,
          'language': PMI_SKIP_CODE,
          'education': PMI_SKIP_CODE,
          'income': PMI_SKIP_CODE,
          'dateOfBirth': datetime.date(1978, 10, 9),
          'CABoRSignature': 'signature.pdf',
        }
      self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
      return participant

    # Create the first batch and fetch their summaries
    batch = [setup_participant(t1) for _ in range(total_dups)]

    summaries = list()
    for record in batch:
      summaries.append(self.send_get('Participant/{0}/Summary'.format(record['participantId'])))

    skew_duplicate_last_modified()

    self.assertEqual(total_dups, len(summaries))

    for item in summaries:
      response = self.send_get('Participant/{0}/Summary'.format(item['participantId']))

      ts1 = parser.parse(item['lastModified'])
      ts2 = parser.parse(response['lastModified'])

      # Make sure timestamp is different
      self.assertNotEqual(ts1, ts2)

      # reset microseconds to zero and make sure timestamps are identical
      ts1 = ts1.replace(microsecond = 0)
      ts2 = ts2.replace(microsecond = 0)
      self.assertEqual(ts1, ts2)
