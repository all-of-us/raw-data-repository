import datetime
import httplib
import main

from clock import FakeClock
from code_constants import PPI_SYSTEM, RACE_WHITE_CODE
from concepts import Concept
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from test_data import load_measurement_json
from test.unit_test.unit_test_util import FlaskTestBase, make_questionnaire_response_json

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)

class ParticipantSummaryApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantSummaryApiTest, self).setUp()
    self.provider_link = {
      "primary": True,
      "organization": {
        "display": None,
        "reference": "Organization/PITT",
      },
      "site": [{
        "display": None,
        "reference": "mayo-clinic",
      }],
      "identifier": [{
        "system": "http://any-columbia-mrn-system",
        "value": "MRN456"
      }]
    }

  def testQuery_noParticipants(self):
    self.send_get('Participant/P1/Summary', expected_status=httplib.NOT_FOUND)
    response = self.send_get('ParticipantSummary')
    self.assertBundle([], response)

  def submit_questionnaire_response(self, participant_id, questionnaire_id, race_code, gender_code,
                                    firstName, middleName, lastName, zipCode, dateOfBirth):
    code_answers = []
    if race_code:
      code_answers.append(("race", Concept(PPI_SYSTEM, race_code)))
    if gender_code:
      code_answers.append(("genderIdentity", Concept(PPI_SYSTEM, gender_code)))
    qr = make_questionnaire_response_json(participant_id,
                                          questionnaire_id,
                                          code_answers = code_answers,
                                          string_answers = [("firstName", firstName),
                                                            ("middleName", middleName),
                                                            ("lastName", lastName),
                                                            ("zipCode", zipCode)],
                                          date_answers = [("dateOfBirth", dateOfBirth)])
    with FakeClock(TIME_1):
      self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def testQuery_oneParticipant(self):
    participant = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id = participant['participantId']
    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    first_name = self.fake.first_name()
    middle_name = self.fake.first_name()
    last_name = self.fake.last_name()
    self.submit_questionnaire_response(participant_id, questionnaire_id, RACE_WHITE_CODE, "male",
                                       first_name, middle_name, last_name, "78751",
                                       datetime.date(1978, 10, 9))

    with FakeClock(TIME_2):
      ps = self.send_get('Participant/%s/Summary' % participant_id)
    expected_ps = {'questionnaireOnHealthcareAccess': 'UNSET',
                   'enrollmentStatus': 'INTERESTED',
                   'samplesToIsolateDNA': 'UNSET',
                   'questionnaireOnOverallHealth': 'UNSET',
                   'signUpTime': participant['signUpTime'],
                   'biobankId': participant['biobankId'],
                   'numBaselineSamplesArrived': 0,
                   'questionnaireOnSociodemographics': 'SUBMITTED',
                   'questionnaireOnSociodemographicsTime': TIME_1.isoformat(),
                   'questionnaireOnPersonalHabits': 'UNSET',
                   'questionnaireOnFamilyHealth': 'UNSET',
                   'questionnaireOnMedications': 'UNSET',
                   'physicalMeasurementsStatus': 'UNSET',
                   'genderIdentity': 'male',
                   'consentForElectronicHealthRecords': 'UNSET',
                   'questionnaireOnMedicalHistory': u'UNSET',
                   'participantId': participant_id,
                   'hpoId': 'PITT',
                   'numCompletedBaselinePPIModules': 1,
                   'consentForStudyEnrollment': 'UNSET',
                   'race': 'WHITE',
                   'dateOfBirth': '1978-10-09',
                   'ageRange': '36-45',
                   'firstName': first_name,
                   'middleName': middle_name,
                   'lastName': last_name,
                   'zipCode' : '78751',
                   'withdrawalStatus': 'NOT_WITHDRAWN',
                   'suspensionStatus': 'NOT_SUSPENDED'}
    self.assertJsonResponseMatches(expected_ps, ps)
    response = self.send_get('ParticipantSummary')
    self.assertBundle([_make_entry(ps)], response)

  def _send_next(self, next_link):
    prefix_index = next_link.index(main.PREFIX)
    return self.send_get(next_link[prefix_index + len(main.PREFIX):])

  def assertResponses(self, initial_query, summaries_list):
    response = self.send_get(initial_query)
    for i in range(0, len(summaries_list)):
      summaries = summaries_list[i]
      next_url = self.assertBundle([_make_entry(ps) for ps in summaries], response,
                                   has_next=i < len(summaries_list) - 1)
      if next_url:
        response = self._send_next(next_url)
      else:
        break

  def _submit_empty_questionnaire_response(self, participant_id, questionnaire_id):
    qr = make_questionnaire_response_json(participant_id, questionnaire_id)
    with FakeClock(TIME_1):
      self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def _store_biobank_sample(self, participant, test_code):
    BiobankStoredSampleDao().insert(BiobankStoredSample(
        biobankStoredSampleId='s' + participant['participantId'] + test_code,
        biobankId=participant['biobankId'][1:],
        test=test_code,
        confirmed=TIME_1))

  def testQuery_manyParticipants(self):
    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    questionnaire_id_2 = self.create_questionnaire('questionnaire4.json')
    questionnaire_id_3 = self.create_questionnaire('all_consents_questionnaire.json')
    participant_1 = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id_1 = participant_1['participantId']
    participant_2 = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id_2 = participant_2['participantId']
    participant_3 = self.send_post('Participant', {})
    participant_id_3 = participant_3['participantId']

    self.submit_questionnaire_response(participant_id_1, questionnaire_id, RACE_WHITE_CODE, "male",
                                       "Bob", "Q", "Jones", "78751", datetime.date(1978, 10, 9))
    self.submit_questionnaire_response(participant_id_2, questionnaire_id, None, None,
                                       "Mary", "Q", "Jones", "78751", datetime.date(1978, 10, 8))
    self.submit_questionnaire_response(participant_id_3, questionnaire_id, RACE_WHITE_CODE, "male",
                                       "Fred", "T", "Smith", "78752", datetime.date(1978, 10, 10))
    # Send an empty questionnaire response for the consent questionnaire for participants 2 and 3
    self._submit_empty_questionnaire_response(participant_id_2, questionnaire_id_3)
    self._submit_empty_questionnaire_response(participant_id_3, questionnaire_id_3)

    # Send an empty questionnaire response for another questionnaire for participant 3,
    # completing the baseline PPI modules.
    self._submit_empty_questionnaire_response(participant_id_3, questionnaire_id_2)

    # Send physical measurements for participants 2 and 3
    measurements_2 = load_measurement_json(participant_id_2)
    measurements_3 = load_measurement_json(participant_id_3)
    path_2 = 'Participant/%s/PhysicalMeasurements' % participant_id_2
    path_3 = 'Participant/%s/PhysicalMeasurements' % participant_id_3
    self.send_post(path_2, measurements_2)
    self.send_post(path_3, measurements_3)

    # Store samples for DNA for participants 1 and 3
    self._store_biobank_sample(participant_1, '1ED10')
    self._store_biobank_sample(participant_3, 'Saliva')
    # Update participant summaries based on these changes.
    ParticipantSummaryDao().update_from_biobank_stored_samples()

    ps_1 = self.send_get('Participant/%s/Summary' % participant_id_1)
    ps_2 = self.send_get('Participant/%s/Summary' % participant_id_2)
    ps_3 = self.send_get('Participant/%s/Summary' % participant_id_3)

    self.assertEquals(1, ps_1['numCompletedBaselinePPIModules'])
    self.assertEquals(1, ps_1['numBaselineSamplesArrived'])
    self.assertEquals('UNSET', ps_1['samplesToIsolateDNA'])
    self.assertEquals('INTERESTED', ps_1['enrollmentStatus'])
    self.assertEquals(1, ps_2['numCompletedBaselinePPIModules'])
    self.assertEquals(0, ps_2['numBaselineSamplesArrived'])
    self.assertEquals('UNSET', ps_2['samplesToIsolateDNA'])
    self.assertEquals('MEMBER', ps_2['enrollmentStatus'])
    self.assertEquals(3, ps_3['numCompletedBaselinePPIModules'])
    self.assertEquals(1, ps_1['numBaselineSamplesArrived'])
    self.assertEquals('RECEIVED', ps_3['samplesToIsolateDNA'])
    self.assertEquals('FULL_PARTICIPANT', ps_3['enrollmentStatus'])

    response = self.send_get('ParticipantSummary')
    self.assertBundle([_make_entry(ps_1), _make_entry(ps_2), _make_entry(ps_3)], response)

    self.assertResponses('ParticipantSummary?_count=2', [[ps_1, ps_2], [ps_3]])

    # Test sorting on fields of different types.
    self.assertResponses('ParticipantSummary?_count=2&_sort=firstName',
                         [[ps_1, ps_3], [ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&_sort:asc=firstName',
                         [[ps_1, ps_3], [ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&_sort:desc=firstName',
                         [[ps_2, ps_3], [ps_1]])
    self.assertResponses('ParticipantSummary?_count=2&_sort=dateOfBirth',
                         [[ps_2, ps_1], [ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&_sort:desc=dateOfBirth',
                         [[ps_3, ps_1], [ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&_sort=genderIdentity',
                         [[ps_2, ps_1], [ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&_sort:desc=genderIdentity',
                         [[ps_1, ps_3], [ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&_sort=questionnaireOnSociodemographics',
                         [[ps_1, ps_2], [ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&_sort=hpoId',
                         [[ps_3, ps_1], [ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&_sort:desc=hpoId',
                         [[ps_1, ps_2], [ps_3]])

    # Test filtering on fields.
    self.assertResponses('ParticipantSummary?_count=2&firstName=Mary',
                         [[ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&middleName=Q',
                         [[ps_1, ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&lastName=Smith',
                         [[ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&zipCode=78752',
                         [[ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&hpoId=PITT',
                         [[ps_1, ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&hpoId=UNSET',
                         [[ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&genderIdentity=male',
                         [[ps_1, ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&genderIdentity=UNSET',
                         [[ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&race=WHITE',
                         [[ps_1, ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&middleName=Q&race=WHITE',
                         [[ps_1]])
    self.assertResponses('ParticipantSummary?_count=2&middleName=Q&race=WHITE&zipCode=78752',
                         [[]])
    self.assertResponses('ParticipantSummary?_count=2&questionnaireOnSociodemographics=SUBMITTED',
                         [[ps_1, ps_2], [ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&consentForStudyEnrollment=UNSET',
                         [[ps_1]])
    self.assertResponses('ParticipantSummary?_count=2&consentForStudyEnrollment=SUBMITTED',
                         [[ps_2, ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&physicalMeasurementsStatus=UNSET',
                         [[ps_1]])
    self.assertResponses('ParticipantSummary?_count=2&physicalMeasurementsStatus=COMPLETED',
                         [[ps_2, ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&enrollmentStatus=INTERESTED',
                         [[ps_1]])
    self.assertResponses('ParticipantSummary?_count=2&enrollmentStatus=MEMBER',
                         [[ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&enrollmentStatus=FULL_PARTICIPANT',
                         [[ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=1978-10-08',
                         [[ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=gt1978-10-08',
                         [[ps_1, ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=lt1978-10-08',
                         [[]])
    self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=le1978-10-08',
                         [[ps_2]])
    self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=ge1978-10-08',
                         [[ps_1, ps_2], [ps_3]])
    self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=ne1978-10-09',
                         [[ps_2, ps_3]])

def _make_entry(ps):
  return { 'fullUrl': 'http://localhost/rdr/v1/Participant/%s/Summary' % ps['participantId'],
           'resource': ps }
