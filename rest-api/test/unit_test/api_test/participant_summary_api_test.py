import datetime
import httplib
import main
import threading

from clock import FakeClock
from code_constants import PPI_SYSTEM, RACE_WHITE_CODE, CONSENT_PERMISSION_YES_CODE
from code_constants import RACE_NONE_OF_THESE_CODE
from concepts import Concept
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.code import CodeType
from model.biobank_stored_sample import BiobankStoredSample
from test_data import load_measurement_json, load_biobank_order_json
from unit_test_util import FlaskTestBase, make_questionnaire_response_json, SqlTestBase

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_5 = datetime.datetime(2016, 1, 5, 0, 1)

class ParticipantSummaryMySqlApiTest(FlaskTestBase):
  def setUp(self):
    super(ParticipantSummaryMySqlApiTest, self).setUp(use_mysql=True)
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

  def testUpdate_raceCondition(self):
    self.create_questionnaire('questionnaire3.json')
    participant = self.send_post('Participant', {})
    participant_id = participant['participantId']
    participant['providerLink'] = [self.provider_link]

    t1 = threading.Thread(target=lambda:
                          self.send_put('Participant/%s' % participant_id, participant,
                                        headers={'If-Match': participant['meta']['versionId']}))

    t2 = threading.Thread(target=lambda:
                          self.send_consent(participant_id))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # The participant summary should exist (consent has been received), and it should have PITT
    # for its HPO ID (the participant update occurred.)
    # This used to fail a decent percentage of the time, before we started using FOR UPDATE in
    # our update statements; see DA-256.
    ps = self.send_get('Participant/%s/Summary' % participant_id)
    self.assertEquals('PITT', ps.get('hpoId'))

class ParticipantSummaryApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantSummaryApiTest, self).setUp()
    self.provider_link = {
      "primary": True,
      "organization": {
        "display": None,
        "reference": "Organization/PITT",
      }
    }

  def test_pairing_summary(self):
    participant = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id = participant['participantId']
    path = 'Participant/%s' % participant_id
    participant['awardee'] = 'PITT'
    particpant_update = self.send_put(path, participant, headers={'If-Match': 'W/"1"'})
    self.assertEquals(particpant_update['awardee'], participant['awardee'])
    participant['organization'] = 'AZ_TUCSON_BANNER_HEALTH'
    participant_update_2 = self.send_put(path, participant, headers={'If-Match': 'W/"2"'})
    self.assertEquals(participant_update_2['organization'], participant['organization'])
    self.assertEquals(participant_update_2['awardee'], 'AZ_TUCSON')

  def testQuery_noParticipants(self):
    self.send_get('Participant/P1/Summary', expected_status=httplib.NOT_FOUND)
    response = self.send_get('ParticipantSummary')
    self.assertBundle([], response)

  def submit_questionnaire_response(self, participant_id, questionnaire_id, race_code, gender_code,
                                    first_name, middle_name, last_name, zip_code,
                                    state_code, street_address, city, sex_code,
                                    sexual_orientation_code, phone_number, recontact_method_code,
                                    language_code, education_code, income_code, date_of_birth,
                                    cabor_signature_uri):
    code_answers = []
    _add_code_answer(code_answers, "race", race_code)
    _add_code_answer(code_answers, "genderIdentity", gender_code)
    _add_code_answer(code_answers, "state", state_code)
    _add_code_answer(code_answers, "sex", sex_code)
    _add_code_answer(code_answers, "sexualOrientation", sexual_orientation_code)
    _add_code_answer(code_answers, "recontactMethod", recontact_method_code)
    _add_code_answer(code_answers, "language", language_code)
    _add_code_answer(code_answers, "education", education_code)
    _add_code_answer(code_answers, "income", income_code)

    qr = make_questionnaire_response_json(participant_id,
                                          questionnaire_id,
                                          code_answers = code_answers,
                                          string_answers = [("firstName", first_name),
                                                            ("middleName", middle_name),
                                                            ("lastName", last_name),
                                                            ("streetAddress", street_address),
                                                            ("city", city),
                                                            ("phoneNumber", phone_number),
                                                            ("zipCode", zip_code)],
                                          date_answers = [("dateOfBirth", date_of_birth)],
                                          uri_answers = [("CABoRSignature", cabor_signature_uri)])
    with FakeClock(TIME_1):
      self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def testQuery_noSummaries(self):
    participant = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id = participant['participantId']
    self.send_get('Participant/%s/Summary' % participant_id, expected_status=httplib.NOT_FOUND)
    response = self.send_get('ParticipantSummary')
    self.assertBundle([], response)

  def testQuery_oneParticipant(self):
    # Set up the codes so they are mapped later.
    SqlTestBase.setup_codes(["PIIState_VA", "male_sex", "male", "straight", "email_code", "en",
                             "highschool", "lotsofmoney"], code_type=CodeType.ANSWER)
    participant = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id = participant['participantId']
    with FakeClock(TIME_1):
      self.send_consent(participant_id)
    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    first_name = self.fake.first_name()
    middle_name = self.fake.first_name()
    last_name = self.fake.last_name()
    self.submit_questionnaire_response(participant_id, questionnaire_id, RACE_WHITE_CODE, "male",
                                       first_name, middle_name, last_name, "78751", "PIIState_VA",
                                       "1234 Main Street", "Austin", "male_sex",
                                       "straight", "512-555-5555", "email_code",
                                       "en", "highschool", "lotsofmoney",
                                       datetime.date(1978, 10, 9), "signature.pdf")
    with FakeClock(TIME_2):
      ps = self.send_get('Participant/%s/Summary' % participant_id)
    expected_ps = {'questionnaireOnHealthcareAccess': 'UNSET',
                   'enrollmentStatus': 'INTERESTED',
                   'samplesToIsolateDNA': 'UNSET',
                   'questionnaireOnOverallHealth': 'UNSET',
                   'signUpTime': participant['signUpTime'],
                   'biobankId': participant['biobankId'],
                   'numBaselineSamplesArrived': 0,
                   'questionnaireOnTheBasics': 'SUBMITTED',
                   'questionnaireOnTheBasicsTime': TIME_1.isoformat(),
                   'questionnaireOnLifestyle': 'UNSET',
                   'questionnaireOnFamilyHealth': 'UNSET',
                   'questionnaireOnMedications': 'UNSET',
                   'physicalMeasurementsStatus': 'UNSET',
                   'state': 'PIIState_VA',
                   'streetAddress': '1234 Main Street',
                   'city': 'Austin',
                   'sex': 'male_sex',
                   'sexualOrientation': 'straight',
                   'phoneNumber': '512-555-5555',
                   'recontactMethod': 'email_code',
                   'language': 'en',
                   'education': 'highschool',
                   'income': 'lotsofmoney',
                   'biospecimenStatus': 'UNSET',
                   'biospecimenSourceSite': 'UNSET',
                   'biospecimenCollectedSite': 'UNSET',
                   'biospecimenProcessedSite': 'UNSET',
                   'biospecimenFinalizedSite': 'UNSET',
                   'physicalMeasurementsCreatedSite': 'UNSET',
                   'physicalMeasurementsFinalizedSite': 'UNSET',
                   'sampleOrderStatus1ED04': 'UNSET',
                   'sampleOrderStatus1ED10': 'UNSET',
                   'sampleOrderStatus1HEP4': 'UNSET',
                   'sampleOrderStatus1PST8': 'UNSET',
                   'sampleOrderStatus1PS08': 'UNSET',
                   'sampleOrderStatus1SAL': 'UNSET',
                   'sampleOrderStatus1SST8': 'UNSET',
                   'sampleOrderStatus1SS08': 'UNSET',
                   'sampleOrderStatus1UR10': 'UNSET',
                   'sampleOrderStatus2ED10': 'UNSET',
                   'sampleStatus1ED04': 'UNSET',
                   'sampleStatus1ED10': 'UNSET',
                   'sampleStatus1HEP4': 'UNSET',
                   'sampleStatus1PST8': 'UNSET',
                   'sampleStatus1PS08': 'UNSET',
                   'sampleStatus1SAL': 'UNSET',
                   'sampleStatus1SST8': 'UNSET',
                   'sampleStatus1SS08': 'UNSET',
                   'sampleStatus1UR10': 'UNSET',
                   'sampleStatus2ED10': 'UNSET',
                   'genderIdentity': 'male',
                   'consentForElectronicHealthRecords': 'UNSET',
                   'consentForCABoR': 'SUBMITTED',
                   'consentForCABoRTime': TIME_1.isoformat(),
                   'questionnaireOnMedicalHistory': u'UNSET',
                   'participantId': participant_id,
                   'hpoId': 'PITT',
                   'awardee': 'PITT',
                   'site': 'UNSET',
                   'organization': 'UNSET',
                   'numCompletedPPIModules': 1,
                   'numCompletedBaselinePPIModules': 1,
                   'consentForStudyEnrollment': 'SUBMITTED',
                   'consentForStudyEnrollmentTime': TIME_1.isoformat(),
                   'race': 'WHITE',
                   'dateOfBirth': '1978-10-09',
                   'ageRange': '36-45',
                   'firstName': first_name,
                   'middleName': middle_name,
                   'lastName': last_name,
                   'email': self.email,
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

  def _submit_consent_questionnaire_response(self, participant_id, questionnaire_id,
                                             ehr_consent_answer, time=TIME_1):
    code_answers = []
    _add_code_answer(code_answers, "ehrConsent", ehr_consent_answer)
    qr = make_questionnaire_response_json(participant_id, questionnaire_id,
                                          code_answers=code_answers)
    with FakeClock(time):
      self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def _submit_empty_questionnaire_response(self, participant_id, questionnaire_id):
    qr = make_questionnaire_response_json(participant_id, questionnaire_id)
    with FakeClock(TIME_1):
      self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def _send_biobank_order(self, participant_id, order):
    with FakeClock(TIME_1):
      self.send_post('Participant/%s/BiobankOrder' % participant_id, order)

  def _store_biobank_sample(self, participant, test_code):
    BiobankStoredSampleDao().insert(BiobankStoredSample(
        biobankStoredSampleId='s' + participant['participantId'] + test_code,
        biobankId=participant['biobankId'][1:],
        test=test_code,
        biobankOrderIdentifier='KIT',
        confirmed=TIME_1))

  def testQuery_ehrConsent(self):
    questionnaire_id = self.create_questionnaire('all_consents_questionnaire.json')
    participant_1 = self.send_post('Participant', {})
    participant_id_1 = participant_1['participantId']
    self.send_consent(participant_id_1)
    ps_1 = self.send_get('Participant/%s/Summary' % participant_id_1)
    self.assertEquals('UNSET', ps_1['consentForElectronicHealthRecords'])

    self._submit_consent_questionnaire_response(participant_id_1, questionnaire_id, 'NOPE')
    ps_1 = self.send_get('Participant/%s/Summary' % participant_id_1)
    self.assertEquals('SUBMITTED_NO_CONSENT', ps_1['consentForElectronicHealthRecords'])

    self._submit_consent_questionnaire_response(participant_id_1, questionnaire_id,
                                                CONSENT_PERMISSION_YES_CODE)
    ps_1 = self.send_get('Participant/%s/Summary' % participant_id_1)
    self.assertEquals('SUBMITTED', ps_1['consentForElectronicHealthRecords'])

  def testQuery_manyParticipants(self):
    SqlTestBase.setup_codes(["PIIState_VA", "male_sex", "male", "straight", "email_code", "en",
                             "highschool", "lotsofmoney"], code_type=CodeType.ANSWER)

    questionnaire_id = self.create_questionnaire('questionnaire3.json')
    questionnaire_id_2 = self.create_questionnaire('questionnaire4.json')
    questionnaire_id_3 = self.create_questionnaire('all_consents_questionnaire.json')
    participant_1 = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id_1 = participant_1['participantId']
    participant_2 = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id_2 = participant_2['participantId']
    participant_3 = self.send_post('Participant', {})
    participant_id_3 = participant_3['participantId']
    with FakeClock(TIME_1):
      self.send_consent(participant_id_1)
      self.send_consent(participant_id_2)
      self.send_consent(participant_id_3)

    self.submit_questionnaire_response(participant_id_1, questionnaire_id, RACE_WHITE_CODE, "male",
                                       "Bob", "Q", "Jones", "78751", "PIIState_VA",
                                       "1234 Main Street", "Austin", "male_sex",
                                       "straight", "512-555-5555", "email_code",
                                       "en", "highschool", "lotsofmoney",
                                       datetime.date(1978, 10, 9), "signature.pdf")
    self.submit_questionnaire_response(participant_id_2, questionnaire_id, None, "female",
                                       "Mary", "Q", "Jones", "78751", None,
                                       None, None, None, None, None, None, None, None, None,
                                       datetime.date(1978, 10, 8), None)
    self.submit_questionnaire_response(participant_id_3, questionnaire_id, RACE_NONE_OF_THESE_CODE, "male",
                                       "Fred", "T", "Smith", "78752", None,
                                       None, None, None, None, None, None, None, None, None,
                                       datetime.date(1978, 10, 10), None)
    # Send a questionnaire response for the consent questionnaire for participants 2 and 3
    self._submit_consent_questionnaire_response(participant_id_2, questionnaire_id_3,
                                                CONSENT_PERMISSION_YES_CODE)
    self._submit_consent_questionnaire_response(participant_id_3, questionnaire_id_3,
                                                CONSENT_PERMISSION_YES_CODE)

    # Send an empty questionnaire response for another questionnaire for participant 3,
    # completing the baseline PPI modules.
    self._submit_empty_questionnaire_response(participant_id_3, questionnaire_id_2)

    # Send physical measurements for participants 2 and 3
    measurements_2 = load_measurement_json(participant_id_2, TIME_1.isoformat())
    measurements_3 = load_measurement_json(participant_id_3, TIME_1.isoformat())
    path_2 = 'Participant/%s/PhysicalMeasurements' % participant_id_2
    path_3 = 'Participant/%s/PhysicalMeasurements' % participant_id_3
    with FakeClock(TIME_2):
      self.send_post(path_2, measurements_2)
      # This pairs participant 3 with PITT and updates their version.
      self.send_post(path_3, measurements_3)


    # Send a biobank order for participant 1
    order_json = load_biobank_order_json(int(participant_id_1[1:]))
    self._send_biobank_order(participant_id_1, order_json)

    # Store samples for DNA for participants 1 and 3
    self._store_biobank_sample(participant_1, '1ED10')
    self._store_biobank_sample(participant_3, '1SAL')
    # Update participant summaries based on these changes.
    ParticipantSummaryDao().update_from_biobank_stored_samples()
    # Update version for participant 3, which has changed.
    participant_3 = self.send_get('Participant/%s' % participant_id_3)

    with FakeClock(TIME_3):
      participant_2['withdrawalStatus'] = 'NO_USE'
      participant_3['suspensionStatus'] = 'NO_CONTACT'
      participant_3['site'] = 'hpo-site-monroeville'
      self.send_put('Participant/%s' % participant_id_2, participant_2,
                    headers={ 'If-Match':'W/"2"'})
      self.send_put('Participant/%s' % participant_id_3, participant_3,
                     headers={ 'If-Match': participant_3['meta']['versionId'] })

    with FakeClock(TIME_4):
      ps_1 = self.send_get('Participant/%s/Summary' % participant_id_1)
      ps_2 = self.send_get('Participant/%s/Summary' % participant_id_2)
      ps_3 = self.send_get('Participant/%s/Summary' % participant_id_3)

    self.assertEquals(1, ps_1['numCompletedBaselinePPIModules'])
    self.assertEquals(1, ps_1['numBaselineSamplesArrived'])
    self.assertEquals('RECEIVED', ps_1['sampleStatus1ED10'])
    self.assertEquals(TIME_1.isoformat(), ps_1['sampleStatus1ED10Time'])
    self.assertEquals('UNSET', ps_1['sampleStatus1SAL'])
    self.assertEquals('UNSET', ps_1['samplesToIsolateDNA'])
    self.assertEquals('INTERESTED', ps_1['enrollmentStatus'])
    self.assertEquals('UNSET', ps_1['physicalMeasurementsStatus'])
    self.assertIsNone(ps_1.get('physicalMeasurementsTime'))
    self.assertEquals('male', ps_1['genderIdentity'])
    self.assertEquals('NOT_WITHDRAWN', ps_1['withdrawalStatus'])
    self.assertEquals('NOT_SUSPENDED', ps_1['suspensionStatus'])
    self.assertEquals('email_code', ps_1['recontactMethod'])
    self.assertIsNone(ps_1.get('withdrawalTime'))
    self.assertIsNone(ps_1.get('suspensionTime'))
    self.assertEquals('UNSET', ps_1['physicalMeasurementsCreatedSite'])
    self.assertEquals('UNSET', ps_1['physicalMeasurementsFinalizedSite'])
    self.assertIsNone(ps_1.get('physicalMeasurementsTime'))
    self.assertIsNone(ps_1.get('physicalMeasurementsFinalizedTime'))
    self.assertEquals('FINALIZED', ps_1['biospecimenStatus'])
    self.assertEquals('2016-01-04T09:40:21', ps_1['biospecimenOrderTime'])
    self.assertEquals('hpo-site-monroeville', ps_1['biospecimenSourceSite'])
    self.assertEquals('hpo-site-monroeville', ps_1['biospecimenCollectedSite'])
    self.assertEquals('hpo-site-monroeville', ps_1['biospecimenProcessedSite'])
    self.assertEquals('hpo-site-bannerphoenix', ps_1['biospecimenFinalizedSite'])
    self.assertEquals('UNSET', ps_1['sampleOrderStatus1ED04'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus1ED10'])
    self.assertEquals('2016-01-04T10:55:41', ps_1['sampleOrderStatus1ED10Time'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus1PST8'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus2ED10'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus1SST8'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus1HEP4'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus1UR10'])
    self.assertEquals('FINALIZED', ps_1['sampleOrderStatus1SAL'])

    # One day after participant 2 withdraws, their fields are still all populated.
    self.assertEquals(1, ps_2['numCompletedBaselinePPIModules'])
    self.assertEquals(0, ps_2['numBaselineSamplesArrived'])
    self.assertEquals('UNSET', ps_2['sampleStatus1ED10'])
    self.assertEquals('UNSET', ps_2['sampleStatus1SAL'])
    self.assertEquals('UNSET', ps_2['samplesToIsolateDNA'])
    self.assertEquals('MEMBER', ps_2['enrollmentStatus'])
    self.assertEquals('COMPLETED', ps_2['physicalMeasurementsStatus'])
    self.assertEquals(TIME_2.isoformat(), ps_2['physicalMeasurementsTime'])
    self.assertEquals('UNMAPPED', ps_2['genderIdentity'])
    self.assertEquals('NO_USE', ps_2['withdrawalStatus'])
    self.assertEquals('NOT_SUSPENDED', ps_2['suspensionStatus'])
    self.assertEquals('NO_CONTACT', ps_2['recontactMethod'])
    self.assertIsNotNone(ps_2['withdrawalTime'])
    self.assertEquals('hpo-site-monroeville', ps_2['physicalMeasurementsCreatedSite'])
    self.assertEquals('hpo-site-bannerphoenix', ps_2['physicalMeasurementsFinalizedSite'])
    self.assertEquals(TIME_2.isoformat(), ps_2['physicalMeasurementsTime'])
    self.assertEquals(TIME_1.isoformat(), ps_2['physicalMeasurementsFinalizedTime'])
    self.assertEquals('UNSET', ps_2['biospecimenStatus'])
    self.assertIsNone(ps_2.get('biospecimenOrderTime'))
    self.assertEquals('UNSET', ps_2['biospecimenSourceSite'])
    self.assertEquals('UNSET', ps_2['biospecimenCollectedSite'])
    self.assertEquals('UNSET', ps_2['biospecimenProcessedSite'])
    self.assertEquals('UNSET', ps_2['biospecimenFinalizedSite'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1ED04'])
    self.assertIsNone(ps_2.get('sampleOrderStatus1ED10Time'))
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1ED10'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1PST8'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus2ED10'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1SST8'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1HEP4'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1UR10'])
    self.assertEquals('UNSET', ps_2['sampleOrderStatus1SAL'])


    self.assertIsNone(ps_2.get('suspensionTime'))
    self.assertEquals(3, ps_3['numCompletedBaselinePPIModules'])
    self.assertEquals(0, ps_3['numBaselineSamplesArrived'])
    self.assertEquals('UNSET', ps_3['sampleStatus1ED10'])
    self.assertEquals('RECEIVED', ps_3['sampleStatus1SAL'])
    self.assertEquals(TIME_1.isoformat(), ps_3['sampleStatus1SALTime'])
    self.assertEquals('RECEIVED', ps_3['samplesToIsolateDNA'])
    self.assertEquals('FULL_PARTICIPANT', ps_3['enrollmentStatus'])
    self.assertEquals('COMPLETED', ps_3['physicalMeasurementsStatus'])
    self.assertEquals(TIME_2.isoformat(), ps_3['physicalMeasurementsTime'])
    self.assertEquals('male', ps_3['genderIdentity'])
    self.assertEquals('NOT_WITHDRAWN', ps_3['withdrawalStatus'])
    self.assertEquals('NO_CONTACT', ps_3['suspensionStatus'])
    self.assertEquals('NO_CONTACT', ps_3['recontactMethod'])
    self.assertEquals('hpo-site-monroeville', ps_3['site'])
    self.assertIsNone(ps_3.get('withdrawalTime'))
    self.assertIsNotNone(ps_3['suspensionTime'])

    # One day after participant 2 withdraws, the participant is still returned.
    with FakeClock(TIME_4):
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
                           [[ps_1, ps_3], [ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=genderIdentity',
                           [[ps_2, ps_1], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort=questionnaireOnTheBasics',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort=hpoId',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=hpoId',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=awardee',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=organization',
                           [[ps_1, ps_3], [ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:asc=site',
                           [[ps_2, ps_1], [ps_3]])
                           # Test filtering on fields.
      self.assertResponses('ParticipantSummary?_count=2&firstName=Mary',
                           [[ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&site=hpo-site-monroeville',
                           [[ps_1, ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&awardee=PITT',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&organization=AZ_TUCSON_BANNER_HEALTH',
                           [])
      self.assertResponses('ParticipantSummary?_count=2&middleName=Q',
                           [[ps_1, ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&lastName=Smith',
                           [[ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&zipCode=78752',
                           [[ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&hpoId=PITT',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&hpoId=UNSET',
                           [[]])
      self.assertResponses('ParticipantSummary?_count=2&genderIdentity=male',
                           [[ps_1, ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&race=WHITE',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&race=OTHER_RACE',
                           [[ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&middleName=Q&race=WHITE',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&middleName=Q&race=WHITE&zipCode=78752',
                           [[]])
      self.assertResponses('ParticipantSummary?_count=2&questionnaireOnTheBasics=SUBMITTED',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&consentForStudyEnrollment=SUBMITTED',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&consentForCABoR=SUBMITTED',
                           [[ps_1]])
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
      self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=ge1978-10-08&'
                           'dateOfBirth=le1978-10-09', [[ps_1, ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&dateOfBirth=ne1978-10-09',
                           [[ps_2, ps_3]])

      self.assertResponses('ParticipantSummary?_count=2&withdrawalStatus=NOT_WITHDRAWN',
                           [[ps_1, ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&withdrawalStatus=NO_USE',
                           [[ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&withdrawalTime=lt2016-01-03',
                           [[]])
      self.assertResponses('ParticipantSummary?_count=2&withdrawalTime=ge2016-01-03',
                           [[ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&suspensionStatus=NOT_SUSPENDED',
                           [[ps_1, ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&suspensionStatus=NO_CONTACT',
                           [[ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&suspensionTime=lt2016-01-03',
                           [[]])
      self.assertResponses('ParticipantSummary?_count=2&suspensionTime=ge2016-01-03',
                           [[ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&physicalMeasurementsCreatedSite=UNSET',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&'
                           + 'physicalMeasurementsCreatedSite=hpo-site-monroeville',
                           [[ps_2, ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&physicalMeasurementsFinalizedSite=UNSET',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&'
                           + 'physicalMeasurementsFinalizedSite=hpo-site-bannerphoenix',
                           [[ps_2, ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&physicalMeasurementsStatus=UNSET',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&physicalMeasurementsStatus=COMPLETED',
                           [[ps_2, ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&biospecimenStatus=FINALIZED',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&biospecimenOrderTime=ge2016-01-04',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&biospecimenOrderTime=lt2016-01-04',
                           [[]])
      self.assertResponses('ParticipantSummary?_count=2&' +
                           'biospecimenSourceSite=hpo-site-monroeville',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&' +
                           'biospecimenCollectedSite=hpo-site-monroeville',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&' +
                           'biospecimenProcessedSite=hpo-site-monroeville',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&' +
                           'biospecimenFinalizedSite=hpo-site-bannerphoenix',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&sampleOrderStatus1ED04=UNSET',
                           [[ps_1, ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&sampleOrderStatus1ED10=FINALIZED',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&sampleOrderStatus1ED10Time=ge2016-01-04',
                           [[ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&sampleOrderStatus1ED10Time=lt2016-01-04',
                           [[]])

    # Two days after participant 2 withdraws, their fields are not set for anything but
    # participant ID, HPO ID, withdrawal status, and withdrawal time
    with FakeClock(TIME_5):
      new_ps_1 = self.send_get('Participant/%s/Summary' % participant_id_1)
      new_ps_2 = self.send_get('Participant/%s/Summary' % participant_id_2)
      new_ps_3 = self.send_get('Participant/%s/Summary' % participant_id_3)

    self.assertEquals(ps_1, new_ps_1)
    self.assertEquals(ps_3, new_ps_3)
    self.assertEquals('Mary', new_ps_2['firstName'])
    self.assertEquals('Q', new_ps_2['middleName'])
    self.assertEquals('Jones', new_ps_2['lastName'])
    self.assertIsNone(new_ps_2.get('numCompletedBaselinePPIModules'))
    self.assertIsNone(new_ps_2.get('numBaselineSamplesArrived'))
    self.assertEquals('UNSET', new_ps_2['sampleStatus1ED10'])
    self.assertEquals('UNSET', new_ps_2['sampleStatus1SAL'])
    self.assertEquals('UNSET', new_ps_2['samplesToIsolateDNA'])
    self.assertEquals('UNSET', new_ps_2['enrollmentStatus'])
    self.assertEquals('UNSET', new_ps_2['physicalMeasurementsStatus'])
    self.assertEquals('SUBMITTED', new_ps_2['consentForStudyEnrollment'])
    self.assertIsNotNone(new_ps_2['consentForStudyEnrollmentTime'])
    self.assertEquals('SUBMITTED', new_ps_2['consentForElectronicHealthRecords'])
    self.assertIsNotNone(new_ps_2['consentForElectronicHealthRecordsTime'])
    self.assertIsNone(new_ps_2.get('physicalMeasurementsTime'))
    self.assertEquals('UNSET', new_ps_2['genderIdentity'])
    self.assertEquals('NO_USE', new_ps_2['withdrawalStatus'])
    self.assertEquals(ps_2['biobankId'], new_ps_2['biobankId'])
    self.assertEquals('UNSET', new_ps_2['suspensionStatus'])
    self.assertEquals('NO_CONTACT', new_ps_2['recontactMethod'])
    self.assertEquals('PITT', new_ps_2['hpoId'])
    self.assertEquals(participant_id_2, new_ps_2['participantId'])
    self.assertIsNotNone(ps_2['withdrawalTime'])
    self.assertIsNone(new_ps_2.get('suspensionTime'))

    # Queries that filter on fields not returned for withdrawn participants no longer return
    # participant 2; queries that filter on fields that are returned for withdrawn participants
    # include it; queries that ask for withdrawn participants get back participant 2 only.
    # Sort order does not affect whether withdrawn participants are included.
    with FakeClock(TIME_5):
      self.assertResponses('ParticipantSummary?_count=2&_sort=firstName',
                           [[ps_1, ps_3], [new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:asc=firstName',
                           [[ps_1, ps_3], [new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=firstName',
                           [[new_ps_2, ps_3], [ps_1]])
      self.assertResponses('ParticipantSummary?_count=2&_sort=dateOfBirth',
                           [[new_ps_2, ps_1], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=dateOfBirth',
                           [[ps_3, ps_1], [new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&_sort=genderIdentity',
                           [[ps_1, ps_3], [new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=genderIdentity',
                           [[new_ps_2, ps_1], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort=questionnaireOnTheBasics',
                           [[ps_1, new_ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort=hpoId',
                           [[ps_1, new_ps_2], [new_ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&_sort:desc=hpoId',
                           [[ps_1, new_ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&firstName=Mary',
                           [[new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&middleName=Q',
                           [[ps_1, new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&lastName=Smith',
                           [[ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&hpoId=PITT',
                           [[ps_1, new_ps_2], [ps_3]])
      self.assertResponses('ParticipantSummary?_count=2&withdrawalStatus=NO_USE',
                           [[new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&withdrawalTime=lt2016-01-03',
                           [[]])
      self.assertResponses('ParticipantSummary?_count=2&withdrawalTime=ge2016-01-03',
                           [[new_ps_2]])
      self.assertResponses('ParticipantSummary?_count=2&suspensionStatus=NOT_SUSPENDED',
                           [[ps_1]])




def _add_code_answer(code_answers, link_id, code):
  if code:
    code_answers.append((link_id, Concept(PPI_SYSTEM, code)))

def _make_entry(ps):
  return { 'fullUrl': 'http://localhost/rdr/v1/Participant/%s/Summary' % ps['participantId'],
           'resource': ps }
