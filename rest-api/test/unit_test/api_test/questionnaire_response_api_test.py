import datetime
import httplib
import json

from dao.code_dao import CodeDao
from dao.questionnaire_dao import QuestionnaireDao
from dao.questionnaire_response_dao import QuestionnaireResponseAnswerDao
from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import data_path

def _questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id

class QuestionnaireResponseApiTest(FlaskTestBase):

  def test_insert(self):
    participant_id = self.create_participant()
    questionnaire_id = self.create_questionnaire('questionnaire1.json')
    with open(data_path('questionnaire_response3.json')) as f:
      resource = json.load(f)
    # Sending response with the dummy participant id in the file is an error
    self.send_post(_questionnaire_response_url('{participant_id}'), resource,
                   expected_status=httplib.NOT_FOUND)
    # Fixing participant id but not the questionnaire id is also an error
    resource['subject']['reference'] = \
        resource['subject']['reference'].format(participant_id=participant_id)
    self.send_post(_questionnaire_response_url(participant_id), resource,
                   expected_status=httplib.BAD_REQUEST)
    # Also fixing questionnaire id succeeds
    resource['questionnaire']['reference'] = \
        resource['questionnaire']['reference'].format(questionnaire_id=questionnaire_id)
    response = self.send_post(_questionnaire_response_url(participant_id), resource)
    resource['id'] = response['id']
    # The resource gets rewritten to include the version
    resource['questionnaire']['reference'] = 'Questionnaire/%s/_history/1' % questionnaire_id
    self.assertJsonResponseMatches(resource, response)
    
    code_dao = CodeDao()
    name_of_child = code_dao.get_code("sys", "nameOfChild")
    birth_weight = code_dao.get_code("sys", "birthWeight")
    birth_length = code_dao.get_code("sys", "birthLength")    
    vitamin_k_dose_1 = code_dao.get_code("sys", "vitaminKDose1")
    vitamin_k_dose_2 = code_dao.get_code("sys", "vitaminKDose2")
    hep_b_given = code_dao.get_code("sys", "hepBgiven")
    answer_dao = QuestionnaireResponseAnswerDao()
    with answer_dao.session() as session:
      code_ids = [code.codeId for code in 
                  [name_of_child, birth_weight, birth_length, vitamin_k_dose_1, vitamin_k_dose_2,
                  hep_b_given]]
      current_answers = answer_dao.get_current_answers_for_concepts(session,
                                                                    participant_id[1:],
                                                                    code_ids)
    self.assertEquals(6, len(current_answers))
    questionnaire = QuestionnaireDao().get_with_children(questionnaire_id)
    question_id_to_answer = {answer.questionId : answer for answer in current_answers}
    code_id_to_answer = {question.codeId: 
                         question_id_to_answer.get(question.questionnaireQuestionId)
                         for question in questionnaire.questions}
    self.assertEquals("Cathy Jones", code_id_to_answer[name_of_child.codeId].valueString)
    self.assertEquals(3.25, code_id_to_answer[birth_weight.codeId].valueDecimal)
    self.assertEquals(44.3, code_id_to_answer[birth_length.codeId].valueDecimal)
    self.assertEquals(44, code_id_to_answer[birth_length.codeId].valueInteger)
    self.assertEquals(True, code_id_to_answer[hep_b_given.codeId].valueBoolean)
    self.assertEquals(datetime.date(1972, 11, 30), 
                      code_id_to_answer[vitamin_k_dose_1.codeId].valueDate)
    self.assertEquals(datetime.datetime(1972, 11, 30, 12, 34, 42), 
                      code_id_to_answer[vitamin_k_dose_2.codeId].valueDateTime)
  '''
  TODO(DA-224): uncomment this once participant summary API is working again
  def test_demographic_questionnaire_responses(self):
    participant_id = self.create_participant()
    questionnaire_id = self.create_questionnaire('questionnaire_demographics.json')
    with open(data_path('questionnaire_response_demographics.json')) as f:
      resource = json.load(f)
    resource['subject']['reference'] = \
      resource['subject']['reference'].format(participant_id=participant_id)
    resource['questionnaire']['reference'] = \
      resource['questionnaire']['reference'].format(questionnaire_id=questionnaire_id)
    response = self.send_post(_questionnaire_response_url(participant_id), resource)

    participant = self.send_get('Participant/%s' % participant_id)
    summary = self.send_get('Participant/%s/Summary' % participant_id)
    expected = {'ageRange': 'UNSET',
                'genderIdentity': 'MALE_TO_FEMALE_TRANSGENDER',
                'ethnicity': 'UNSET',
                'race': 'UNSET',
                'hpoId': 'UNSET',
                'firstName': None,
                'lastName': None,
                'middleName': None,
                'membershipTier': 'UNSET',
                'numBaselineSamplesArrived': 0,
                'numCompletedBaselinePPIModules': 1,
                'biobankId': participant['biobankId'],
                'participantId': participant_id,
                'physicalMeasurementsStatus': 'UNSET',
                'zipCode': None,
                'consentForElectronicHealthRecords': 'UNSET',
                'consentForStudyEnrollment': 'UNSET',
                'questionnaireOnFamilyHealth': 'UNSET',
                'questionnaireOnHealthcareAccess': 'UNSET',
                'questionnaireOnMedicalHistory' : 'UNSET',
                'questionnaireOnMedications': 'UNSET',
                'questionnaireOnOverallHealth': 'SUBMITTED',
                'questionnaireOnPersonalHabits': 'UNSET',
                'questionnaireOnSociodemographics': 'UNSET',
                'signUpTime': participant['signUpTime'],
              }
    self.assertJsonResponseMatches(expected, summary)
   '''