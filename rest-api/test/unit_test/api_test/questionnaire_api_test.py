import httplib
import json

from code_constants import PPI_EXTRA_SYSTEM
from dao.code_dao import CodeDao
from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import data_path

class QuestionnaireApiTest(FlaskTestBase):

  def test_insert(self):
    questionnaire_files = (
        'questionnaire1.json',
        'questionnaire2.json',
        'questionnaire_demographics.json',
    )

    for json_file in questionnaire_files:
      with open(data_path(json_file)) as f:
        questionnaire = json.load(f)
      response = self.send_post('Questionnaire', questionnaire)
      questionnaire_id = response['id']
      del response['id']
      questionnaire['version'] = '1'
      self.assertJsonResponseMatches(questionnaire, response)

      response = self.send_get('Questionnaire/%s' % questionnaire_id)
      del response['id']
      self.assertJsonResponseMatches(questionnaire, response)
  
    # Ensure we didn't create codes in the extra system
    self.assertIsNone(CodeDao().get_code(PPI_EXTRA_SYSTEM, 'IgnoreThis'))
    
  def insert_questionnaire(self):
    with open(data_path('questionnaire1.json')) as f:
      questionnaire = json.load(f)
      return self.send_post('Questionnaire', questionnaire,
                            expected_response_headers = { 'ETag': 'W/"1"'})

  def test_update_before_insert(self):
    with open(data_path('questionnaire1.json')) as f:
      questionnaire = json.load(f)
      self.send_put('Questionnaire/1', questionnaire, expected_status=httplib.BAD_REQUEST)

  def test_update_no_ifmatch_specified(self):
    response = self.insert_questionnaire()

    with open(data_path('questionnaire2.json')) as f2:
      questionnaire2 = json.load(f2)
      self.send_put('Questionnaire/%s' % response['id'], questionnaire2,
                    expected_status=httplib.BAD_REQUEST)

  def test_update_invalid_ifmatch_specified(self):
    response = self.insert_questionnaire()

    with open(data_path('questionnaire2.json')) as f2:
      questionnaire2 = json.load(f2)
      self.send_put('Questionnaire/%s' % response['id'], questionnaire2,
                    expected_status=httplib.BAD_REQUEST,
                    headers={ 'If-Match': 'Blah' })

  def test_update_wrong_ifmatch_specified(self):
    response = self.insert_questionnaire()

    with open(data_path('questionnaire2.json')) as f2:
      questionnaire2 = json.load(f2)
      self.send_put('Questionnaire/%s' % response['id'], questionnaire2,
                    expected_status=httplib.PRECONDITION_FAILED,
                    headers={ 'If-Match': 'W/"123"' })

  def test_update_right_ifmatch_specified(self):
    response = self.insert_questionnaire()
    self.assertEquals('W/"1"', response['meta']['versionId'])
    with open(data_path('questionnaire2.json')) as f2:
      questionnaire2 = json.load(f2)
      update_response = self.send_put('Questionnaire/%s' % response['id'], questionnaire2,
                                      headers={ 'If-Match': response['meta']['versionId'] },
                                      expected_response_headers = { 'ETag': 'W/"2"'})
    questionnaire2['id'] = response['id']
    questionnaire2['version'] = '2'
    self.assertJsonResponseMatches(questionnaire2, update_response)
    self.assertEquals('W/"2"', update_response['meta']['versionId'])