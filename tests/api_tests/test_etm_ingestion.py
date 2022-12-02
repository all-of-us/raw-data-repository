import json

from rdr_service.model import etm
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path


class EtmIngestionTest(BaseTestCase):
    def test_questionnaire_ingestion(self):
        with open(data_path('etm_questionnaire.json')) as file:
            questionnaire_json = json.load(file)
        response = self.send_post('Questionnaire', questionnaire_json)

        questionnaire_obj: etm.EtmQuestionnaire = self.session.query(etm.EtmQuestionnaire).filter(
            etm.EtmQuestionnaire.etm_questionnaire_id == response['id']
        ).one()
        self.assertEqual(questionnaire_json['url'], questionnaire_obj.questionnaire_type)
        self.assertEqual(questionnaire_json['version'], questionnaire_obj.semantic_version)
        self.assertEqual(questionnaire_json['name'], questionnaire_obj.name)
        self.assertEqual(questionnaire_json['title'], questionnaire_obj.title)

    def test_questionnaire_response_ingestion(self):
        participant_id = self.data_generator.create_database_participant().participantId

        with open(data_path('etm_questionnaire_response.json')) as file:
            questionnaire_response_json = json.load(file)
        response = self.send_post(f'Participant/P{participant_id}/QuestionnaireResponse', questionnaire_response_json)

        print('bob')
