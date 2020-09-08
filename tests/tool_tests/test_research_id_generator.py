import mock

from rdr_service.model.participant import Participant
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.tools.tool_libs.research_id_generator import ResearchIdGeneratorClass
from tests.helpers.unittest_base import BaseTestCase
from sqlalchemy.exc import IntegrityError


class FinalizeOrdersTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = ParticipantDao()

    @staticmethod
    def run_tool(input_data, type):
        environment = mock.MagicMock()
        environment.project = 'unit_test'

        args = mock.MagicMock()
        args.type = type

        # Patching to bypass opening file and provide input data
        with mock.patch('rdr_service.tools.tool_libs.research_id_generator.open'),\
                mock.patch('rdr_service.tools.tool_libs.research_id_generator.csv') as mock_csv:
            mock_csv.DictReader.return_value = input_data

            finalize_orders_tool = ResearchIdGeneratorClass(args, environment)
            finalize_orders_tool.run()

    def test_generate_research_id_from_csv_file(self):
        p1 = Participant(participantId=1, biobankId=4)
        self.dao.insert(p1)
        p2 = Participant(participantId=11, researchId=33, biobankId=44)
        self.dao.insert(p2)
        input_data = [{
            'participant_id': 1,
            'research_id': 2
        }, {
            'participant_id': 11,
            'research_id': 22
        }]
        self.run_tool(input_data, 'import')
        result = self.dao.get(1)
        self.assertEqual(result.researchId, 2)
        # no change for existing record
        result = self.dao.get(11)
        self.assertEqual(result.researchId, 33)

    def test_generate_research_id_from_csv_file_with_duplication_research_id(self):
        p1 = Participant(participantId=1, biobankId=4)
        self.dao.insert(p1)
        p2 = Participant(participantId=11, biobankId=44)
        self.dao.insert(p2)
        input_data = [{
            'participant_id': 1,
            'research_id': 2
        }, {
            'participant_id': 11,
            'research_id': 2
        }]
        with self.assertRaises(IntegrityError):
            self.run_tool(input_data, 'import')

    def test_generate_research_id_for_exist_participant(self):
        p1 = Participant(participantId=1, biobankId=4)
        self.dao.insert(p1)
        p2 = Participant(participantId=11, researchId=22, biobankId=44)
        self.dao.insert(p2)

        self.run_tool(None, 'new')
        result = self.dao.get(1)
        self.assertEqual(len(str(result.researchId)), 7)
        # no change for existing record
        result = self.dao.get(11)
        self.assertEqual(result.researchId, 22)
