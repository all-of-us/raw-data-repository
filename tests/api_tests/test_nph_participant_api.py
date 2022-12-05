from graphene.test import Client
from unittest import TestCase
from unittest.mock import patch
import names
import faker
from datetime import datetime
from itertools import zip_longest
from graphql import GraphQLSyntaxError

from rdr_service.api.nph_participant_api_schemas.schema import NPHParticipantSchema
import rdr_service.api.nph_participant_api as api

fake = faker.Faker()

MOCK_DATAS = []


def get_gender():
    profile = fake.simple_profile()
    return profile.get('sex')


def get_state():
    states = fake.military_state()
    return states


for num in range(1, 100):
    data = {'participant_nph_id': num}
    gender = get_gender()
    first_name = names.get_first_name(gender)
    last_name = names.get_last_name()
    data['first_name'] = first_name
    data['last_name'] = last_name
    data['state'] = get_state()
    data['city'] = first_name
    data['street_address'] = fake.address().replace("\n", " ")
    data['gender'] = gender
    data['food_insecurity'] = {"current": {"value": "whatever", "time": datetime.utcnow()}, "historical": [
        {"value": "historical_value", "time": datetime.utcnow()}, {"value": names.get_first_name(gender),
                                                                   "time": datetime.utcnow()},
        {"value": names.get_first_name(gender), "time": datetime.utcnow()}]}
    data['aou_basics_questionnaire'] = {'value': "HAHA", 'time': datetime.utcnow()}
    data['sample_sa_1'] = {"ordered": [{"parent": [{"current": {"value": "parent_current_value",
                                                                "time": datetime.utcnow()},
                                                    "historical": [{"value": "parent_historical_value",
                                                                    "time": datetime.utcnow()}]}],
                                       "child": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                  "historical": [{"value": "child_historical_value",
                                                                  "time": datetime.utcnow()}]}]}],
                           "stored": [{"parent": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                   "historical": [{"value": gender, "time": datetime.utcnow()}]}],
                                       "child": [{"current": {"value": gender, "time": datetime.utcnow()},
                                                  "historical": [{"value": gender, "time": datetime.utcnow()}]}]}]
                           }

    MOCK_DATAS.append(data)


query_5 = '''{ participant(first: 5){ totalCount resultCount pageInfo { startCursor endCursor hasNextPage }edges
{ node {firstName lastName streetAddress foodInsecurity{current{value time} historical{value time}}
aouBasicsQuestionnaire{value time} sampleSa1{ordered{parent{current{value time}}} }} } } }'''

query_10 = '''{ participant(first: 10){ totalCount resultCount pageInfo { startCursor endCursor hasNextPage }edges
{ node {firstName lastName streetAddress foodInsecurity{current{value time} historical{value time}}
aouBasicsQuestionnaire{value time} sampleSa1{ordered{parent{current{value time}}} }} } } }'''

query_25 = '''{ participant(first: 25){ totalCount resultCount pageInfo { startCursor endCursor hasNextPage }edges
{ node {firstName lastName streetAddress foodInsecurity{current{value time} historical{value time}}
aouBasicsQuestionnaire{value time} sampleSa1{ordered{parent{current{value time}}} }} } } }'''

query_99 = '''{ participant{ totalCount resultCount pageInfo { startCursor endCursor hasNextPage }edges
{ node {firstName lastName streetAddress foodInsecurity{current{value time} historical{value time}}
aouBasicsQuestionnaire{value time} sampleSa1{ordered{parent{current{value time}}} }} } } }'''

query_with_id = '''{ participant(nphId: 25){ totalCount resultCount pageInfo { startCursor endCursor hasNextPage }edges
{ node {participantNphId firstName lastName streetAddress foodInsecurity{current{value time} historical{value time}}
aouBasicsQuestionnaire{value time} sampleSa1{ordered{parent{current{value time}}} }} } } }'''

query_with_syntax_error = '''{ participant(nphId: 25){ totalCount resultCount pageInfo
{ startCursor endCursor hasNextPage }edges{ node {firstName lastName streetAddress
foodInsecurity{current{value time} historical{value time}}aouBasicsQuestionnaire{value time}
sampleSa1{ordered{parent{current{value time}}} }} } } '''

query_with_field_error = '''{ participant(nphId: 25){ totalCount resultCount pageInfo
{ startCursor endCursor hasNextPage }edges{ node {firstName lastName streetAddres
foodInsecurity{current{value time} historical{value time}}aouBasicsQuestionnaire{value time}
sampleSa1{ordered{parent{current{value time}}} }} } } }'''

query_with_multiple_fields_error = '''{ participant(nphId: 25){ totalCount resultCount pageInfo
{ startCursor endCursor hasNextPage }edges{ node {firstNam lastNam streetAddres
foodIsecurity{current{value time} historical{value time}} aouBasicsQuestionnaire{value time}
sampleSa1{ordered{parent{current{value time}}} }} } } }'''


class TestQueryExecution(TestCase):

    @patch('rdr_service.api.nph_participant_api_schemas.schema.db')
    def test_client_good_result_check_length(self, mock_datas):
        mock_datas.datas = MOCK_DATAS
        client = Client(NPHParticipantSchema)
        queries = [query_99, query_5, query_10, query_25, query_with_id]
        lengths = [99, 5, 10, 25, 1]
        for (query, length) in zip_longest(queries, lengths):
            executed = client.execute(query)
            self.assertEqual(length, len(executed.get('data').get('participant').get('edges')),
                             "{} - is not returning same amount of resultset".format(query))

    @patch('rdr_service.api.nph_participant_api_schemas.schema.db')
    def test_client_good_result_single_result(self, mock_datas):
        mock_datas.datas = MOCK_DATAS
        client = Client(NPHParticipantSchema)
        executed = client.execute(query_with_id)
        self.assertEqual(1, len(executed.get('data').get('participant').get('edges')),
                         "{} - is not returning same amount of resultset".format(query_with_id))
        self.assertEqual(25, executed.get('data').get('participant').get('edges')[0].get('node')
                         .get('participantNphId'),
                         "{} - is not returning same amount of resultset".format(query_with_id))

    @patch('rdr_service.api.nph_participant_api_schemas.schema.db')
    def test_client_graphql_syntax_error(self, mock_datas):
        mock_datas.datas = MOCK_DATAS
        client = Client(NPHParticipantSchema)
        executed = client.execute(query_with_syntax_error)
        self.assertIn("Syntax Error", executed.get('errors')[0].get('message'))

    @patch('rdr_service.api.nph_participant_api_schemas.schema.db')
    def test_client_graphql_field_error(self, mock_datas):
        mock_datas.datas = MOCK_DATAS
        client = Client(NPHParticipantSchema)
        queries = [query_with_field_error, query_with_multiple_fields_error]
        for query in queries:
            executed = client.execute(query)
            for error in executed.get('errors'):
                self.assertIn('message', error)
                self.assertIn('locations', error)
                self.assertIn('path', error)


class TestQueryValidator(TestCase):

    def test_validation_error(self):
        self.assertRaises(GraphQLSyntaxError, api.validate_query, query_with_syntax_error)

    def test_validation_no_error(self):
        result = api.validate_query(query_with_id)
        self.assertEqual([], result)

