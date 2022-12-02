from graphene.test import Client
from unittest import TestCase
from unittest.mock import patch
import names
import faker
from datetime import datetime
from itertools import zip_longest

from rdr_service.api.nph_participant_api_schemas.schema import NPHParticipantSchema

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
{ node {firstName lastName streetAddress foodInsecurity{current{value time} historical{value time}}
aouBasicsQuestionnaire{value time} sampleSa1{ordered{parent{current{value time}}} }} } } }'''


class TestQueryExecution(TestCase):

    @patch('rdr_service.api.nph_participant_api_schemas.schema.db')
    def test_client(self, mock_datas):
        mock_datas.datas = MOCK_DATAS
        client = Client(NPHParticipantSchema)
        queries = [query_99, query_5, query_10, query_25, query_with_id]
        lengths = [99, 5, 10, 25, 1]
        for (query, length) in zip_longest(queries, lengths):
            executed = client.execute(query)
            self.assertEqual(length, len(executed.get('data').get('participant').get('edges')),
                             "{} - is not returning same amount of resultset".format(query))

