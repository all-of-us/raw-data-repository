from graphene.test import Client
from unittest import TestCase
import faker
from collections import defaultdict
from graphql import GraphQLSyntaxError

from rdr_service.api.nph_participant_api_schemas.schema import NPHParticipantSchema, SortableField
from rdr_service.api.nph_participant_api_schemas.schema import Participant as Part
from rdr_service.dao import database_factory
from rdr_service.model.participant import Participant
from rdr_service.model.nph_sample import NphSample
from sqlalchemy.orm import Query
from rdr_service.model.participant import Participant as DbParticipant
from rdr_service.api.nph_participant_api_schemas.util import SortContext

import rdr_service.api.nph_participant_api as api


def mock_load_participant_data(query):
    fake = faker.Faker()
    with database_factory.get_database().session() as session:
        query.session = session

        num = session.query(Participant).count()
        print(f'NPH TESTING: found {num} participants')

        if num < 10:
            print('NPH TESTING: generating test data')

            for _ in range(1000):
                participant = Participant(
                    biobankId=fake.random.randint(100000000, 999999999),
                    participantId=fake.random.randint(100000000, 999999999),
                    version=1,
                    lastModified=fake.date_time_this_decade(),
                    signUpTime=fake.date_time_this_decade(),
                    withdrawalStatus=1,
                    suspensionStatus=1,
                    participantOrigin='test',
                    hpoId=0
                )
                session.add(participant)

                session.add(
                    NphSample(
                        test='SA2',
                        status='received',
                        time=fake.date_time_this_decade(),
                        participant=participant,
                        children=[NphSample(
                            test='SA2',
                            status='disposed',
                            time=fake.date_time_this_decade()
                        )]
                    )
                )
                session.add(
                    NphSample(
                        test='RU3',
                        status='disposed',
                        time=fake.date_time_this_decade(),
                        participant=participant
                    )
                )
        results = []
        for participants in query.all():
            samples_data = defaultdict(lambda: {
                'stored': {
                    'parent': {
                        'current': None
                    },
                    'child': {
                        'current': None
                    }
                }
            })
            for parent_sample in participants.samples:
                data_struct = samples_data[f'sample{parent_sample.test}']['stored']
                data_struct['parent']['current'] = {
                    'value': parent_sample.status,
                    'time': parent_sample.time
                }

                if len(parent_sample.children) == 1:
                    child = parent_sample.children[0]
                    data_struct['child']['current'] = {
                        'value': child.status,
                        'time': child.time
                    }

            results.append(
                {
                    'participantNphId': participants.participantId,
                    'lastModified': participants.lastModified,
                    'biobankId': participants.biobankId,
                    **samples_data
                }
            )

        return results


query_with_id = ''' { participant (nphId: 153296765) {totalCount resultCount pageInfo
{ startCursor  endCursor hasNextPage }  edges { node { participantNphId sampleSA2 { stored { child { current
{ value time  } } } } } } } }'''

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

    def test_client_result_check_length(self):
        query = Query(DbParticipant)
        lengths = [99, 5, 10, 25]
        executed = mock_load_participant_data(query)
        self.assertEqual(1013, len(executed), "Should return 1013 records back")
        for length in lengths:
            query = query.limit(length)
            executed = mock_load_participant_data(query)
            self.assertEqual(length, len(executed), "Should return {} records back".format(length))

    def test_client_single_result(self):
        query = Query(DbParticipant)
        query = query.filter(DbParticipant.participantId == 153296765)
        executed = mock_load_participant_data(query)
        self.assertEqual(1, len(executed), "Should return 1 record back")
        self.assertEqual(153296765, executed[0].get('participantNphId'))

    def test_client_sorting_result(self):
        query = Query(DbParticipant)
        current_class = Part
        sort_context = SortContext(query)
        sort_parts = "sampleSA2:stored:child:current:time".split(":")
        for sort_field_name in sort_parts:
            sort_field: SortableField = getattr(current_class, sort_field_name)
            sort_field.sort(current_class, sort_field_name, sort_context)
            current_class = sort_field.type
        query = sort_context.get_resulting_query()
        executed = mock_load_participant_data(query)
        time_list = []
        for each in executed:
            time_list.append(each.get('sampleSA2').get('stored').get('child').get('current').get('time'))
        sorted_time_list = sorted(time_list)
        self.assertTrue(time_list == sorted_time_list, msg="Resultset is not in sorting order")

    def test_client_graphql_syntax_error(self):
        client = Client(NPHParticipantSchema)
        executed = client.execute(query_with_syntax_error)
        self.assertIn("Syntax Error", executed.get('errors')[0].get('message'))

    def test_client_graphql_field_error(self):
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

