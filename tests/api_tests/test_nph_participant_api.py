import faker
from itertools import zip_longest
from graphql import GraphQLSyntaxError
import json
from datetime import datetime

from rdr_service.dao import database_factory
from rdr_service.data_gen.generators.data_generator import DataGenerator
from sqlalchemy.orm import Query
from rdr_service.model import study_nph
from rdr_service.model.participant import Participant as aouParticipant
from rdr_service.model.participant_summary import ParticipantSummary as ParticipantSummaryModel
from rdr_service.model.rex import ParticipantMapping, Study
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.main import app
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.data_gen.generators.nph import NphDataGenerator
import rdr_service.api.nph_participant_api as api


QUERY_WITH_SYNTAX_ERROR = '''{ participant(nphId: 25){ totalCount resultCount pageInfo
{ startCursor endCursor hasNextPage }edges{ node {firstName lastName streetAddress
foodInsecurity{current{value time} historical{value time}}aouBasicsQuestionnaire{value time}
sampleSa1{ordered{parent{current{value time}}} }} } } '''

QUERY_WITH_FIELD_ERROR = '''{ participant(nphId: 25){ totalCount resultCount pageInfo
{ startCursor endCursor hasNextPage }edges{ node {firstName lastName streetAddres
foodInsecurity{current{value time} historical{value time}}aouBasicsQuestionnaire{value time}
sampleSa1{ordered{parent{current{value time}}} }} } } }'''

QUERY_WITH_MULTI_FIELD_ERROR = '''{ participant(nphId: 25){ totalCount resultCount pageInfo
{ startCursor endCursor hasNextPage }edges{ node {firstNam lastNam streetAddres
foodIsecurity{current{value time} historical{value time}} aouBasicsQuestionnaire{value time}
sampleSa1{ordered{parent{current{value time}}} }} } } }'''

QUERY_WITH_NONE_VALUE = '''
{ participant  { edges { node { aouLifestyleStatus{ value time } aouBasicStatus{ value time }
aouOverallHealthStatus{ value time } aouLifestyleStatus{ value time } aouSDOHStatus{ value time }}}}}
'''


def simple_query(value):
    return ''' { participant  {totalCount resultCount pageInfo
           { startCursor  endCursor hasNextPage }  edges { node { participantNphId %s } } } }''' % value


def condition_query(condition, sort_value, sort_field):
    return ''' { participant (%s: %s) {totalCount resultCount pageInfo
           { startCursor  endCursor hasNextPage }  edges { node { %s } } } }''' % (condition, sort_value, sort_field)


def mock_load_participant_data(session):
    fake = faker.Faker()
    ps_query = session.query(ParticipantSummaryModel)
    ps_query.session = session
    ps_result = ps_query.all()
    for each in ps_result:
        each.questionnaireOnTheBasics = QuestionnaireStatus.UNSET
        each.questionnaireOnHealthcareAccess = QuestionnaireStatus.UNSET
        each.questionnaireOnLifestyle = QuestionnaireStatus.UNSET
        each.questionnaireOnSocialDeterminantsOfHealth = QuestionnaireStatus.UNSET
        session.add(each)
    session.commit()
    num = len(ps_result)
    print(f'NPH TESTING: found {num} participants')
    if num < 10:
        print('NPH TESTING: generating test data')
        aou_generator = DataGenerator(session, fake)
        aou_generator.create_database_hpo()
        aou_generator.create_database_site()
        aou_generator.create_database_code()
        for _ in enumerate(range(11)):
            aou_generator.create_database_participant(hpoId=0)
        participant_query = Query(aouParticipant)
        participant_query.session = session
        participant_result = participant_query.all()
        for each in participant_result:
            aou_generator.create_database_participant_summary(hpoId=0, participant=each, siteId=1,
                                                              dateOfBirth=fake.date_of_birth(),
                                                              deceasedAuthored=fake.date_time())
        rdr_study_record = Study(ignore_flag=0, schema_name="rdr")
        nph_study_record = Study(ignore_flag=0, schema_name='nph', prefix=1000)
        for each in [rdr_study_record, nph_study_record]:
            session.add(each)
        participant_mapping_query = Query(ParticipantMapping)
        participant_mapping_query.session = session
        participant_mapping_result = participant_mapping_query.all()
        if len(participant_mapping_result) < 10:
            ancillary_participant_id = 100000000
            for each in participant_result:
                pm = ParticipantMapping(primary_participant_id=each.participantId,
                                        ancillary_participant_id=ancillary_participant_id,
                                        ancillary_study_id=2
                                        )
                session.add(pm)
                ancillary_participant_id = ancillary_participant_id + 1
            session.commit()
    nph_data_gen = NphDataGenerator()
    for activity_name in ['ENROLLMENT', 'PAIRING', 'CONSENT']:
        nph_data_gen.create_database_activity(
            name=activity_name
        )

    nph_data_gen.create_database_pairing_event_type(name="INITIAL")

    for i in range(1, 3):
        nph_data_gen.create_database_site(
            external_id=f"nph-test-site-{i}",
            name=f"nph-test-site-name-{i}",
            awardee_external_id="nph-test-hpo",
            organization_external_id="nph-test-org"
        )
    for _ in range(2):
        participant = nph_data_gen.create_database_participant()
        nph_data_gen.create_database_pairing_event(
            participant_id=participant.id,
            event_authored_time=datetime(2023, 1, 1, 12, 0),
            site_id=1
        )

    nph_data_gen.create_database_pairing_event(
        participant_id=100000000,
        event_authored_time=datetime(2023, 1, 1, 12, 1),
        site_id=2
    )


class TestQueryExecution(BaseTestCase):

    def test_client_result_participant_summary(self):
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            query = Query(ParticipantSummaryModel)
            query.session = session
            result = query.all()
            self.assertEqual(11, len(result))

    def test_client_result_check_length(self):
        query_return_one = condition_query("limit", "1", "DOB")
        query_return_two = simple_query("DOB")
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            lengths = [1, 2]
            queries = [query_return_one, query_return_two]
            for (length, query) in zip_longest(lengths, queries):
                executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
                result = json.loads(executed.data.decode('utf-8'))
                self.assertEqual(length, len(result.get('participant').get('edges')),
                                 "Should return {} records back".format(length))

    def test_client_single_result(self):
        fetch_value = '"{}"'.format("1000100000001")
        query = condition_query("nphId", fetch_value, "participantNphId")
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            self.assertEqual(1, len(result.get('participant').get('edges')), "Should return 1 record back")
            self.assertEqual(100000001, result.get('participant').get('edges')[0].get('node').get('participantNphId'))

    def test_client_none_value_field(self):
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=QUERY_WITH_NONE_VALUE)
            result = json.loads(executed.data.decode('utf-8'))
            self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 record back")
            for each in result.get('participant').get('edges'):
                for _, v in each.get('node').items():
                    self.assertEqual(str(QuestionnaireStatus.UNSET), v.get('value'))
                    self.assertIsNone(v.get('time'))

    def test_client_nph_pair_site(self):
        field_to_test = "nphPairedSite"
        query = simple_query(field_to_test)
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
            expected_site_name = ["nph-test-site-1", "nph-test-site-2"]
            for index, each in enumerate(result.get('participant').get('edges')):
                self.assertEqual(expected_site_name[index], each.get('node').get(field_to_test))

    def test_client_nph_awardee_external_id(self):
        field_to_test = "nphPairedAwardee"
        query = simple_query(field_to_test)
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
            for each in result.get('participant').get('edges'):
                self.assertEqual('nph-test-hpo', each.get('node').get('nphPairedAwardee'))

    def test_client_nph_organization_external_id(self):
        field_to_test = "nphPairedOrg"
        query = simple_query(field_to_test)
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
            for each in result.get('participant').get('edges'):
                self.assertEqual('nph-test-org', each.get('node').get(field_to_test))

    def test_client_nph_pair_site_with_id(self):
        fetch_value = '"{}"'.format("1000100000001")
        query = condition_query("nphId", fetch_value, "nphPairedSite")
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            self.assertEqual(1, len(result.get('participant').get('edges')), "Should return 1 record back")
            self.assertEqual("nph-test-site-1", result.get('participant').get('edges')[0].get('node'
                                                                                              ).get('nphPairedSite'))

    def test_client_sorting_date_of_birth(self):
        sort_field = '"{}"'.format("DOB")
        query = condition_query("sortBy", sort_field, "DOB")
        dob_list = []
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8')).get('participant').get('edges')
            for each in result:
                dob_list.append(each.get('node').get('DOB'))
            sorted_list = dob_list.copy()
            sorted_list.sort()
            self.assertTrue(dob_list == sorted_list, msg="Resultset is not in sorting order")

    def test_client_sorting_deceased_status(self):
        sort_field = '"{}"'.format("aouDeceasedStatus:time")
        query = condition_query("sortBy", sort_field, "aouDeceasedStatus {value time}")
        deceased_list = []
        with database_factory.get_database().session() as session:
            mock_load_participant_data(session)
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            for each in result.get('participant').get('edges'):
                datetime_object = datetime.strptime(each.get('node').get('aouDeceasedStatus').get('time'),
                                                    '%Y-%m-%dT%H:%M:%S')
                deceased_list.append(datetime_object)
            sorted_list = deceased_list.copy()
            sorted_list.sort()
            self.assertTrue(deceased_list == sorted_list, msg="Resultset is not in sorting order")

    def test_client_filter_parameter(self):
        mock_load_participant_data(self.session)
        participant_nph_id, first_name = (
            self.session.query(study_nph.Participant.id, ParticipantSummaryModel.firstName)
            .join(
                ParticipantMapping,
                ParticipantMapping.primary_participant_id == ParticipantSummaryModel.participantId
            ).join(
                study_nph.Participant,
                study_nph.Participant.id == ParticipantMapping.ancillary_participant_id
            ).first()
        )

        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (firstName: "%s") { edges { node { participantNphId firstName } } } }' % first_name
        )
        result = json.loads(executed.data.decode('utf-8'))

        result_participant_list = result.get('participant').get('edges')
        self.assertEqual(1, len(result_participant_list))

        resulting_participant_data = result_participant_list[0].get('node')
        self.assertEqual(first_name, resulting_participant_data.get('firstName'))
        self.assertEqual(participant_nph_id, resulting_participant_data.get('participantNphId'))

    def test_graphql_syntax_error(self):
        executed = app.test_client().post('/rdr/v1/nph_participant', data=QUERY_WITH_SYNTAX_ERROR)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertIn("Syntax Error", result.get('errors').get('message'))

    def test_graphql_field_error(self):
        queries = [QUERY_WITH_FIELD_ERROR, QUERY_WITH_MULTI_FIELD_ERROR]
        for query in queries:
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            for error in result.get('errors'):
                self.assertIn('message', error)
                self.assertIn('locations', error)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("rex.participant_mapping")
        self.clear_table_after_test("rex.study")
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.pairing_event_type")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.pairing_event")


class TestQueryValidator(BaseTestCase):

    def test_validation_error(self):
        self.assertRaises(GraphQLSyntaxError, api.validate_query, QUERY_WITH_SYNTAX_ERROR)

    def test_validation_no_error(self):
        query = condition_query("nphId", "100000001", "participantNphId")
        result = api.validate_query(query)
        self.assertEqual([], result)
