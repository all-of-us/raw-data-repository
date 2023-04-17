import faker
from typing import Iterable, Dict, List
from collections import defaultdict
from itertools import zip_longest
from graphql import GraphQLSyntaxError
import json
from datetime import datetime, timedelta

from rdr_service.ancillary_study_resources.nph.enums import ParticipantOpsElementTypes
from rdr_service.config import NPH_PROD_BIOBANK_PREFIX, NPH_TEST_BIOBANK_PREFIX
from rdr_service.data_gen.generators.data_generator import DataGenerator
from sqlalchemy.orm import Query
from rdr_service.model import study_nph
from rdr_service.model.participant import Participant as aouParticipant
from rdr_service.model.participant_summary import ParticipantSummary as ParticipantSummaryModel
from rdr_service.model.rex import ParticipantMapping, Study
from rdr_service.model.study_nph import (
    PairingEvent, ConsentEventType, ConsentEvent, Participant as NphParticipant, Site as NphSite, OrderedSample, Order
)
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.main import app
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.data_gen.generators.nph import NphDataGenerator
import rdr_service.api.nph_participant_api as api
from rdr_service import config
from rdr_service.data_gen.generators.study_nph import (
    generate_fake_study_categories,
    generate_fake_orders,
    generate_fake_ordered_samples,
    generate_fake_sample_updates,
    generate_fake_stored_samples,
)

NPH_BIOBANK_PREFIX = NPH_PROD_BIOBANK_PREFIX if config.GAE_PROJECT == "all-of-us-rdr-prod" else NPH_TEST_BIOBANK_PREFIX

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
{ participant  { edges { node { aouLifestyleStatus{ value time } aouBasicsStatus{ value time }
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
    aou_generator = DataGenerator(session, fake)
    aou_generator.create_database_hpo()
    aou_generator.create_database_site()
    aou_generator.create_database_code()
    for _ in enumerate(range(11)):
        aou_generator.create_database_participant(hpoId=0)
    participant_query = Query(aouParticipant)
    participant_query.session = session
    participant_result = participant_query.all()
    for aou_participant in participant_result:
        aou_generator.create_database_participant_summary(hpoId=0, participant=aou_participant, siteId=1,
                                                          dateOfBirth=fake.date_of_birth(),
                                                          deceasedAuthored=fake.date_time())
    rdr_study_record = Study(ignore_flag=0, schema_name="rdr")
    nph_study_record = Study(ignore_flag=0, schema_name='nph', prefix=1000)
    for study in [
        rdr_study_record,
        nph_study_record
    ]:
        session.add(study)

    nph_data_gen = NphDataGenerator()
    for activity_name in ['ENROLLMENT', 'PAIRING', 'CONSENT', 'WITHDRAWAL', 'DEACTIVATION']:
        nph_data_gen.create_database_activity(
            name=activity_name
        )

    nph_data_gen.create_database_pairing_event_type(name="INITIAL")
    status = ['referred', 'consented']

    for name in status:
        nph_data_gen.create_database_enrollment_event_type(name=name, source_name=f'module1_{name}')
    participant_mapping_query = Query(ParticipantMapping)
    participant_mapping_query.session = session
    participant_mapping_result = participant_mapping_query.all()
    if len(participant_mapping_result) < 10:
        ancillary_participant_id = 100000000
        participants = []
        for each in participant_result:
            participant = nph_data_gen.create_database_participant(id=ancillary_participant_id)
            participants.append(participant)
            pm = ParticipantMapping(
                primary_participant_id=each.participantId,
                ancillary_participant_id=ancillary_participant_id,
                ancillary_study_id=2
            )
            session.add(pm)
            nph_data_gen.create_database_enrollment_event(ancillary_participant_id)
            ancillary_participant_id = ancillary_participant_id + 1

    session.commit()

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

        for counter, _ in enumerate(status):
            nph_data_gen.create_database_enrollment_event(
                participant_id=participant.id,
                event_authored_time=datetime(2023, 1, 1, 12, 0) - timedelta(days=counter + 1),
                event_id=1,
                event_type_id=counter + 1
            )

    consent_event_types = [
        ("Module 1 GPS Consent", "m1_consent_gps"),
        ("Module 1 Consent Recontact", "m1_consent_recontact"),
        ("Module 1 Consent Tissue", "m1_consent_tissue"),
    ]
    consent_event_type_objs: Iterable[ConsentEventType] = []
    for name, source_name in consent_event_types:
        consent_event_type = nph_data_gen.create_database_consent_event_type(
            name=name, source_name=source_name
        )
        consent_event_type_objs.append(consent_event_type)

    consent_events: Iterable[ConsentEvent] = []
    for participant in participants:
        for consent_event_type in consent_event_type_objs:
            consent_events.append(
                nph_data_gen.create_database_consent_event(
                    participant_id=participant.id,
                    event_type_id=consent_event_type.id
                )
            )

    nph_data_gen.create_database_pairing_event(
        participant_id=100000000,
        event_authored_time=datetime(2023, 1, 1, 12, 1),
        site_id=1
    )

    nph_data_gen.create_database_pairing_event(
        participant_id=100000001,
        event_authored_time=datetime(2023, 1, 1, 12, 1),
        site_id=2
    )

    nph_data_gen.create_database_participant_ops_data_element(
        participant_id=100000000,
        source_data_element=ParticipantOpsElementTypes.BIRTHDATE,
        source_value='1980-01-01'
    )


class TestQueryExecution(BaseTestCase):

    def test_client_result_participant_summary(self):
        mock_load_participant_data(self.session)
        query = Query(ParticipantSummaryModel)
        query.session = self.session
        result = query.all()
        self.assertEqual(11, len(result))

    def test_client_result_check_length(self):
        query_return_one = condition_query("limit", "1", "DOB")
        query_return_two = simple_query("DOB")
        mock_load_participant_data(self.session)
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
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(1, len(result.get('participant').get('edges')), "Should return 1 record back")
        self.assertEqual("1000100000001",
                         result.get('participant').get('edges')[0].get('node').get('participantNphId'))

    def test_client_none_value_field(self):
        mock_load_participant_data(self.session)
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
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
        expected_site_name = ["nph-test-site-1", "nph-test-site-2"]
        for index, each in enumerate(result.get('participant').get('edges')):
            self.assertEqual(expected_site_name[index], each.get('node').get(field_to_test))

    def test_client_nph_awardee_external_id(self):
        field_to_test = "nphPairedAwardee"
        query = simple_query(field_to_test)
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
        for each in result.get('participant').get('edges'):
            self.assertEqual('nph-test-hpo', each.get('node').get('nphPairedAwardee'))

    def test_client_nph_organization_external_id(self):
        field_to_test = "nphPairedOrg"
        query = simple_query(field_to_test)
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
        for each in result.get('participant').get('edges'):
            self.assertEqual('nph-test-org', each.get('node').get(field_to_test))

    def test_client_biobank_id_prefix(self):
        mock_load_participant_data(self.session)
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data=simple_query('biobankId')
        )
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")
        self.assertListEqual(
            ['T1100000000', 'T1100000001'],
            [
                participant_data['node']['biobankId']
                for participant_data in result.get('participant').get('edges')
            ]
        )

    def test_client_nph_pair_site_with_id(self):
        fetch_value = '"{}"'.format("1000100000000")
        query = condition_query("nphId", fetch_value, "nphPairedSite")
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(1, len(result.get('participant').get('edges')), "Should return 1 record back")
        self.assertEqual("nph-test-site-1", result.get('participant').get('edges')[0].get('node'
                                                                                          ).get('nphPairedSite'))

    def test_client_sorting_date_of_birth(self):
        sort_field = '"{}"'.format("DOB")
        query = condition_query("sortBy", sort_field, "DOB")
        dob_list = []
        mock_load_participant_data(self.session)
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
        mock_load_participant_data(self.session)
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
            ).join(
                PairingEvent,
                PairingEvent.participant_id == ParticipantMapping.ancillary_participant_id
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

    def test_nphEnrollmentStatus_fields(self):
        field_to_test = "nphEnrollmentStatus {value time} "
        query = simple_query(field_to_test)

        mock_load_participant_data(self.session)
        nph_datagen = NphDataGenerator()
        for nph_id in [100000000, 100000001]:
            nph_datagen.create_database_enrollment_event(participant_id=nph_id,
                                                         event_type_id=2,
                                                         event_authored_time=datetime.now())

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        enrollment_statuses = result.get('participant').get('edges')[0].get('node').get('nphEnrollmentStatus')

        for status in enrollment_statuses:
            self.assertIn("time", status)
            self.assertIn("value", status)
            if status['time']:
                self.assertEqual(status['value'], 'module1_consented')

    def test_nphModule1ConsentStatus_fields(self):
        field_to_test = "nphModule1ConsentStatus {value time optIn} "
        query = simple_query(field_to_test)

        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))
        consent_events = result.get('participant').get('edges')[0].get('node').get('nphModule1ConsentStatus')
        for status in consent_events:
            self.assertIn("time", status)
            self.assertIn(
                status["value"],
                ["m1_consent_gps", "m1_consent_recontact", "m1_consent_tissue"]
            )
            self.assertIn(status["optIn"], ["PERMIT"])

    def test_nphWithdrawalStatus_fields(self):
        field_to_test = "nphWithdrawalStatus {value time} "
        query = simple_query(field_to_test)
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))
        actual_result = result.get('participant').get('edges')[0].get('node').get('nphWithdrawalStatus')
        self.assertIn("time", actual_result)
        self.assertIn("value", actual_result)

    def test_nphDeactivationStatus_fields(self):
        field_to_test = "nphDeactivationStatus {value time} "
        query = simple_query(field_to_test)
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))
        actual_result = result.get('participant').get('edges')[0].get('node').get('nphDeactivationStatus')
        self.assertIn("time", actual_result)
        self.assertIn("value", actual_result)

    def test_nphDateOfBirth_field(self):
        field_to_test = "nphDateOfBirth"
        query = simple_query(field_to_test)
        mock_load_participant_data(self.session)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))
        has_nph_dob = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(has_nph_dob.get('nphDateOfBirth') == '1980-01-01')
        no_nph_dob = result.get('participant').get('edges')[1].get('node')
        self.assertTrue(no_nph_dob.get('nphDateOfBirth') == 'UNSET')

    def _group_ordered_samples_by_participant(
        self,
        nph_participants: Iterable[NphParticipant],
        grouped_orders: Dict[int, List[Order]],
        grouped_ordered_samples: Dict[int, List[OrderedSample]],
    ) -> List[OrderedSample]:
        grouped_ordered_samples_by_participant = defaultdict(list)
        for participant in nph_participants:
            for order in grouped_orders[participant.id]:
                grouped_ordered_samples_by_participant[participant.id].extend(grouped_ordered_samples[order.id])
        return grouped_ordered_samples_by_participant

    def _create_test_sample_updates(self):
        participant_query = Query(NphParticipant)
        participant_query.session = self.session
        nph_participants = list(participant_query.all())

        nph_sites_query = Query(NphSite)
        nph_sites_query.session = self.session
        sites = list(nph_sites_query.all())
        study_categories = generate_fake_study_categories()
        orders = generate_fake_orders(
            fake_participants=nph_participants,
            fake_study_categories=study_categories,
            fake_sites=sites,
        )
        ordered_samples = generate_fake_ordered_samples(fake_orders=orders)
        generate_fake_sample_updates(fake_ordered_samples=ordered_samples)
        grouped_orders = defaultdict(list)
        _grouped_ordered_samples = defaultdict(list)
        for order in orders:
            grouped_orders[order.participant_id].append(order)

        for ordered_sample in ordered_samples:
            _grouped_ordered_samples[ordered_sample.order_id].append(ordered_sample)
        grouped_ordered_samples_by_participant = self._group_ordered_samples_by_participant(nph_participants, grouped_orders, _grouped_ordered_samples)
        generate_fake_stored_samples(nph_participants, grouped_ordered_samples_by_participant)

    def test_nph_biospecimen_for_participant(self):
        mock_load_participant_data(self.session)
        self._create_test_sample_updates()
        field_to_test = "nphBiospecimens {orderID specimenCode studyID visitID timepointID biobankStatus { limsID biobankModified status } } "
        query = simple_query(field_to_test)

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))
        n_participants = len(result.get('participant').get('edges'))
        for i in range(n_participants):
            self.assertEqual(
                12,
                len(result.get("participant").get("edges")[i].get("node").get("nphBiospecimens"))
            )

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
        self.clear_table_after_test("rdr.code")
        self.clear_table_after_test("rdr.hpo")
        self.clear_table_after_test("rdr.site")
        self.clear_table_after_test("rdr.participant")
        self.clear_table_after_test("rdr.participant_summary")
        self.clear_table_after_test("rex.participant_mapping")
        self.clear_table_after_test("rex.study")
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.pairing_event_type")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.sample_update")
        self.clear_table_after_test("nph.stored_sample")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.pairing_event")
        self.clear_table_after_test("nph.enrollment_event")
        self.clear_table_after_test("nph.enrollment_event_type")
        self.clear_table_after_test("nph.participant_ops_data_element")


class TestQueryValidator(BaseTestCase):

    def test_validation_error(self):
        self.assertRaises(GraphQLSyntaxError, api.validate_query, QUERY_WITH_SYNTAX_ERROR)

    def test_validation_no_error(self):
        query = condition_query("nphId", "100000001", "participantNphId")
        result = api.validate_query(query)
        self.assertEqual([], result)
