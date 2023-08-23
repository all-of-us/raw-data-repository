import json
import time

from typing import Iterable, Dict, List
from collections import defaultdict
from graphql import GraphQLSyntaxError
from datetime import datetime, timedelta

from rdr_service.ancillary_study_resources.nph.enums import ParticipantOpsElementTypes, StoredSampleStatus, DietType, \
    DietStatus, ModuleTypes, ConsentOptInTypes
from rdr_service.config import NPH_PROD_BIOBANK_PREFIX, NPH_TEST_BIOBANK_PREFIX
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.rex_dao import RexStudyDao, RexParticipantMappingDao
from sqlalchemy.orm import Query

from rdr_service.dao.study_nph_dao import NphParticipantDao, NphDefaultBaseDao
from rdr_service.model.study_nph import (
    ConsentEventType, Participant as NphParticipant, Site as NphSite, OrderedSample, Order, WithdrawalEvent,
    DeactivationEvent
)
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.main import app
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.data_gen.generators.nph import NphDataGenerator, NphSmsDataGenerator
import rdr_service.api.nph_participant_api as api
from rdr_service import config, clock
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


def simple_query_with_pagination(value: str, limit: int, offset: int):
    return ''' { participant (limit: %s, offSet: %s) {totalCount resultCount pageInfo
           { startCursor  endCursor hasNextPage }  edges { node { participantNphId %s } } } }''' % (limit, offset, value)


def simple_query(value):
    return ''' { participant  {totalCount resultCount pageInfo
           { startCursor  endCursor hasNextPage }  edges { node { participantNphId %s } } } }''' % value


def condition_query(condition, sort_value, sort_field):
    return ''' { participant (%s: %s) {totalCount resultCount pageInfo
           { startCursor  endCursor hasNextPage }  edges { node { %s } } } }''' % (condition, sort_value, sort_field)


class NphParticipantAPITest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.nph_data_gen = NphDataGenerator()
        self.sms_data_gen = NphSmsDataGenerator()
        self.rex_study_dao = RexStudyDao()
        self.rex_mapping_dao = RexParticipantMappingDao()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.nph_participant_dao = NphParticipantDao()
        self.nph_consent_event_type_dao = NphDefaultBaseDao(model_type=ConsentEventType)
        self.nph_withdrawal_event_dao = NphDefaultBaseDao(model_type=WithdrawalEvent)
        self.nph_deactivation_event_dao = NphDefaultBaseDao(model_type=DeactivationEvent)

        self.data_generator.create_database_hpo()
        self.data_generator.create_database_site()
        self.data_generator.create_database_code()

        self.base_participant_ids = [100000000, 100000001]

        # study records
        for study in ['rdr', 'nph']:
            self.rex_study_dao.insert(
                self.rex_study_dao.model_type(**{
                    'schema_name': study
                }))

        # nph activities
        for activity_name in ['ENROLLMENT', 'PAIRING', 'CONSENT', 'WITHDRAWAL', 'DEACTIVATION']:
            self.nph_data_gen.create_database_activity(
                name=activity_name
            )

        # nph sites
        for i in range(1, 3):
            self.nph_data_gen.create_database_site(
                external_id=f"nph-test-site-{i}",
                name=f"nph-test-site-name-{i}",
                awardee_external_id="nph-test-hpo",
                organization_external_id="nph-test-org"
            )

        # EVENT TYPES
        # pairing event type(s)
        self.nph_data_gen.create_database_pairing_event_type(name="INITIAL")
        # enrollment event type(s)
        enrollment_event_types = {
            'nph_referred': {
                'name': 'REFERRED',
            },
            'module1_consented': {
                'name': 'Module 1 Consented'
            }
        }
        for name, value in enrollment_event_types.items():
            self.nph_data_gen.create_database_enrollment_event_type(
                name=value['name'],
                source_name=name
            )
        # consent event type(s)
        consent_event_types = [
            ("Module 1 Consent", "m1_consent"),
            ("Module 1 Consent GPS", "m1_consent_gps"),
            ("Module 1 Consent Recontact", "m1_consent_recontact"),
            ("Module 1 Consent Tissue", "m1_consent_tissue"),
            ("Module 2 Consent", "m2_consent"),
            ("Module 3 Consent", "m3_consent")
        ]
        consent_event_type_objs: Iterable[ConsentEventType] = []
        for name, source_name in consent_event_types:
            consent_event_type = self.nph_data_gen.create_database_consent_event_type(
                name=name,
                source_name=source_name
            )
            consent_event_type_objs.append(consent_event_type)

        # main participant(s) (AOU Summary)
        for _ in enumerate(range(10)):
            self.data_generator.create_database_participant_summary(
                hpoId=0,
                siteId=1,
                dateOfBirth=self.fake.date_of_birth(),
                deceasedAuthored=self.fake.date_time()
            )

        # rex mapping and participant(s) (NPH)
        ancillary_participant_id = 100000000
        for summary in self.participant_summary_dao.get_all():
            self.rex_mapping_dao.insert(
                self.rex_mapping_dao.model_type(**{
                    'primary_participant_id': summary.participantId,
                    'ancillary_participant_id': ancillary_participant_id,
                    'ancillary_study_id': 2
                }))

            self.nph_data_gen.create_database_participant(id=ancillary_participant_id)
            ancillary_participant_id = ancillary_participant_id + 1

    # base method for adding consent events - main req for showing in resp
    def add_consents(self, nph_participant_ids: List = None, module_nums: List[int] = None, **kwargs) -> None:
        all_consent_event_types = self.nph_consent_event_type_dao.get_all()
        if module_nums:
            updated_consent_event_types = []
            for num in module_nums:
                updated_consent_event_types.extend(
                    [obj for obj in all_consent_event_types if f'm{num}_' in obj.source_name]
                )
            all_consent_event_types = updated_consent_event_types

        if not nph_participant_ids:
            nph_participant_ids = [obj.id for obj in self.nph_participant_dao.get_all()]

        for participant_id in nph_participant_ids:
            for consent_event_type in all_consent_event_types:
                self.nph_data_gen.create_database_consent_event(
                        participant_id=participant_id,
                        event_type_id=consent_event_type.id,
                        opt_in=kwargs.get('opt_in', ConsentOptInTypes.PERMIT)
                )

    def test_client_single_result(self):
        self.add_consents(nph_participant_ids=[self.base_participant_ids[1]])
        fetch_value = '"{}"'.format("100000001")
        query = condition_query("nphId", fetch_value, "participantNphId")
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(1, len(result.get('participant').get('edges')), "Should return 1 record back")
        self.assertEqual("100000001",
                         result.get('participant').get('edges')[0].get('node').get('participantNphId'))

    def test_client_none_value_field(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=QUERY_WITH_NONE_VALUE)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 record back")

        for each in result.get('participant').get('edges'):
            for _, v in each.get('node').items():
                self.assertEqual(str(QuestionnaireStatus.UNSET), v.get('value'))
                self.assertIsNone(v.get('time'))

    def test_client_nph_pair_site(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[0],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=1
        )
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[1],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=2
        )
        field_to_test = "nphPairedSite"
        query = simple_query(field_to_test)
        expected_site_name = ["nph-test-site-1", "nph-test-site-2"]
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")

        for index, each in enumerate(result.get('participant').get('edges')):
            self.assertEqual(expected_site_name[index], each.get('node').get(field_to_test))

    def test_client_nph_awardee_external_id(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[0],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=1
        )
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[1],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=1
        )
        field_to_test = "nphPairedAwardee"
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")

        for each in result.get('participant').get('edges'):
            self.assertEqual('nph-test-hpo', each.get('node').get('nphPairedAwardee'))

    def test_client_nph_organization_external_id(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[0],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=1
        )
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[1],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=1
        )
        field_to_test = "nphPairedOrg"
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')), "Should return 2 records back")

        for each in result.get('participant').get('edges'):
            self.assertEqual('nph-test-org', each.get('node').get(field_to_test))

    def test_client_biobank_id_prefix(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
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
        self.add_consents(nph_participant_ids=[self.base_participant_ids[0]])
        self.nph_data_gen.create_database_pairing_event(
            participant_id=self.base_participant_ids[0],
            event_authored_time=datetime(2023, 1, 1, 12, 1),
            site_id=1
        )
        fetch_value = '"{}"'.format("100000000")
        query = condition_query("nphId", fetch_value, "nphPairedSite")
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(1, len(result.get('participant').get('edges')), "Should return 1 record back")
        self.assertEqual("nph-test-site-1", result.get('participant').get('edges')[0].get('node'
                                                                                          ).get('nphPairedSite'))

    def test_client_sorting_deceased_status(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        sort_field = '"{}"'.format("aouDeceasedStatus:time")
        query = condition_query("sortBy", sort_field, "aouDeceasedStatus {value time}")
        deceased_list = []
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        for each in result.get('participant').get('edges'):
            datetime_object = datetime.strptime(each.get('node').get('aouDeceasedStatus').get('time'),
                                                '%Y-%m-%dT%H:%M:%S')
            deceased_list.append(datetime_object)
        sorted_list = deceased_list.copy()
        sorted_list.sort()
        self.assertTrue(deceased_list == sorted_list, msg="Resultset is not in sorting order")

    def test_client_filter_parameters(self):
        summary = self.participant_summary_dao.get_by_participant_id(900000000)
        rex_participants = self.rex_mapping_dao.get_all()
        nph_participant = list(filter(lambda x: x.primary_participant_id == summary.participantId, rex_participants))[0]
        self.add_consents(nph_participant_ids=[nph_participant.ancillary_participant_id])
        # firstname filter
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (firstName: "%s") { edges { node { participantNphId firstName } } } }' %
                 summary.firstName
        )
        result = json.loads(executed.data.decode('utf-8'))

        result_participant_list = result.get('participant').get('edges')
        self.assertEqual(1, len(result_participant_list))

        resulting_participant_data = result_participant_list[0].get('node')
        self.assertEqual(summary.firstName, resulting_participant_data.get('firstName'))
        self.assertEqual(nph_participant.ancillary_participant_id,
                         int(resulting_participant_data.get('participantNphId')))

    def test_nph_dob_filter_parameter(self):
        summary = self.participant_summary_dao.get_by_participant_id(900000000)
        rex_participants = self.rex_mapping_dao.get_all()
        nph_participant = list(filter(lambda x: x.primary_participant_id == summary.participantId, rex_participants))[0]
        nph_dob = '1986-01-01'

        self.add_consents(nph_participant_ids=[nph_participant.ancillary_participant_id])
        self.nph_data_gen.create_database_participant_ops_data_element(
            source_data_element=ParticipantOpsElementTypes.BIRTHDATE,
            participant_id=nph_participant.ancillary_participant_id,
            source_value=nph_dob
        )
        # nphDateOfBirth filter - response firstName and nphDateOfBirth
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (nphDateOfBirth: "%s" ) { edges { node { participantNphId '
                 'firstName nphDateOfBirth } } } }' %
                 nph_dob
        )
        result = json.loads(executed.data.decode('utf-8'))

        result_participant_list = result.get('participant').get('edges')
        self.assertEqual(1, len(result_participant_list))

        self.assertTrue(result_participant_list[0].get('node').get('firstName') == summary.firstName)
        self.assertTrue(result_participant_list[0].get('node').get('nphDateOfBirth') == nph_dob)
        self.assertTrue(result_participant_list[0].get('node').get('participantNphId') ==
                        str(nph_participant.ancillary_participant_id))

        # nphDateOfBirth filter - bad nphDateOfBirth should be no result
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (nphDateOfBirth: "1989-01-01" ) { edges { node { participantNphId '
                 'firstName nphDateOfBirth } } } }'
        )
        result = json.loads(executed.data.decode('utf-8'))
        self.assertTrue(result.get('participant').get('edges') == [])

    def test_nph_prefix_strip_filter_parameter(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        current_nph_participant = self.nph_participant_dao.get(self.base_participant_ids[0])

        # biobankId w/ prefix filter - response biobankId nphParticipantId
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (biobankId: "%s" ) { edges { node { participantNphId biobankId } } } }' %
                 f'T{current_nph_participant.biobank_id}'
        )
        result = json.loads(executed.data.decode('utf-8'))

        result_participant_list = result.get('participant').get('edges')
        self.assertEqual(1, len(result_participant_list))
        self.assertEqual(
            result_participant_list[0].get('node').get('biobankId'), f'T{current_nph_participant.biobank_id}'
        )
        self.assertEqual(
            result_participant_list[0].get('node').get('participantNphId'), str(current_nph_participant.id)
        )

        # biobankId w/o prefix filter - response biobankId nphParticipantId
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (biobankId: "%s" ) { edges { node { participantNphId biobankId } } } }' %
                 current_nph_participant.biobank_id
        )
        result = json.loads(executed.data.decode('utf-8'))

        result_participant_list = result.get('participant').get('edges')
        self.assertEqual(1, len(result_participant_list))
        self.assertEqual(
            result_participant_list[0].get('node').get('biobankId'), f'T{current_nph_participant.biobank_id}'
        )
        self.assertEqual(
            result_participant_list[0].get('node').get('participantNphId'), str(current_nph_participant.id)
        )

        # fake biobankId w prefix filter - should be no response
        executed = app.test_client().post(
            '/rdr/v1/nph_participant',
            data='{participant (biobankId: "%s" ) { edges { node { participantNphId biobankId } } } }' % '21212121212'
        )
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(result.get('participant').get('edges'), [])

    def test_nphEnrollmentStatus_fields(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self.nph_data_gen.create_database_enrollment_event(
            participant_id=self.base_participant_ids[0],
            event_authored_time=clock.CLOCK.now(),
            event_type_id=2
        )
        self.nph_data_gen.create_database_enrollment_event(
            participant_id=self.base_participant_ids[1],
            event_authored_time=clock.CLOCK.now(),
            event_type_id=2
        )
        field_to_test = "nphEnrollmentStatus {value time} "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        enrollment_statuses = result.get('participant').get('edges')[0].get('node').get('nphEnrollmentStatus')

        for status in enrollment_statuses:
            self.assertIn("time", status)
            self.assertIn("value", status)
            self.assertEqual(status['value'], 'module1_consented')

    def test_blocked_nphEnrollmentStatus_fields(self):
        self.add_consents(nph_participant_ids=[self.base_participant_ids[0]])
        # module1_consented
        self.nph_data_gen.create_database_enrollment_event(
            participant_id=self.base_participant_ids[0],
            event_authored_time=clock.CLOCK.now(),
            event_type_id=2
        )
        # death and losttofollowup
        blocked_enrollment_event_types = {
            'module1_death': {
                'name': 'Module 1 Death'
            },
            'module1_losttofollowup': {
                'name': 'Module 1 Lost to follow up'
            }
        }
        for name, value in blocked_enrollment_event_types.items():
            event_type = self.nph_data_gen.create_database_enrollment_event_type(
                name=value['name'],
                source_name=name
            )
            self.nph_data_gen.create_database_enrollment_event(
                participant_id=self.base_participant_ids[0],
                event_authored_time=clock.CLOCK.now(),
                event_type_id=event_type.id
            )
        field_to_test = "nphEnrollmentStatus {value time} "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(1, len(result.get('participant').get('edges')))

        enrollment_statuses = result.get('participant').get('edges')[0].get('node').get('nphEnrollmentStatus')

        self.assertEqual(len(enrollment_statuses), 1)
        for status in enrollment_statuses:
            self.assertIn("time", status)
            self.assertIn("value", status)
            self.assertEqual(status['value'], 'module1_consented')

    def test_nphModule1ConsentStatus_fields(self):
        self.add_consents(
            nph_participant_ids=self.base_participant_ids,
            opt_in=ConsentOptInTypes.PERMIT2
        )
        field_to_test = "nphModule1ConsentStatus {value time optIn} "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        # should have following consents
        m1_consents = ["m1_consent", "m1_consent_gps", "m1_consent_recontact", "m1_consent_tissue"]
        edges = result.get('participant').get('edges')
        for edge in edges:
            self.assertTrue(int(edge.get('node').get('participantNphId')) in self.base_participant_ids)
            consents = edge.get('node').get('nphModule1ConsentStatus')
            self.assertTrue(all(obj.get('value') in m1_consents for obj in consents))
            self.assertTrue(all(obj.get('time') is not None for obj in consents))
            self.assertTrue(all(obj.get('optIn') == 'PERMIT2' for obj in consents))

    def test_nphModule2ConsentStatus_fields(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids, module_nums=[1, 2])
        field_to_test = "nphModule2ConsentStatus {value time optIn} "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        # should have following consents
        m2_consents = ["m2_consent"]
        edges = result.get('participant').get('edges')
        for edge in edges:
            self.assertTrue(int(edge.get('node').get('participantNphId')) in self.base_participant_ids)
            consents = edge.get('node').get('nphModule2ConsentStatus')
            self.assertTrue(all(obj.get('value') in m2_consents for obj in consents))
            self.assertTrue(all(obj.get('time') is not None for obj in consents))
            self.assertTrue(all(obj.get('optIn') == 'PERMIT' for obj in consents))

    def test_nphModule3ConsentStatus_fields(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        field_to_test = "nphModule3ConsentStatus {value time optIn} "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        # should have following consents
        m3_consents = ["m3_consent"]
        edges = result.get('participant').get('edges')
        for edge in edges:
            self.assertTrue(int(edge.get('node').get('participantNphId')) in self.base_participant_ids)
            consents = edge.get('node').get('nphModule3ConsentStatus')
            self.assertTrue(all(obj.get('value') in m3_consents for obj in consents))
            self.assertTrue(all(obj.get('time') is not None for obj in consents))
            self.assertTrue(all(obj.get('optIn') == 'PERMIT' for obj in consents))

    def test_nphDietStatus_fields(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        current_diet_types = [obj for obj in DietType if obj.name != 'LMT']
        # add diet data - module 2 -> 100000000
        first_diet_bundle_time = datetime.utcnow() - timedelta(days=1)
        with clock.FakeClock(first_diet_bundle_time):
            # Ensuring the created time for each diet event is the exact same, and 1 day ago
            for num, diet_type in enumerate(current_diet_types, start=1):
                for diet_status in [DietStatus.STARTED, DietStatus.COMPLETED]:
                    self.nph_data_gen.create_database_diet_event(
                        participant_id=self.base_participant_ids[0],
                        module=ModuleTypes.lookup_by_number(2),
                        event_id=1,
                        diet_id=num,
                        status_id=1,
                        status=diet_status,
                        current=1,
                        diet_name=diet_type,
                        event_authored_time=datetime(2023, 1, num, 12, 1)
                    )

        field_to_test = "nphModule2DietStatus { dietName dietStatus { time status current } }"
        query = simple_query(field_to_test)

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        # should only be 100000000 that has diet data
        first_participant = result.get('participant').get('edges')[0]
        self.assertEqual(len(first_participant.get('node').get('nphModule2DietStatus')), 3)
        for diet_node in first_participant.get('node').get('nphModule2DietStatus'):
            self.assertEqual(len(diet_node.get('dietStatus')), 2)
            self.assertIsNotNone(DietType.lookup_by_name(diet_node.get('dietName')))
            for diet_status in diet_node.get('dietStatus'):
                self.assertTrue(type(diet_status.get('current')) is bool)
                self.assertTrue(diet_status.get('status') is not None)
                self.assertTrue(diet_status.get('time') is not None)

                self.assertTrue('2023-01' in diet_status.get('time'))

        second_participant = result.get('participant').get('edges')[1]
        self.assertTrue(second_participant.get('node').get('nphModule2DietStatus') == [])

        time.sleep(5)
        # add more diet data - module 2 -> 100000000 - get max created date
        with clock.FakeClock(datetime.utcnow()):
            for num, diet_type in enumerate(current_diet_types, start=1):
                for diet_status in [DietStatus.STARTED, DietStatus.COMPLETED]:
                    self.nph_data_gen.create_database_diet_event(
                        participant_id=self.base_participant_ids[0],
                        module=ModuleTypes.lookup_by_number(2),
                        event_id=1,
                        diet_id=num,
                        status_id=2,
                        status=diet_status,
                        current=2,
                        diet_name=diet_type,
                        event_authored_time=datetime(2023, 2, num, 12, 1)
                    )

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        # should only be 100000000 and have updated event_authored_time
        first_participant = result.get('participant').get('edges')[0]
        self.assertEqual(len(first_participant.get('node').get('nphModule2DietStatus')), 3)
        for diet_node in first_participant.get('node').get('nphModule2DietStatus'):
            self.assertEqual(len(diet_node.get('dietStatus')), 2)
            self.assertIsNotNone(DietType.lookup_by_name(diet_node.get('dietName')))
            for diet_status in diet_node.get('dietStatus'):
                self.assertTrue(type(diet_status.get('current')) is bool)
                self.assertTrue(diet_status.get('status') is not None)
                self.assertTrue(diet_status.get('time') is not None)

                self.assertTrue('2023-02' in diet_status.get('time'))

        second_participant = result.get('participant').get('edges')[1]
        self.assertTrue(second_participant.get('node').get('nphModule2DietStatus') == [])

    def test_nphWithdrawalStatus_fields(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        # withdrawal for module 1
        self.nph_data_gen.create_database_withdrawal_event(
            event_authored_time=clock.CLOCK.now(),
            participant_id=100000000,
            event_id=1,
            module=ModuleTypes.MODULE1
        )
        # add another withdrawal for module 2
        self.nph_data_gen.create_database_withdrawal_event(
            event_authored_time=clock.CLOCK.now(),
            participant_id=100000000,
            event_id=1,
            module=ModuleTypes.MODULE2
        )

        field_to_test = "nphWithdrawalStatus { value time module } "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        first_participant = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(first_participant.get('participantNphId') == str(self.base_participant_ids[0]))

        current_withdrawals = self.nph_withdrawal_event_dao.get_all()
        self.assertEqual(len(current_withdrawals), 2)

        first_pid_db_withdrawals = list(
            filter(lambda x: x.participant_id == self.base_participant_ids[0], current_withdrawals))
        self.assertEqual(len(first_pid_db_withdrawals), 2)

        first_participant_withdrawals = first_participant.get('nphWithdrawalStatus')
        module_strings = ['module1', 'module2']

        for withdrawal in first_participant_withdrawals:
            self.assertTrue(withdrawal.get('time') is not None)
            self.assertTrue(withdrawal.get('value') == 'WITHDRAWN')
            self.assertTrue(withdrawal.get('module') in module_strings)

        second_participant = result.get('participant').get('edges')[1].get('node')
        self.assertTrue(second_participant.get('participantNphId') == str(self.base_participant_ids[1]))
        second_pid_withdrawals = second_participant.get('nphWithdrawalStatus')
        self.assertEqual(second_pid_withdrawals, [])

    def test_nphDeactivationStatus_fields(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        # deactivation for module 1
        self.nph_data_gen.create_database_deactivated_event(
            event_authored_time=clock.CLOCK.now(),
            participant_id=100000000,
            event_id=1,
            module=ModuleTypes.MODULE1
        )
        # deactivation for module 2
        self.nph_data_gen.create_database_deactivated_event(
            event_authored_time=clock.CLOCK.now(),
            participant_id=100000000,
            event_id=1,
            module=ModuleTypes.MODULE2
        )

        field_to_test = "nphDeactivationStatus { value time module } "
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(2, len(result.get('participant').get('edges')))

        first_participant = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(first_participant.get('participantNphId') == str(self.base_participant_ids[0]))

        current_deactivations = self.nph_deactivation_event_dao.get_all()
        self.assertEqual(len(current_deactivations), 2)

        first_pid_db_deactivations = list(
            filter(lambda x: x.participant_id == self.base_participant_ids[0], current_deactivations))
        self.assertEqual(len(first_pid_db_deactivations), 2)

        first_participant_deactivations = first_participant.get('nphDeactivationStatus')
        module_strings = ['module1', 'module2']

        for deactivation in first_participant_deactivations:
            self.assertTrue(deactivation.get('time') is not None)
            self.assertTrue(deactivation.get('value') == 'DEACTIVATED')
            self.assertTrue(deactivation.get('module') in module_strings)

        second_participant = result.get('participant').get('edges')[1].get('node')
        self.assertTrue(second_participant.get('participantNphId') == str(self.base_participant_ids[1]))
        second_pid_deactivations = second_participant.get('nphDeactivationStatus')
        self.assertEqual(second_pid_deactivations, [])

    def test_nphDateOfBirth_field(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self.nph_data_gen.create_database_participant_ops_data_element(
            source_data_element=ParticipantOpsElementTypes.BIRTHDATE,
            participant_id=100000000,
            source_value='1986-01-01'
        )
        field_to_test = "nphDateOfBirth"
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))

        first_participant = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(first_participant.get('participantNphId') == str(self.base_participant_ids[0]))
        nph_dob = first_participant.get('nphDateOfBirth')
        self.assertTrue(nph_dob is not None)
        self.assertTrue(nph_dob == '1986-01-01')

        second_participant = result.get('participant').get('edges')[1].get('node')
        self.assertTrue(second_participant.get('participantNphId') == str(self.base_participant_ids[1]))
        nph_dob = second_participant.get('nphDateOfBirth')
        self.assertTrue(nph_dob == 'UNSET')

        # add another dob on same participant_id should be updated data returned on first_participant
        self.nph_data_gen.create_database_participant_ops_data_element(
            source_data_element=ParticipantOpsElementTypes.BIRTHDATE,
            participant_id=100000000,
            source_value='1988-01-01'
        )

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))

        first_participant = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(first_participant.get('participantNphId') == str(self.base_participant_ids[0]))
        nph_dob = first_participant.get('nphDateOfBirth')
        self.assertTrue(nph_dob is not None)
        self.assertTrue(nph_dob == '1988-01-01')

    def test_optional_field_returns_correctly(self):
        self.add_consents(nph_participant_ids=[self.base_participant_ids[0]])
        summary_with_site = self.participant_summary_dao.get_by_participant_id(900000000)

        field_to_test = "siteId"
        query = simple_query(field_to_test)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(1, len(result.get('participant').get('edges')))

        current_participant = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(current_participant.get('participantNphId') == str(self.base_participant_ids[0]))
        aou_site = current_participant.get('siteId')
        self.assertTrue(aou_site is not None)
        self.assertTrue(aou_site == 'hpo-site-monroeville')

        # remove site from participant summary - re-call route
        summary_with_site.siteId = None
        self.participant_summary_dao.update(summary_with_site)

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))

        self.assertEqual(1, len(result.get('participant').get('edges')))
        current_participant = result.get('participant').get('edges')[0].get('node')
        self.assertTrue(current_participant.get('participantNphId') == str(self.base_participant_ids[0]))
        aou_site = current_participant.get('siteId')
        self.assertTrue(aou_site == 'UNSET')

    def test_nph_biospecimen_for_participant(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self._create_test_sample_updates()
        field_to_test = "nphBiospecimens {orderID specimenCode studyID visitID collectionDateUTC processingDateUTC " \
                        "timepointID biobankStatus { limsID biobankModified status } } "
        query = simple_query(field_to_test)

        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, len(result.get('participant').get('edges')))
        n_participants = len(result.get('participant').get('edges'))
        for i in range(n_participants):
            biospecimens: Iterable[Dict[str, str]] = (
                result.get("participant").get("edges")[i].get("node").get("nphBiospecimens")
            )
            self.assertEqual(8, len(biospecimens))
            for biospecimen in biospecimens:
                self.assertIsNotNone(biospecimen.get("biobankStatus")[0].get("status"))
                self.assertNotEqual(biospecimen.get("processingDateUTC"), biospecimen.get("collectionDateUTC"))

    def test_nph_biospecimen_duplicate_stored_samples(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        # Create test orders, ordered samples, and stored samples
        category = self.sms_data_gen.create_database_study_category(
            type_label="Test",
        )

        order = self.sms_data_gen.create_database_order(
            nph_order_id="100",
            participant_id=100000000,
            notes="Test",
            category_id=category.id

        )
        self.sms_data_gen.create_database_ordered_sample(
            order_id=order.id,
            aliquot_id="1234",
        )
        self.sms_data_gen.create_database_ordered_sample(
            order_id=order.id,
            aliquot_id="5678",
        )
        for _ in range(5):
            self.sms_data_gen.create_database_stored_sample(
                biobank_id=1100000000,
                sample_id="1234",
                lims_id="142857"
            )
        for _ in range(5):
            self.sms_data_gen.create_database_stored_sample(
                biobank_id=1100000000,
                sample_id="5678",
                lims_id="857142"
            )
        # Add another status
        self.sms_data_gen.create_database_stored_sample(
            biobank_id=1100000000,
            sample_id="5678",
            lims_id="857142",
            status=StoredSampleStatus.DISPOSED
        )
        # Call api
        gql_query = """
            { participant (nphId: "100000000") {totalCount resultCount pageInfo
           { startCursor  endCursor hasNextPage }  edges { node { nphBiospecimens { sampleID biobankStatus { limsID } } } } } }
        """
        executed = app.test_client().post('/rdr/v1/nph_participant', data=gql_query)
        result = json.loads(executed.data.decode('utf-8'))

        participant = result.get('participant').get('edges')[0].get('node')

        self.assertEqual(2, len(participant.get('nphBiospecimens')))
        for specimen in participant.get('nphBiospecimens'):
            if specimen['sampleID'] == '1234':
                self.assertEqual(1, len(specimen.get('biobankStatus')))
            if specimen['sampleID'] == '5678':
                self.assertEqual(2, len(specimen.get('biobankStatus')))

    def test_nph_biospecimen_for_participant_with_pagination(self):
        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self._create_test_sample_updates()
        field_to_test = "nphBiospecimens {orderID specimenCode studyID visitID timepointID biobankStatus { limsID biobankModified status } } "
        query_1 = simple_query_with_pagination(field_to_test, limit=1, offset=0)
        result_1 = app.test_client().post('/rdr/v1/nph_participant', data=query_1)
        result_1 = json.loads(result_1.data.decode('utf-8'))
        self.assertEqual(1, len(result_1.get('participant').get('edges')))

        query_2 = simple_query_with_pagination(field_to_test, limit=1, offset=1)
        result_2 = app.test_client().post('/rdr/v1/nph_participant', data=query_2)
        result_2 = json.loads(result_2.data.decode('utf-8'))
        self.assertEqual(1, len(result_2.get('participant').get('edges')))
        for result in [result_1, result_2]:
            biospecimens: Iterable[Dict[str, str]] = (
                result.get("participant").get("edges")[0].get("node").get("nphBiospecimens")
            )
            self.assertEqual(8, len(biospecimens))
            for biospecimen in biospecimens:
                self.assertIsNotNone(biospecimen.get("biobankStatus")[0].get("status"))

    def test_total_count(self):
        """
        Check that the totalCount given in a response matches the count of participants
        that can be expected for a given query (the total number of participants that match,
        and would be included if all pages are retrieved).
        """

        self.add_consents(nph_participant_ids=self.base_participant_ids)
        self.nph_data_gen.create_database_participant_ops_data_element(
            source_data_element=ParticipantOpsElementTypes.BIRTHDATE,
            participant_id=100000000,
            source_value='1986-01-01'
        )
        query = simple_query_with_pagination(value='', limit=1, offset=1)
        executed = app.test_client().post('/rdr/v1/nph_participant', data=query)

        result = json.loads(executed.data.decode('utf-8'))
        self.assertEqual(2, result['participant']['totalCount'])  # assuming the two consented participants are returned

    @staticmethod
    def _group_ordered_samples_by_participant(
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
        self.clear_table_after_test("nph.consent_event")
        self.clear_table_after_test("nph.diet_event")


class NphParticipantAPITestValidation(BaseTestCase):

    def test_validation_error(self):
        self.assertRaises(GraphQLSyntaxError, api.validate_query, QUERY_WITH_SYNTAX_ERROR)

    def test_validation_no_error(self):
        query = condition_query("nphId", "100000001", "participantNphId")
        result = api.validate_query(query)
        self.assertEqual([], result)

    def test_graphql_field_error(self):
        queries = [QUERY_WITH_FIELD_ERROR, QUERY_WITH_MULTI_FIELD_ERROR]
        for query in queries:
            executed = app.test_client().post('/rdr/v1/nph_participant', data=query)
            result = json.loads(executed.data.decode('utf-8'))
            for error in result.get('errors'):
                self.assertIn('message', error)
                self.assertIn('locations', error)

    def test_graphql_syntax_error(self):
        executed = app.test_client().post('/rdr/v1/nph_participant', data=QUERY_WITH_SYNTAX_ERROR)
        result = json.loads(executed.data.decode('utf-8'))
        self.assertIn("Syntax Error", result.get('errors').get('message'))
