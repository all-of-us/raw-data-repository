import json

from rdr_service.ancillary_study_resources.nph.enums import ParticipantOpsElementTypes, ConsentOptInTypes, ModuleTypes, \
    DietType
from rdr_service.dao.study_nph_dao import NphParticipantDao, NphSiteDao, NphParticipantEventActivityDao, \
    NphEnrollmentEventTypeDao, NphPairingEventDao, NphDefaultBaseDao, NphActivityDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.model.study_nph import ConsentEvent, EnrollmentEvent, WithdrawalEvent, DeactivationEvent, \
    ParticipantOpsDataElement, DietEvent
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path


class NphIntakeAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_data_gen = NphDataGenerator()
        activities = ['ENROLLMENT', 'PAIRING', 'CONSENT', 'WITHDRAWAL', 'DEACTIVATION', 'DIET']
        self.nph_activity = NphActivityDao()
        self.nph_participant = NphParticipantDao()
        self.nph_participant_activity_dao = NphParticipantEventActivityDao()
        self.nph_site_dao = NphSiteDao()
        self.nph_enrollment_type_dao = NphEnrollmentEventTypeDao()
        self.nph_pairing_event_dao = NphPairingEventDao()
        self.nph_consent_event_dao = NphDefaultBaseDao(model_type=ConsentEvent)
        self.nph_enrollment_event_dao = NphDefaultBaseDao(model_type=EnrollmentEvent)
        self.nph_withdrawal_event_dao = NphDefaultBaseDao(model_type=WithdrawalEvent)
        self.nph_deactivation_event_dao = NphDefaultBaseDao(model_type=DeactivationEvent)
        self.nph_diet_event_dao = NphDefaultBaseDao(model_type=DietEvent)
        self.participant_ops_data = NphDefaultBaseDao(model_type=ParticipantOpsDataElement)

        for activity in activities:
            self.nph_data_gen.create_database_activity(
                name=activity
            )

        self.nph_data_gen.create_database_pairing_event_type(
            name="Initial"
        )

        # Module 1 Enrollment
        self.nph_data_gen.create_database_enrollment_event_type(
            name="Module 1 Eligibility Confirmed",
            source_name='module1_eligibilityConfirmed'
        )
        self.nph_data_gen.create_database_enrollment_event_type(
            name="Module 1 Consented",
            source_name='module1_consented'
        )

        # Module 2 Enrollment
        self.nph_data_gen.create_database_enrollment_event_type(
            name="Module 2 Eligibility Confirmed",
            source_name='module2_eligibilityConfirmed'
        )
        self.nph_data_gen.create_database_enrollment_event_type(
            name="Module 2 Consented",
            source_name='module2_consented'
        )

        self.nph_data_gen.create_database_enrollment_event_type(
            name="Module 2 Started",
            source_name='module2_started'
        )

        self.nph_data_gen.create_database_enrollment_event_type(
            name="Module 2 Complete",
            source_name='module2_completed'
        )

        for _ in range(2):
            self.nph_data_gen.create_database_participant()

        for i in range(1, 3):
            self.nph_data_gen.create_database_site(
                external_id=f"nph-test-site-{i}",
                name=f"nph-test-site-name-{i}",
                awardee_external_id="nph-test-hpo",
                organization_external_id="nph-test-org"
            )

        # Module 1 Consent
        # overall consent obj
        self.nph_data_gen.create_database_consent_event_type(
            name='Module 1 Consent',
            source_name='m1_consent'
        )

        # consent opt-ins only for
        self.nph_data_gen.create_database_consent_event_type(
            name='Module 1 Consent GPS',
            source_name='m1_consent_gps'
        )

        self.nph_data_gen.create_database_consent_event_type(
            name='Module 1 Consent Recontact',
            source_name='m1_consent_recontact'
        )

        self.nph_data_gen.create_database_consent_event_type(
            name='Module 1 Consent Tissue',
            source_name='m1_consent_tissue'
        )

        # Module 2 Consent
        self.nph_data_gen.create_database_consent_event_type(
            name='Module 2 Consent',
            source_name='m2_consent'
        )

    # FHIR payloads
    def test_m1_detailed_consent_payload(self):

        with open(data_path('nph_m1_detailed_consent_multi.json')) as f:
            consent_json = json.load(f)

        all_participant_ids = self.nph_participant.get_all()
        all_participant_ids = [obj.id for obj in all_participant_ids]

        # response
        response = self.send_post('nph/Intake/FHIR', request_data=consent_json)

        self.assertEqual(len(response), len(all_participant_ids))
        self.assertTrue(all(int(obj['nph_participant_id']) in all_participant_ids for obj in response))

        # participant event activities
        participant_event_activities = self.nph_participant_activity_dao.get_all()

        pairing_activity = self.nph_activity.get(2)
        consent_activity = self.nph_activity.get(3)

        # pairing and consent ids
        should_have_activity_ids = [pairing_activity.id, consent_activity.id]

        for event in participant_event_activities:
            current_participant_events = list(
                filter(lambda x: x.participant_id == event.participant_id, participant_event_activities)
            )

            self.assertTrue(len(current_participant_events), len(should_have_activity_ids))

            activity_ids = [obj.activity_id for obj in current_participant_events]
            self.assertEqual(activity_ids, should_have_activity_ids)

            self.assertTrue(all(obj.resource is not None for obj in current_participant_events))
            self.assertTrue(all(obj.resource.get('bundle_identifier') is not None for obj in
                                current_participant_events))

        # pairing events
        pairing_events = self.nph_pairing_event_dao.get_all()

        self.assertEqual(len(pairing_events), len(all_participant_ids))
        self.assertTrue(all(obj.event_type_id == 1 for obj in pairing_events))
        self.assertTrue(all(obj.site_id is not None for obj in pairing_events))
        self.assertTrue(all(obj.site_id == 1 for obj in pairing_events))
        self.assertTrue(all(obj.participant_id in all_participant_ids for obj in pairing_events))

        pairing_participant_event_activity = list(
            filter(lambda x: x.activity_id == pairing_activity.id, participant_event_activities))
        pairing_participant_event_activity_ids = [obj.id for obj in pairing_participant_event_activity]

        self.assertTrue(all(obj.event_id in pairing_participant_event_activity_ids for obj in pairing_events))

        # consent events
        consent_events = self.nph_consent_event_dao.get_all()

        # should have 4 (1 main + 3 opt-ins) consent events per participant_id
        self.assertEqual(len(consent_events), len(all_participant_ids) * 4)
        self.assertTrue(all(obj.event_type_id in [1, 2, 3, 4] for obj in consent_events))
        self.assertTrue(all(obj.participant_id in all_participant_ids for obj in consent_events))
        self.assertTrue(all(obj.opt_in is not None for obj in consent_events))

        # main consent decision (permit)
        self.assertTrue(all(obj.opt_in == ConsentOptInTypes.PERMIT for obj in consent_events if obj.event_type_id == 1))
        # granular answers for consent opt ins
        self.assertTrue(all(obj.event_id == 2 for obj in consent_events if obj.participant_id == 100000000))
        # permit deny permit permit
        self.assertTrue([obj.opt_in.number for obj in consent_events if
                         obj.participant_id == 100000000] == [1, 2, 1, 1])
        self.assertTrue(all(obj.event_id == 4 for obj in consent_events if obj.participant_id == 100000001))
        # permit deny permit deny
        self.assertTrue([obj.opt_in.number for obj in consent_events if
                         obj.participant_id == 100000001] == [1, 2, 1, 2])

        consent_participant_event_activity = list(
            filter(lambda x: x.activity_id == consent_activity.id, participant_event_activities))
        consent_participant_event_activity_ids = [obj.id for obj in consent_participant_event_activity]

        self.assertTrue(all(obj.event_id in consent_participant_event_activity_ids for obj in consent_events))

        # other events (should be null)
        self.assertTrue(self.nph_enrollment_event_dao.get_all() == [])
        self.assertTrue(self.nph_withdrawal_event_dao.get_all() == [])
        self.assertTrue(self.nph_deactivation_event_dao.get_all() == [])
        self.assertTrue(self.nph_diet_event_dao.get_all() == [])

    def test_m1_operational_payload(self):

        with open(data_path('nph_m1_operational_multi.json')) as f:
            consent_json = json.load(f)

        current_participant_id = 100000000

        # response
        response = self.send_post('nph/Intake/FHIR', request_data=consent_json)

        self.assertEqual(len(response), 1)
        self.assertTrue(all(int(obj['nph_participant_id']) == current_participant_id for obj in response))

        # participant event activities
        participant_event_activities = self.nph_participant_activity_dao.get_all()

        enrollment_activity = self.nph_activity.get(1)
        pairing_activity = self.nph_activity.get(2)
        deactivation_activity = self.nph_activity.get(5)

        # pairing, enrollment, deactivation ids
        should_have_activity_ids = [enrollment_activity.id, pairing_activity.id, deactivation_activity.id]

        for event in participant_event_activities:
            current_participant_events = list(
                filter(lambda x: x.participant_id == event.participant_id, participant_event_activities)
            )

            self.assertTrue(len(current_participant_events), len(should_have_activity_ids))

            activity_ids = [obj.activity_id for obj in current_participant_events]
            self.assertEqual(activity_ids, should_have_activity_ids)

            self.assertTrue(all(obj.resource is not None for obj in current_participant_events))
            self.assertTrue(all(obj.resource.get('bundle_identifier') is not None for obj in
                                current_participant_events))

        # pairing events
        pairing_events = self.nph_pairing_event_dao.get_all()

        self.assertEqual(len(pairing_events), 1)
        self.assertTrue(all(obj.event_type_id == 1 for obj in pairing_events))
        self.assertTrue(all(obj.site_id is not None for obj in pairing_events))
        self.assertTrue(all(obj.site_id == 1 for obj in pairing_events))
        self.assertTrue(all(obj.participant_id == current_participant_id for obj in pairing_events))

        pairing_participant_event_activity = list(
            filter(lambda x: x.activity_id == pairing_activity.id, participant_event_activities))
        pairing_participant_event_activity_ids = [obj.id for obj in pairing_participant_event_activity]

        self.assertTrue(all(obj.event_id in pairing_participant_event_activity_ids for obj in pairing_events))

        # enrollment events
        enrollment_events = self.nph_enrollment_event_dao.get_all()

        self.assertEqual(len(enrollment_events), 1)
        self.assertTrue(all(obj.event_type_id == 1 for obj in enrollment_events))
        self.assertTrue(all(obj.participant_id == current_participant_id for obj in enrollment_events))

        enrollment_participant_event_activity = list(
            filter(lambda x: x.activity_id == enrollment_activity.id, participant_event_activities))
        consent_participant_event_activity_ids = [obj.id for obj in enrollment_participant_event_activity]

        self.assertTrue(all(obj.event_id in consent_participant_event_activity_ids for obj in enrollment_events))

        # other events (should be null)
        self.assertTrue(self.nph_consent_event_dao.get_all() == [])
        self.assertTrue(self.nph_withdrawal_event_dao.get_all() == [])
        self.assertTrue(self.nph_diet_event_dao.get_all() == [])

    def test_m1_2_operational_payload(self):

        with open(data_path('nph_m1_2_operational_multi.json')) as f:
            consent_json = json.load(f)

        current_participant_ids = [100000000, 100000001]

        response = self.send_post('nph/Intake/FHIR', request_data=consent_json)

        self.assertEqual(len(response), 2)
        self.assertTrue(all(int(obj['nph_participant_id']) in current_participant_ids for obj in response))

        # participant event activities
        participant_event_activities = self.nph_participant_activity_dao.get_all()

        enrollment_activity = self.nph_activity.get(1)
        withdrawal_activity = self.nph_activity.get(4)

        # pairing, enrollment and withdrawal ids
        should_have_activity_ids = [enrollment_activity.id, withdrawal_activity.id]

        for event in participant_event_activities:
            current_participant_events = list(
                filter(lambda x: x.participant_id == event.participant_id, participant_event_activities)
            )

            self.assertTrue(len(current_participant_events), len(should_have_activity_ids))

            activity_ids = [obj.activity_id for obj in current_participant_events]
            self.assertEqual(activity_ids, should_have_activity_ids)

            self.assertTrue(all(obj.resource is not None for obj in current_participant_events))
            self.assertTrue(all(obj.resource.get('bundle_identifier') is not None for obj in
                                current_participant_events))

        # enrollment events
        enrollment_events = self.nph_enrollment_event_dao.get_all()

        self.assertEqual(len(enrollment_events), 1)
        self.assertTrue(all(obj.event_type_id == 1 for obj in enrollment_events))
        self.assertTrue(all(obj.participant_id == current_participant_ids[1] for obj in enrollment_events))

        enrollment_participant_event_activity = list(
            filter(lambda x: x.activity_id == enrollment_activity.id, participant_event_activities))
        consent_participant_event_activity_ids = [obj.id for obj in enrollment_participant_event_activity]

        self.assertTrue(all(obj.event_id in consent_participant_event_activity_ids for obj in enrollment_events))

        # withdrawal events
        withdrawal_events = self.nph_withdrawal_event_dao.get_all()

        self.assertEqual(len(withdrawal_events), 1)
        self.assertTrue(all(obj.participant_id == current_participant_ids[1] for obj in withdrawal_events))
        self.assertTrue(all(obj.module is not None for obj in withdrawal_events))
        self.assertTrue(all(obj.module == ModuleTypes.MODULE1 for obj in withdrawal_events))

        withdrawal_participant_event_activity = list(
            filter(lambda x: x.activity_id == withdrawal_activity.id, participant_event_activities))
        withdrawal_participant_event_activity_ids = [obj.id for obj in withdrawal_participant_event_activity]

        self.assertTrue(all(obj.event_id in withdrawal_participant_event_activity_ids for obj in withdrawal_events))

        # check date of birth was added from Patient resourceType - should be TWO records
        all_ops_data = self.participant_ops_data.get_all()
        self.assertTrue(len(all_ops_data), 2)

        self.assertTrue(
            all(ops_data_record.participant_id in current_participant_ids for ops_data_record in all_ops_data))
        self.assertTrue(
            all(ops_data_record.source_value in ['1988-02-02', '1988-02-03'] for ops_data_record in all_ops_data))
        self.assertTrue(all(ops_data_record.source_data_element == ParticipantOpsElementTypes.lookup_by_name(
            'BIRTHDATE') for ops_data_record in all_ops_data))

        # other events (should be null)
        self.assertTrue(self.nph_consent_event_dao.get_all() == [])
        self.assertTrue(self.nph_pairing_event_dao.get_all() == [])
        self.assertTrue(self.nph_deactivation_event_dao.get_all() == [])
        self.assertTrue(self.nph_diet_event_dao.get_all() == [])

    # JSON payloads
    def test_m1_operational_json_payload(self):

        with open(data_path("nph_m1_operational_multi_non_fhir.json")) as f:
            consent_json = json.load(f)

        current_participant_ids = [100000000, 100000001]

        response = self.send_post('nph/Intake', request_data=consent_json)

        self.assertEqual(len(response), len(current_participant_ids))
        self.assertTrue(all(int(obj['nph_participant_id']) in current_participant_ids for obj in response))

        # participant event activities
        participant_event_activities = self.nph_participant_activity_dao.get_all()

        for participant_id in current_participant_ids:
            current_participant_events = list(
                filter(lambda x: x.participant_id == participant_id, participant_event_activities)
            )
            activity_ids = [obj.activity_id for obj in current_participant_events]
            if participant_id == current_participant_ids[0]:
                # 2 enrollments 1 consent 1 pairing
                self.assertEqual([1, 1, 3, 2], activity_ids)
            elif participant_id == current_participant_ids[1]:
                # 1 enrollments 1 pairing 1 deactivation 1 withdrawal
                self.assertEqual([1, 2, 5, 4], activity_ids)
            self.assertTrue(all(obj.resource is not None for obj in current_participant_events))
            self.assertTrue(all(obj.resource.get('bundle_identifier') is not None for obj in
                                current_participant_events))

        # pairing events
        pairing_events = self.nph_pairing_event_dao.get_all()

        self.assertEqual(len(pairing_events), len(current_participant_ids))
        self.assertTrue(all(obj.event_type_id == 1 for obj in pairing_events))
        self.assertTrue(all(obj.site_id is not None for obj in pairing_events))
        self.assertTrue(all(obj.site_id in [1, 2] for obj in pairing_events))
        self.assertTrue(all(obj.participant_id in current_participant_ids for obj in pairing_events))

        pairing_participant_event_activity = list(
            filter(lambda x: x.activity_id == 2, participant_event_activities))
        pairing_participant_event_activity_ids = [obj.id for obj in pairing_participant_event_activity]
        self.assertTrue(all(obj.event_id in pairing_participant_event_activity_ids for obj in pairing_events))

        # enrollment events
        enrollment_events = self.nph_enrollment_event_dao.get_all()

        self.assertEqual(len(enrollment_events), len(current_participant_ids) + 1)
        self.assertEqual(len([obj for obj in enrollment_events if obj.participant_id == current_participant_ids[0]]),
                         2)
        self.assertEqual(len([obj for obj in enrollment_events if obj.participant_id == current_participant_ids[1]]),
                         1)
        self.assertTrue(all(obj.participant_id in current_participant_ids for obj in enrollment_events))

        enrollment_participant_event_activity = list(
            filter(lambda x: x.activity_id == 1, participant_event_activities))
        enrollment_participant_event_activity_ids = [obj.id for obj in enrollment_participant_event_activity]
        self.assertTrue(all(obj.event_id in enrollment_participant_event_activity_ids for obj in enrollment_events))

        # consent events
        consent_events = self.nph_consent_event_dao.get_all()

        # 1 for primary + 2 for opt-ins
        self.assertEqual(len(consent_events), 3)
        self.assertTrue(all(obj.participant_id == current_participant_ids[0] for obj in consent_events))
        # all permit
        self.assertTrue(all(obj.opt_in == ConsentOptInTypes.PERMIT for obj in consent_events))
        self.assertEqual([1, 4, 3], [obj.event_type_id for obj in consent_events])
        # should be none for second participant
        self.assertFalse(any(obj.participant_id == current_participant_ids[1] for obj in consent_events))

        consent_participant_event_activity = list(
            filter(lambda x: x.activity_id == 3, participant_event_activities))
        consent_participant_event_activity_ids = [obj.id for obj in consent_participant_event_activity]
        self.assertTrue(all(obj.event_id in consent_participant_event_activity_ids for obj in consent_events))

        # withdrawal events
        withdrawal_events = self.nph_withdrawal_event_dao.get_all()

        self.assertEqual(len(withdrawal_events), 1)
        self.assertTrue(all(obj.participant_id == current_participant_ids[1] for obj in withdrawal_events))
        self.assertTrue(all(obj.module is not None for obj in withdrawal_events))
        self.assertTrue(all(obj.module == ModuleTypes.MODULE1 for obj in withdrawal_events))

        # deactivation events
        deactivation_events = self.nph_deactivation_event_dao.get_all()

        self.assertEqual(len(deactivation_events), 1)
        self.assertTrue(all(obj.participant_id == current_participant_ids[1] for obj in deactivation_events))
        self.assertTrue(all(obj.module is not None for obj in deactivation_events))
        self.assertTrue(all(obj.module == ModuleTypes.MODULE1 for obj in deactivation_events))

        # other events (should be null)
        self.assertTrue(self.nph_diet_event_dao.get_all() == [])

    def test_m2_operational_json_payload(self):

        with open(data_path("nph_m2_operational_multi_non_fhir.json")) as f:
            consent_json = json.load(f)

        current_participant_ids = [100000000]

        response = self.send_post('nph/Intake', request_data=consent_json)

        self.assertEqual(len(response), len(current_participant_ids))
        self.assertTrue(all(int(obj['nph_participant_id']) in current_participant_ids for obj in response))

        # participant event activities
        participant_event_activities = self.nph_participant_activity_dao.get_all()

        for participant_id in current_participant_ids:
            current_participant_events = list(
                filter(lambda x: x.participant_id == participant_id, participant_event_activities)
            )
            activity_ids = [obj.activity_id for obj in current_participant_events]
            if participant_id == current_participant_ids[0]:
                # 4 enrollments 3 diet 1 consent 1 pairing
                self.assertEqual([1, 1, 1, 1, 6, 6, 6, 3, 2], activity_ids)

            self.assertTrue(all(obj.resource is not None for obj in current_participant_events))
            self.assertTrue(all(obj.resource.get('bundle_identifier') is not None for obj in
                                current_participant_events))

        # pairing events
        pairing_events = self.nph_pairing_event_dao.get_all()

        self.assertEqual(len(pairing_events), len(current_participant_ids))
        self.assertTrue(all(obj.event_type_id == 1 for obj in pairing_events))
        self.assertTrue(all(obj.site_id is not None for obj in pairing_events))
        self.assertTrue(all(obj.site_id == 2 for obj in pairing_events))
        self.assertTrue(all(obj.participant_id == current_participant_ids[0] for obj in pairing_events))

        pairing_participant_event_activity = list(
            filter(lambda x: x.activity_id == 2, participant_event_activities))
        pairing_participant_event_activity_ids = [obj.id for obj in pairing_participant_event_activity]
        self.assertTrue(all(obj.event_id in pairing_participant_event_activity_ids for obj in pairing_events))

        # enrollment events
        enrollment_events = self.nph_enrollment_event_dao.get_all()

        self.assertEqual(len(enrollment_events), 4)
        self.assertTrue(all(obj.participant_id == current_participant_ids[0] for obj in enrollment_events))

        enrollment_participant_event_activity = list(
            filter(lambda x: x.activity_id == 1, participant_event_activities))
        enrollment_participant_event_activity_ids = [obj.id for obj in enrollment_participant_event_activity]
        self.assertTrue(all(obj.event_id in enrollment_participant_event_activity_ids for obj in enrollment_events))

        # consent events
        consent_events = self.nph_consent_event_dao.get_all()

        # 1 for primary + 2 for opt-ins
        self.assertEqual(len(consent_events), 1)
        self.assertTrue(all(obj.participant_id == current_participant_ids[0] for obj in consent_events))
        # all permit
        self.assertTrue(all(obj.opt_in == ConsentOptInTypes.PERMIT for obj in consent_events))
        self.assertEqual([5], [obj.event_type_id for obj in consent_events])

        consent_participant_event_activity = list(
            filter(lambda x: x.activity_id == 3, participant_event_activities))
        consent_participant_event_activity_ids = [obj.id for obj in consent_participant_event_activity]
        self.assertTrue(all(obj.event_id in consent_participant_event_activity_ids for obj in consent_events))

        # diet events
        diet_events = self.nph_diet_event_dao.get_all()

        # should be 6 3 * 2 for each diet type
        self.assertEqual(len(diet_events), 6)
        self.assertTrue(all(obj.module is not None for obj in diet_events))
        self.assertTrue(all(obj.module == ModuleTypes.MODULE2 for obj in diet_events))
        self.assertTrue(all(obj.diet_id is not None for obj in diet_events))
        self.assertTrue(all(obj.event_authored_time is not None for obj in diet_events))
        self.assertTrue(all(obj.participant_id == current_participant_ids[0] for obj in diet_events))
        self.assertTrue(all(obj.status is not None for obj in diet_events))

        all_diet_types = [obj for obj in DietType if obj.name != 'LMT']
        self.assertTrue(all(obj.diet_name in all_diet_types for obj in diet_events))

        for diet_type in all_diet_types:
            current_diet_records = list(filter(lambda x: x.diet_name == diet_type, diet_events))
            self.assertEqual(len(current_diet_records), 2)

        # other events (should be null)
        self.assertTrue(self.nph_deactivation_event_dao.get_all() == [])
        self.assertTrue(self.nph_withdrawal_event_dao.get_all() == [])

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.pairing_event")
        self.clear_table_after_test("nph.enrollment_event")
        self.clear_table_after_test("nph.consent_event")
        self.clear_table_after_test("nph.withdrawal_event")
        self.clear_table_after_test("nph.deactivation_event")
        self.clear_table_after_test("nph.pairing_event_type")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.consent_event_type")
        self.clear_table_after_test("nph.enrollment_event_type")
        self.clear_table_after_test("nph.diet_event")
        self.clear_table_after_test("nph.participant_ops_data_element")
