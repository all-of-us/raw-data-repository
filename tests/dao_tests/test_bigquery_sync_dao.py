from datetime import datetime, timedelta
import json

from rdr_service.clock import FakeClock
from rdr_service import clock
from rdr_service.code_constants import *
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.site import Site
from rdr_service.participant_enums import WithdrawalAIANCeremonyStatus
from tests.test_data import load_measurement_json
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin


class BigQuerySyncDaoTest(BaseTestCase, PDRGeneratorTestMixin):
    TIME_1 = datetime(2018, 9, 20, 5, 49, 11)
    TIME_2 = datetime(2018, 9, 24, 14, 21, 1)
    TIME_3 = datetime(2018, 9, 25, 12, 25, 30)


    site = None
    hpo = None

    summary = None
    pm_json = None
    pm = None
    bio_order = None

    qn_thebasics_id = None
    qn_ehrconsent_id = None
    qn_dvehrconsent_id = None
    qn_lifestyle_id = None
    qn_overall_health_id = None
    qn_gror_id = None

    def setUp(self):
        super(BigQuerySyncDaoTest, self).setUp(with_consent_codes=True)

        self.dao = ParticipantDao()

        with self.dao.session() as session:
            self.site = session.query(Site).filter(Site.googleGroup == 'hpo-site-monroeville').first()
            self.hpo = session.query(HPO).filter(HPO.name == 'PITT').first()
            self.provider_link = {
                "primary": True, "organization": {"display": None, "reference": "Organization/PITT"}}

        with clock.FakeClock(self.TIME_1):
            self.participant = self.create_participant(self.provider_link)
            self.participant_id = int(self.participant['participantId'].replace('P', ''))
            self.biobank_id = int(self.participant['biobankId'].replace('Z', ''))


    def create_participant(self, provider_link=None):
        if provider_link:
            provider_link = {"providerLink": [provider_link]}
        else:
            provider_link = {}
        response = self.send_post("Participant", provider_link)
        return response

    def _submit_ehrconsent(self, participant_id, response_code=CONSENT_PERMISSION_YES_CODE, response_time=None):
        """ Submit the EHRConsent questionnaire """
        if not self.qn_ehrconsent_id:
            self.qn_ehrconsent_id = self.create_questionnaire("ehr_consent_questionnaire.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('ehrConsent', response_code))

        qr = self.make_questionnaire_response_json(participant_id, self.qn_ehrconsent_id,
                                                   code_answers=code_answers)
        with FakeClock(response_time or self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_sensitive_ehr(self, participant_id, response_data=dict(), response_time=None):
        if not self.qn_ehrconsent_id:
            self.qn_ehrconsent_id = self.create_questionnaire("ehr_consent_questionnaire.json")

        code_answers = list()
        for key, value in response_data.items():
            code_answers.append(self.make_code_answer(key, value))

        qr = self.make_questionnaire_response_json(participant_id, self.qn_ehrconsent_id,
                                                   code_answers=code_answers)
        with FakeClock(response_time or self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_ehrconsent_expired(self, participant_id, response_code=CONSENT_PERMISSION_NO_CODE, response_time=None):
        """ Submit the EHRConsent questionnaire """
        if not self.qn_ehrconsent_id:
            self.qn_ehrconsent_id = self.create_questionnaire("ehr_consent_questionnaire.json")

        code_answers = []
        code_answers.append(self.make_code_answer('ehrConsent', response_code))
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            self.qn_ehrconsent_id,
            string_answers=[['ehrConsentExpired', 'EHRConsentPII_ConsentExpired_Yes']],
            code_answers=code_answers,
            authored=response_time if response_time else self.TIME_1
        )

        with FakeClock(response_time or self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr_json)

    def _submit_dvehrconsent(self, participant_id, response_code=DVEHRSHARING_CONSENT_CODE_YES, response_time=None):
        """ Submit the DVEHRConsent questionnaire """
        if not self.qn_dvehrconsent_id:
            self.qn_dvehrconsent_id = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")

        code_answers = list()
        code_answers.append(self.make_code_answer(DVEHR_SHARING_QUESTION_CODE, response_code))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_dvehrconsent_id,
                                                   code_answers=code_answers)
        with FakeClock(response_time or self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_thebasics(self, participant_id):
        """ Submit the TheBasics questionnaire """
        if not self.qn_thebasics_id:
            self.qn_thebasics_id = self.create_questionnaire("questionnaire3.json")

        string_answers = list()
        string_answers.append(('firstName', 'John'))
        string_answers.append(('lastName', 'Doe'))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_thebasics_id,
                                                   string_answers=string_answers)
        with FakeClock(self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_lifestyle(self, participant_id):
        """ Submit the LifeStyle questionnaire """
        if not self.qn_lifestyle_id:
            self.qn_lifestyle_id = self.create_questionnaire("questionnaire4.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('state', UNSET))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_lifestyle_id,
                                                   code_answers=code_answers)
        with FakeClock(self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_overall_health(self, participant_id):
        """ Submit the OverallHealth questionnaire """
        if not self.qn_overall_health_id:
            self.qn_overall_health_id = self.create_questionnaire("questionnaire_overall_health.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('physicalHealth', UNSET))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_overall_health_id,
                                                   code_answers=code_answers)
        with FakeClock(self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_genomics_ror(self, participant_id, consent_response=CONSENT_GROR_YES_CODE, response_time=None):
        """ Submit the Genomics ROR questionnaire """
        if not self.qn_gror_id:
            self.qn_gror_id = self.create_questionnaire("consent_for_genomic_ror_question.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('genomic_consent', consent_response))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_gror_id, code_answers=code_answers)
        with FakeClock(response_time or self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _make_physical_measurements(self, **kwargs):
        """Makes a new PhysicalMeasurements (same values every time) with valid/complete defaults.
        Kwargs pass through to PM constructor, overriding defaults.
        """
        for k, default_value in (
            ('physicalMeasurementsId', 1),
            ('participantId', self.participant_id),

            ('createdSiteId', self.site.siteId),
            ('finalizedSiteId', self.site.siteId)):
            if k not in kwargs:
                kwargs[k] = default_value

        record = PhysicalMeasurements(**kwargs)
        PhysicalMeasurementsDao.store_record_fhir_doc(record, self.pm_json)
        return record

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

        Kwargs pass through to BiobankOrder constructor, overriding defaults.
        """
        for k, default_value in (
            ('biobankOrderId', '1'),
            ('created', clock.CLOCK.now()),
            ('participantId', self.participant_id),
            ('sourceSiteId', 1),
            ('sourceUsername', 'fred@pmi-ops.org'),
            ('collectedSiteId', 1),
            ('collectedUsername', 'joe@pmi-ops.org'),
            ('processedSiteId', 1),
            ('processedUsername', 'sue@pmi-ops.org'),
            ('finalizedSiteId', 2),
            ('finalizedUsername', 'bob@pmi-ops.org'),
            ('identifiers', [BiobankOrderIdentifier(system='https://www.pmi-ops.org', value='123456789')]),
            ('samples', [BiobankOrderedSample(
                biobankOrderId='1',
                test='1ED04',
                description=u'description',
                finalized=self.TIME_1,
                processingRequired=True)])):
            if k not in kwargs:
                kwargs[k] = default_value
        biobank_order = BiobankOrder(**kwargs)

        bss = BiobankStoredSample()
        bss.biobankId = self.biobank_id
        bss.test = '1ED04'
        bss.biobankOrderIdentifier = '123456789'
        bss.confirmed = self.TIME_2
        bss.created = self.TIME_2
        bss.biobankStoredSampleId = 'I11111111'
        bss.family_id = 'F11111111'

        with self.dao.session() as session:
            session.add(bss)

        return biobank_order

    def test_registered_participant_gen(self):
        """ Test a BigQuery after initial participant creation """
        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        self.assertEqual(ps_json['enrollment_status'], 'REGISTERED')

    def test_interested_participant_gen(self):
        """ Basic Participant Creation Test"""
        self.send_consent(self.participant_id)
        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        self.assertEqual(ps_json.get('sign_up_time', None),
                         self.TIME_1.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(ps_json.get('suspension_status', None), 'NOT_SUSPENDED')
        self.assertEqual(ps_json.get('withdrawn_status'), None, 'NOT_WITHDRAWN')
        self.assertEqual(ps_json.get('enrollment_status', None), 'PARTICIPANT')

    def test_member_participant_status(self):
        """ Member Participant Test"""
        # set up questionnaires to hit the calculate_max_core_sample_time in participant summary
        self.send_consent(self.participant_id)
        self._submit_ehrconsent(self.participant_id)
        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        self.assertEqual(ps_json['enrollment_status'], 'FULLY_CONSENTED')

    def _set_up_participant_data(self, fake_time=None, skip_ehr=False):
        # set up questionnaires to hit the calculate_max_core_sample_time in participant summary
        with clock.FakeClock(fake_time or self.TIME_1):
            self.send_consent(self.participant_id)
            if not skip_ehr:
                self._submit_ehrconsent(self.participant_id)
            self._submit_lifestyle(self.participant_id)
            self._submit_thebasics(self.participant_id)
            self._submit_overall_health(self.participant_id)

            self.pm_json = json.dumps(load_measurement_json(self.participant_id, self.TIME_2.isoformat()))
            self.pm = PhysicalMeasurementsDao().insert(self._make_physical_measurements())

            self.dao = BiobankOrderDao()
            self.bio_order = BiobankOrderDao().insert(
                self._make_biobank_order(participantId=self.participant_id))

    def test_full_participant_status(self):
        """ Full Participant Test"""
        self._set_up_participant_data()
        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        self.assertEqual('COHORT_2', ps_json['consent_cohort'], 'Test is built assuming cohort 2')
        self.assertEqual(ps_json['pm'][0]['pm_finalized_site'], 'hpo-site-monroeville')
        self.assertEqual(ps_json['pm'][0]['pm_status'], 'COMPLETED')
        self.assertEqual(ps_json['enrollment_status'], 'CORE_PARTICIPANT')

    def test_ehr_consent_expired_for_full_consent_participant(self):
        p_response = self.create_participant(self.provider_link)
        p_id = int(p_response['participantId'].replace('P', ''))
        self.send_consent(p_id, authored=self.TIME_1)
        self._submit_ehrconsent(p_id, response_time=self.TIME_1)

        ps_json = self.make_bq_participant_summary(p_id)
        self.assertIsNotNone(ps_json)
        self.assertEqual(ps_json['enrollment_status'], 'FULLY_CONSENTED')

        # send ehr consent expired response
        self._submit_ehrconsent_expired(p_id, response_time=self.TIME_2)
        ps_json = self.make_bq_participant_summary(p_id)
        self.assertIsNotNone(ps_json)
        # downgrade FULLY_CONSENTED to PARTICIPANT
        self.assertEqual(ps_json['enrollment_status'], 'PARTICIPANT')

    def test_ehr_consent_expired_for_core_participant(self):
        self._set_up_participant_data(fake_time=self.TIME_1)
        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertIsNotNone(ps_json)
        self.assertEqual('COHORT_2', ps_json['consent_cohort'], 'Test is built assuming cohort 2')
        self.assertEqual(ps_json['pm'][0]['pm_finalized_site'], 'hpo-site-monroeville')
        self.assertEqual(ps_json['pm'][0]['pm_status'], 'COMPLETED')
        self.assertEqual(ps_json['enrollment_status'], 'CORE_PARTICIPANT')

        # send ehr consent expired response
        self._submit_ehrconsent_expired(self.participant_id, response_time=self.TIME_3)
        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertIsNotNone(ps_json)
        # once CORE, always CORE
        self.assertEqual(ps_json['enrollment_status'], 'CORE_PARTICIPANT')

    def test_sensitive_ehr_consent_no(self):
        p_response = self.create_participant(self.provider_link)
        p_id = int(p_response['participantId'].replace('P', ''))
        self.send_consent(p_id, authored=self.TIME_1)
        # Participant opts out at ehrconsentpii_sensitivestype2 question
        self._submit_sensitive_ehr(p_id, response_time=self.TIME_1,
                                   response_data={
                                       'sensitiveEhrProceedToForm': 'readytoshare_yes',
                                       'sensitiveEhrAgreeToRelease': 'sensitivetype2__donotagree'
                                        })
        ps_json = self.make_bq_participant_summary(p_id)
        self.assertIsNotNone(ps_json['modules'])
        ehr_module_data = self.get_generated_items(ps_json['modules'], item_key='mod_module',
                                                   item_value='EHRConsentPII')
        self.assertEqual(ehr_module_data[0]['mod_status'], 'SUBMITTED_NO_CONSENT')
        self.assertEqual(ehr_module_data[0]['mod_consent_value'], 'sensitivetype2__donotagree')

    def test_sensitive_ehr_consent_yes(self):
        p_response = self.create_participant(self.provider_link)
        p_id = int(p_response['participantId'].replace('P', ''))
        self.send_consent(p_id, authored=self.TIME_1)
        self._submit_sensitive_ehr(p_id, response_time=self.TIME_1,
                                   response_data={
                                       'sensitiveEhrProceedToForm': 'readytoshare_yes',
                                       'sensitiveEhrAgreeToRelease': 'sensitivetype2__agree'
                                   })
        ps_json = self.make_bq_participant_summary(p_id)
        self.assertIsNotNone(ps_json['modules'])
        ehr_module_data = self.get_generated_items(ps_json['modules'], item_key='mod_module',
                                                   item_value='EHRConsentPII')
        self.assertEqual(ehr_module_data[0]['mod_status'], 'SUBMITTED')
        self.assertEqual(ehr_module_data[0]['mod_consent_value'], 'sensitivetype2__agree')

    def test_cohort_3_without_gror(self):
        self._set_up_participant_data(fake_time=datetime(2020, 6, 1))
        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        self.assertEqual('COHORT_3', ps_json['consent_cohort'], 'Test is built assuming cohort 3')
        self.assertEqual('FULLY_CONSENTED', ps_json['enrollment_status'])

    def test_cohort_3_with_gror(self):
        self._set_up_participant_data(fake_time=datetime(2020, 6, 1))
        self._submit_genomics_ror(self.participant_id)
        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        self.assertEqual('COHORT_3', ps_json['consent_cohort'], 'Test is built assuming cohort 3')
        self.assertEqual('CORE_PARTICIPANT', ps_json['enrollment_status'])

    def test_participant_stays_core(self):
        self._set_up_participant_data(fake_time=datetime(2020, 5, 1))
        self._submit_genomics_ror(self.participant_id,
                                  consent_response=CONSENT_GROR_YES_CODE,
                                  response_time=datetime(2020, 7, 1))

        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('COHORT_3', ps_json['consent_cohort'], 'Test is built assuming cohort 3')
        self.assertEqual('CORE_PARTICIPANT', ps_json['enrollment_status'],
                         'Test is built assuming participant starts as core')

        # Send an update to remove GROR consent and make sure participant is still CORE
        self._submit_genomics_ror(self.participant_id,
                                  consent_response=CONSENT_GROR_NO_CODE,
                                  response_time=datetime(2020, 9, 1))
        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('CORE_PARTICIPANT', ps_json['enrollment_status'])

        # This verifies the module submitted status from the participant generator data for each of the GROR modules
        # Also checks that an external id key/value pair exists (but value likely None for test data modules)
        gror_modules = self.get_generated_items(ps_json['modules'], item_key='mod_module', item_value='GROR',
                                                sort_key='mod_authored')
        self.assertIn('mod_external_id', gror_modules[0])
        self.assertEqual('SUBMITTED', gror_modules[0]['mod_status'])
        self.assertEqual('SUBMITTED_NO_CONSENT', gror_modules[1]['mod_status'])

    def test_previous_ehr_and_dv_ehr_reverted(self):
        # Scenario: a participant previously reached core participant status with EHR and DV EHR consent both YES
        # If EHR consent is changed to No, they should remain Core
        self._set_up_participant_data(skip_ehr=True)

        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('COHORT_2', ps_json['consent_cohort'],
                         'Test is built assuming cohort 2 (and that GROR consent is not required for Core status')
        self.assertNotEqual('CORE_PARTICIPANT', ps_json['enrollment_status'],
                            'Test is built assuming participant does not initialize as Core')

        # Get Core status through EHR consents
        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_YES_CODE,
                                response_time=datetime(2019, 2, 14))
        self._submit_dvehrconsent(self.participant_id, response_time=datetime(2019, 4, 1))
        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('CORE_PARTICIPANT', ps_json['enrollment_status'],
                         'Test is built assuming participant achieves Core status')

        # Send an update to remove EHR consent and make sure participant is still CORE
        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_NO_CODE,
                                response_time=datetime(2019, 7, 1))
        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('CORE_PARTICIPANT', ps_json['enrollment_status'])

        # This verifies the module submitted status from the participant generator data for ehr modules
        # Also checks that an external id key/value pair exists (but value likely None for test data modules)
        ehr_modules = self.get_generated_items(ps_json['modules'], item_key='mod_module', item_value='EHRConsentPII',
                                               sort_key="mod_authored")
        self.assertIn('mod_external_id',ehr_modules[0])
        self.assertEqual('SUBMITTED', ehr_modules[0]['mod_status'])
        self.assertEqual('SUBMITTED_NO_CONSENT', ehr_modules[1]['mod_status'])

    def test_no_on_ehr_overrides_yes_on_dv(self):
        # Scenario: a participant has had DV_EHR yes, but previously had a no on EHR.
        # No on EHR should supersede a yes on DV_EHR.
        self._set_up_participant_data(skip_ehr=True)

        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('COHORT_2', ps_json['consent_cohort'],
                         'Test is built assuming cohort 2 (and that GROR consent is not required for Core status')

        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_NO_CODE,
                                response_time=datetime(2019, 2, 14))
        self._submit_dvehrconsent(self.participant_id, response_time=datetime(2019, 4, 1))
        ps_json = self.make_bq_participant_summary(self.participant_id)
        self.assertEqual('PARTICIPANT', ps_json['enrollment_status'])

    def test_ehr_consent_expired_and_renewed(self):
        self._set_up_participant_data(fake_time=self.TIME_1)
        # send ehr consent expired response
        self._submit_ehrconsent_expired(self.participant_id, response_time=self.TIME_2)
        # send a new ehr consent (renewal/reconsent)
        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_YES_CODE,
                                response_time=self.TIME_3)

        ps_json = self.make_bq_participant_summary(self.participant_id)

        self.assertIsNotNone(ps_json)
        ehr_consents = self.get_generated_items(ps_json['consents'], item_key='consent_module',
                                                item_value='EHRConsentPII', sort_key='consent_module_authored')

        # Confirm a total of three EHR Consent responses
        self.assertEqual(len(ehr_consents), 3)
        # Verify the initial EHR consent details (sent by _set_up_participant_data)
        self.assertEqual(ehr_consents[0].get('consent_module_authored', None),
                         self.TIME_1.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(ehr_consents[0].get('consent_value', None), CONSENT_PERMISSION_YES_CODE)
        # This field should be None for consent payloads that don't contain the expiration hidden question code
        self.assertIsNone(ehr_consents[0].get('consent_expired', ''))

        # Verify the expired consent response details (contains the hidden expiration question code / answer value)
        self.assertEqual(ehr_consents[1].get('consent_module_authored', None),
                         self.TIME_2.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(ehr_consents[1].get('consent_value', None), CONSENT_PERMISSION_NO_CODE)
        self.assertEqual(ehr_consents[1].get('consent_expired', None), EHR_CONSENT_EXPIRED_YES)

        # Verify the last EHR consent renewal;  'consent_expired' value should not be carried forward from last consent
        self.assertEqual(ehr_consents[2].get('consent_module_authored', None),
                         self.TIME_3.strftime("%Y-%m-%dT%H:%M:%S"))
        self.assertEqual(ehr_consents[2].get('consent_value', None), CONSENT_PERMISSION_YES_CODE)
        # This field should be None for consent payloads that don't contain the expiration hidden question code
        self.assertIsNone(ehr_consents[2].get('consent_expired', ''))

    def test_ceremony_decision_fields(self):
        # Set up data for different scenarios of withdrawn participants
        # Clearing microseconds to avoid rounding time up in database and causing test to fail
        two_days_ago = datetime.today().replace(microsecond=0) - timedelta(days=2)
        withdrawal_reason_justification = 'testing withdrawal'
        no_ceremony_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.DECLINED,
            withdrawal_time=two_days_ago
        )
        ceremony_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.REQUESTED,
            withdrawal_time=two_days_ago
        )
        # Non-AIAN should not have been presented with a ceremony choice
        non_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=False,
            requests_ceremony=None,
            withdrawal_time=two_days_ago
        )

        ps_bqs_data = self.make_bq_participant_summary(no_ceremony_native_american_participant.participantId)
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.DECLINED))
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.DECLINED))

        ps_bqs_data = self.make_bq_participant_summary(ceremony_native_american_participant.participantId)
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.REQUESTED))
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.REQUESTED))

        ps_bqs_data = self.make_bq_participant_summary(non_native_american_participant.participantId)
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.UNSET))
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.UNSET))
