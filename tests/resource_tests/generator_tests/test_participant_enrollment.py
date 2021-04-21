import json
from datetime import datetime

from rdr_service import clock
from rdr_service.code_constants import *
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.code import CodeType
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.site import Site
from tests.helpers.unittest_base import BaseTestCase, BiobankTestMixin, PDRGeneratorTestMixin
from tests.test_data import load_measurement_json

class ParticipantEnrollmentTest(BaseTestCase, BiobankTestMixin, PDRGeneratorTestMixin):
    TIME_1 = datetime(2018, 9, 20, 5, 49, 11)
    TIME_2 = datetime(2018, 9, 24, 14, 21, 1)

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
        super(ParticipantEnrollmentTest, self).setUp(with_consent_codes=True)

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
        self.create_code_if_needed(response_code, CodeType.ANSWER)

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_ehrconsent_id,
                                                   code_answers=code_answers)
        with clock.FakeClock(response_time or self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_dvehrconsent(self, participant_id, response_code=DVEHRSHARING_CONSENT_CODE_YES, response_time=None):
        """ Submit the DVEHRConsent questionnaire """
        if not self.qn_dvehrconsent_id:
            self.qn_dvehrconsent_id = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")

        code_answers = list()
        code_answers.append(self.make_code_answer(DVEHR_SHARING_QUESTION_CODE, response_code))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_dvehrconsent_id,
                                                   code_answers=code_answers)
        with clock.FakeClock(response_time or self.TIME_1):
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
        with clock.FakeClock(self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_lifestyle(self, participant_id):
        """ Submit the LifeStyle questionnaire """
        if not self.qn_lifestyle_id:
            self.qn_lifestyle_id = self.create_questionnaire("questionnaire4.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('state', UNSET))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_lifestyle_id,
                                                   code_answers=code_answers)
        with clock.FakeClock(self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_overall_health(self, participant_id):
        """ Submit the OverallHealth questionnaire """
        if not self.qn_overall_health_id:
            self.qn_overall_health_id = self.create_questionnaire("questionnaire_overall_health.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('physicalHealth', UNSET))

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_overall_health_id,
                                                   code_answers=code_answers)
        with clock.FakeClock(self.TIME_1):
            self.send_post(f"Participant/P{participant_id}/QuestionnaireResponse", qr)

    def _submit_genomics_ror(self, participant_id, consent_response=CONSENT_GROR_YES_CODE, response_time=None):
        """ Submit the Genomics ROR questionnaire """
        if not self.qn_gror_id:
            self.qn_gror_id = self.create_questionnaire("consent_for_genomic_ror_question.json")

        code_answers = list()
        code_answers.append(self.make_code_answer('genomic_consent', consent_response))
        self.create_code_if_needed(consent_response, CodeType.ANSWER)

        qr = self.make_questionnaire_response_json(self.participant_id, self.qn_gror_id, code_answers=code_answers)
        with clock.FakeClock(response_time or self.TIME_1):
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

    def _set_up_participant_data(self, fake_time=None, skip_ehr=False):
        # set up questionnaires to hit the calculate_max_core_sample_time in participant summary
        with clock.FakeClock(fake_time or self.TIME_2):
            self.send_consent(self.participant_id)
            if not skip_ehr:
                self._submit_ehrconsent(self.participant_id)
            self._submit_lifestyle(self.participant_id)
            self._submit_thebasics(self.participant_id)
            self._submit_overall_health(self.participant_id)

            self.pm_json = json.dumps(load_measurement_json(self.participant_id, self.TIME_1.isoformat()))
            self.pm = PhysicalMeasurementsDao().insert(self._make_physical_measurements())

            self._make_default_biobank_order(self.participant_id)


    def test_full_participant_status(self):
        """ Full Participant Test"""
        self._set_up_participant_data()
        ps_data = self.make_participant_resource(self.participant_id)

        self.assertIsNotNone(ps_data)
        self.assertEqual('COHORT_2', ps_data['consent_cohort'], 'Test is built assuming cohort 2')
        self.assertEqual(ps_data['pm'][0]['finalized_site'], 'hpo-site-monroeville')
        self.assertEqual(ps_data['pm'][0]['status'], 'COMPLETED')
        self.assertEqual(ps_data['enrollment_status'], 'CORE_PARTICIPANT')
        self.assertEqual(ps_data['ehr_status'], 'NOT_PRESENT')
        self.assertIsNone(ps_data['ehr_receipt'])
        self.assertIsNone(ps_data['ehr_update'])

    def test_cohort_3_without_gror(self):
        self._set_up_participant_data(fake_time=datetime(2020, 6, 1))
        ps_data = self.make_participant_resource(self.participant_id)

        self.assertIsNotNone(ps_data)
        self.assertEqual('COHORT_3', ps_data['consent_cohort'], 'Test is built assuming cohort 3')
        self.assertEqual('FULLY_CONSENTED', ps_data['enrollment_status'])

    def test_cohort_3_with_gror(self):
        self._set_up_participant_data(fake_time=datetime(2020, 6, 1))
        self._submit_genomics_ror(self.participant_id)

        ps_data = self.make_participant_resource(self.participant_id)
        self.assertIsNotNone(ps_data)
        self.assertEqual('COHORT_3', ps_data['consent_cohort'], 'Test is built assuming cohort 3')
        self.assertEqual('CORE_PARTICIPANT', ps_data['enrollment_status'])

    def test_participant_stays_core(self):
        self._set_up_participant_data(fake_time=datetime(2020, 5, 1))
        self._submit_genomics_ror(self.participant_id,
                                  consent_response=CONSENT_GROR_YES_CODE,
                                  response_time=datetime(2020, 7, 1))

        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('COHORT_3', ps_data['consent_cohort'], 'Test is built assuming cohort 3')
        self.assertEqual('CORE_PARTICIPANT', ps_data['enrollment_status'],
                         'Test is built assuming participant starts as core')

        # Send an update to remove GROR consent and make sure participant is still CORE
        self._submit_genomics_ror(self.participant_id,
                                  consent_response=CONSENT_GROR_NO_CODE,
                                  response_time=datetime(2020, 9, 1))
        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('CORE_PARTICIPANT', ps_data['enrollment_status'])

        # This verifies the module submitted status from the participant generator data for each of the GROR modules
        gror_modules = self.get_generated_items(ps_data['modules'], item_key='module', item_value='GROR',
                                                sort_key='module_authored')
        self.assertEqual('SUBMITTED', gror_modules[0]['status'])
        self.assertEqual('SUBMITTED_NO_CONSENT', gror_modules[1]['status'])


    def test_previous_ehr_and_dv_ehr_reverted(self):
        # Scenario: a participant previously had their EHR consent YES, and their DV_EHR as YES.
        # If EHR consent is changed to No, they should remain Core
        self._set_up_participant_data(skip_ehr=True)

        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('COHORT_2', ps_data['consent_cohort'],
                         'Test is built assuming cohort 2 (and that GROR consent is not required for Core status')
        self.assertNotEqual('CORE_PARTICIPANT', ps_data['enrollment_status'],
                            'Test is built assuming participant does not initialize as Core')

        # Get Core status through EHR consents
        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_YES_CODE,
                                response_time=datetime(2019, 2, 14))
        self._submit_dvehrconsent(self.participant_id, response_time=datetime(2019, 4, 1))
        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('CORE_PARTICIPANT', ps_data['enrollment_status'],
                         'Test is built assuming participant achieves Core status')

        # Send an update to remove EHR consent and make sure participant is still CORE
        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_NO_CODE,
                                response_time=datetime(2019, 7, 1))
        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('CORE_PARTICIPANT', ps_data['enrollment_status'])

        # This checks the module submitted status for each of the EHR consent module responses
        ehr_modules = self.get_generated_items(ps_data['modules'], item_key='module', item_value='EHRConsentPII',
                                               sort_key='module_authored')
        self.assertEqual('SUBMITTED', ehr_modules[0]['status'])
        self.assertEqual('SUBMITTED_NO_CONSENT', ehr_modules[1]['status'])

    def test_no_on_ehr_overrides_yes_on_dv(self):
        # Scenario: a participant has had DV_EHR yes, but previously had a no on EHR.
        # No on EHR should supersede a yes on DV_EHR.
        self._set_up_participant_data(skip_ehr=True)

        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('COHORT_2', ps_data['consent_cohort'],
                         'Test is built assuming cohort 2 (and that GROR consent is not required for Core status')

        self._submit_ehrconsent(self.participant_id,
                                response_code=CONSENT_PERMISSION_NO_CODE,
                                response_time=datetime(2019, 2, 14))
        self._submit_dvehrconsent(self.participant_id, response_time=datetime(2019, 4, 1))
        ps_data = self.make_participant_resource(self.participant_id)
        self.assertEqual('PARTICIPANT', ps_data['enrollment_status'])
