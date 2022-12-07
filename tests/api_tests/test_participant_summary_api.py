import datetime
import faker
import http.client
import mock
import threading
import unittest

from copy import deepcopy
from mock import patch
from urllib.parse import urlencode

from rdr_service import config, main
from rdr_service.api_util import PTC, CURATION, HEALTHPRO
from rdr_service.clock import FakeClock
from rdr_service.code_constants import (CONSENT_PERMISSION_NO_CODE, CONSENT_PERMISSION_YES_CODE,
                                        DVEHRSHARING_CONSENT_CODE_NO, DVEHRSHARING_CONSENT_CODE_NOT_SURE,
                                        DVEHRSHARING_CONSENT_CODE_YES, GENDER_MAN_CODE, GENDER_NONBINARY_CODE,
                                        GENDER_PREFER_NOT_TO_ANSWER_CODE, GENDER_WOMAN_CODE, PMI_SKIP_CODE, PPI_SYSTEM,
                                        RACE_NONE_OF_THESE_CODE, RACE_WHITE_CODE, UNSET)
from rdr_service.concepts import Concept
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import CodeType
from rdr_service.model.config_utils import from_client_biobank_id
from rdr_service.model.consent_file import ConsentType
from rdr_service.model.enrollment_status_history import EnrollmentStatusHistory
from rdr_service.model.hpo import HPO
from rdr_service.model.utils import from_client_participant_id
from rdr_service.participant_enums import (
    ANSWER_CODE_TO_GENDER, ANSWER_CODE_TO_RACE, OrganizationType,
    TEST_HPO_ID, TEST_HPO_NAME, QuestionnaireStatus, EhrStatus)
from tests.test_data import load_biobank_order_json, load_measurement_json, to_client_participant_id,\
    load_remote_measurement_json
from tests.helpers.unittest_base import BaseTestCase

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)
TIME_5 = datetime.datetime(2016, 1, 5, 0, 1)
TIME_6 = datetime.datetime(2015, 1, 1)

participant_summary_default_values = {
    "ageRange": "UNSET",
    "race": "PMI_Skip",
    "hpoId": "UNSET",
    "awardee": "UNSET",
    "site": "UNSET",
    "organization": "UNSET",
    "education": "PMI_Skip",
    "income": "PMI_Skip",
    "language": "UNSET",
    "primaryLanguage": "UNSET",
    "sex": "PMI_Skip",
    "sexualOrientation": "PMI_Skip",
    "state": "UNSET",
    "recontactMethod": "UNSET",
    "enrollmentStatus": "INTERESTED",
    "enrollmentStatusV3_0": "PARTICIPANT",
    "enrollmentStatusV3_1": "PARTICIPANT",
    "samplesToIsolateDNA": "UNSET",
    "numBaselineSamplesArrived": 0,
    "numCompletedPPIModules": 1,
    "numCompletedBaselinePPIModules": 1,
    "clinicPhysicalMeasurementsStatus": "UNSET",
    "selfReportedPhysicalMeasurementsStatus": "UNSET",
    "physicalMeasurementsStatus": "UNSET",
    "physicalMeasurementsCollectType": "UNSET",
    "physicalMeasurementsCreatedSite": "UNSET",
    "physicalMeasurementsFinalizedSite": "UNSET",
    "consentForGenomicsROR": "UNSET",
    "consentForDvElectronicHealthRecordsSharing": "UNSET",
    "consentForElectronicHealthRecords": "UNSET",
    "consentForStudyEnrollment": "SUBMITTED",
    "consentForCABoR": "UNSET",
    "consentForEtM": "UNSET",
    "questionnaireOnFamilyHealth": "UNSET",
    "questionnaireOnHealthcareAccess": "UNSET",
    "questionnaireOnMedicalHistory": "UNSET",
    "questionnaireOnMedications": "UNSET",
    "questionnaireOnOverallHealth": "UNSET",
    "questionnaireOnSocialDeterminantsOfHealth": "UNSET",
    "questionnaireOnPersonalAndFamilyHealthHistory": "UNSET",
    "questionnaireOnLifestyle": "UNSET",
    "questionnaireOnTheBasics": "SUBMITTED",
    "questionnaireOnCopeMay": "UNSET",
    "questionnaireOnCopeJune": "UNSET",
    "questionnaireOnCopeJuly": "UNSET",
    "questionnaireOnCopeNov": "UNSET",
    "questionnaireOnCopeDec": "UNSET",
    "questionnaireOnCopeFeb": "UNSET",
    "questionnaireOnDnaProgram": "UNSET",
    "biospecimenCollectedSite": "UNSET",
    "biospecimenFinalizedSite": "UNSET",
    "biospecimenProcessedSite": "UNSET",
    "biospecimenSourceSite": "UNSET",
    "clinicPhysicalMeasurementsCreatedSite": "UNSET",
    "clinicPhysicalMeasurementsFinalizedSite": "UNSET",
    "biospecimenStatus": "UNSET",
    "sampleOrderStatus1ED04": "UNSET",
    "sampleOrderStatus1ED10": "UNSET",
    "sampleOrderStatus1HEP4": "UNSET",
    "sampleOrderStatus1PST8": "UNSET",
    "sampleOrderStatus1PS08": "UNSET",
    "sampleOrderStatus2PST8": "UNSET",
    "sampleOrderStatus1SAL": "UNSET",
    "sampleOrderStatus1SAL2": "UNSET",
    "sampleOrderStatus1SST8": "UNSET",
    "sampleOrderStatus2SST8": "UNSET",
    "sampleOrderStatus1SS08": "UNSET",
    "sampleOrderStatus1UR10": "UNSET",
    "sampleOrderStatus1UR90": "UNSET",
    "sampleOrderStatus2ED10": "UNSET",
    "sampleOrderStatus1CFD9": "UNSET",
    "sampleOrderStatus1PXR2": "UNSET",
    "sampleOrderStatus1ED02": "UNSET",
    "sampleOrderStatusDV1SAL2": "UNSET",
    "sampleStatus1ED04": "UNSET",
    "sampleStatus1ED10": "UNSET",
    "sampleStatus1HEP4": "UNSET",
    "sampleStatus1PST8": "UNSET",
    "sampleStatus2PST8": "UNSET",
    "sampleStatus1PS08": "UNSET",
    "sampleStatus1SAL": "UNSET",
    "sampleStatus1SAL2": "UNSET",
    "sampleStatus1SST8": "UNSET",
    "sampleStatus2SST8": "UNSET",
    "sampleStatus1SS08": "UNSET",
    "sampleStatus1UR10": "UNSET",
    "sampleStatus1UR90": "UNSET",
    "sampleStatus2ED10": "UNSET",
    "sampleStatus1CFD9": "UNSET",
    "sampleStatus1ED02": "UNSET",
    "sampleStatus1PXR2": "UNSET",
    "sampleStatusDV1SAL2": "UNSET",
    "withdrawalStatus": "NOT_WITHDRAWN",
    "withdrawalReason": "UNSET",
    "suspensionStatus": "NOT_SUSPENDED",
    "numberDistinctVisits": 0,
    "ehrStatus": "UNSET",
    "healthDataStreamSharingStatusV3_1": "NEVER_SHARED",
    "ehrConsentExpireStatus": "UNSET",
    "patientStatus": [],
    "participantOrigin": 'example',
    "semanticVersionForPrimaryConsent": "v1",
    "deceasedStatus": "UNSET",
    "retentionEligibleStatus": "UNSET",
    "retentionType": "UNSET",
    "enrollmentSite": "UNSET",
    "sample1SAL2CollectionMethod": "UNSET",
    "isEhrDataAvailable": False,
    "wasEhrDataAvailable": False,
    "questionnaireOnCopeVaccineMinute1": "UNSET",
    "questionnaireOnCopeVaccineMinute2": "UNSET",
    "questionnaireOnCopeVaccineMinute3": "UNSET",
    "questionnaireOnCopeVaccineMinute4": "UNSET",
    "onsiteIdVerificationType": "UNSET",
    "onsiteIdVerificationVisitType": "UNSET",
    "questionnaireOnLifeFunctioning": "UNSET",
    "aian": False
}

participant_summary_default_values_no_basics = dict(participant_summary_default_values)
participant_summary_default_values_no_basics.update({
    "questionnaireOnTheBasics": "UNSET",
    "race": "UNSET",
    "education": "UNSET",
    "income": "UNSET",
    "sex": "UNSET",
    "sexualOrientation": "UNSET"
})


def _add_code_answer(code_answers, link_id, code):
    if code:
        code_answers.append((link_id, Concept(PPI_SYSTEM, code)))


def _make_entry(ps):
    return {"fullUrl": "http://localhost/rdr/v1/Participant/%s/Summary" % ps["participantId"], "resource": ps}


class ParticipantSummaryMySqlApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.provider_link = {
            "primary": True,
            "organization": {"display": None, "reference": "Organization/PITT"},
            "site": [{"display": None, "reference": "mayo-clinic"}],
            "identifier": [{"system": "http://any-columbia-mrn-system", "value": "MRN456"}],
        }

        # Patching to prevent consent validation checks from running
        build_validator_patch = mock.patch(
            'rdr_service.services.consent.validation.ConsentValidationController.build_controller'
        )
        build_validator_patch.start()
        self.addCleanup(build_validator_patch.stop)

    def testUpdate_raceCondition(self):
        self.create_questionnaire("questionnaire3.json")
        participant = self.send_post("Participant", {})
        participant_id = participant["participantId"]
        participant["providerLink"] = [self.provider_link]

        t1 = threading.Thread(
            target=lambda: self.send_put(
                "Participant/%s" % participant_id, participant, headers={"If-Match": participant["meta"]["versionId"]}
            )
        )

        t2 = threading.Thread(target=lambda: self.send_consent(participant_id))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # The participant summary should exist (consent has been received), and it should have PITT
        # for its HPO ID (the participant update occurred.)
        # This used to fail a decent percentage of the time, before we started using FOR UPDATE in
        # our update statements; see DA-256.
        ps = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual("PITT", ps.get("hpoId"))


class ParticipantSummaryApiTest(BaseTestCase):
    provider_link = {"primary": True, "organization": {"display": None, "reference": "Organization/PITT"}}
    az_provider_link = {"primary": True, "organization": {"display": None, "reference": "Organization/AZ_TUCSON"}}
    # Some link ids relevant to the demographics questionnaire
    code_link_ids = (
        "race",
        "genderIdentity",
        "state",
        "sex",
        "sexualOrientation",
        "recontactMethod",
        "language",
        "education",
        "income",
    )
    string_link_ids = ("firstName", "middleName", "lastName", "streetAddress",
                       "streetAddress2", "city", "phoneNumber", "zipCode")

    def setUp(self):
        super().setUp()
        self.faker = faker.Faker()
        self.hpo_dao = HPODao()
        self.ps_dao = ParticipantSummaryDao()
        self.site_dao = SiteDao()
        # Needed by test_switch_to_test_account
        self.hpo_dao.insert(
            HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName="Test", organizationType=OrganizationType.UNSET)
        )

        # Patching to prevent consent validation checks from running
        build_validator_patch = mock.patch(
            'rdr_service.services.consent.validation.ConsentValidationController.build_controller'
        )
        build_validator_patch.start()
        self.addCleanup(build_validator_patch.stop)

    def overwrite_test_user_awardee(self, awardee, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        new_user_info['example@example.com']['awardee'] = awardee
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def overwrite_test_user_roles(self, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def overwrite_test_user_site(self, site):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['site'] = site
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def create_demographics_questionnaire(self):
        """Uses the demographics test data questionnaire.  Returns the questionnaire id"""
        return self.create_questionnaire("questionnaire3.json")

    def create_expected_response(self, participant, answers, consent_language=None, patient_statuses=None):
        """Generates what we should expect as the return value of the participant summary API after the
    given participant has submitted the given answers to the questionnaire generated by
    `create_demographics_questionnaire`.
    """
        # Remove the signature field if it exists
        answers.pop("CABoRSignature", None)
        # Copy and mutate the copy, not the original answer dict
        expected = dict(participant_summary_default_values)
        expected.update(answers)
        # These properties are mutated in between sending and retrieving, they require special handling
        if "dateOfBirth" in answers:
            dob = answers["dateOfBirth"]
            expected["dateOfBirth"] = "{}-{:02d}-{:02d}".format(dob.year, dob.month, dob.day)
        if "race" in answers:
            expected["race"] = str(ANSWER_CODE_TO_RACE.get(answers["race"]))

        if "genderIdentity" in answers:
            expected["genderIdentity"] = str(ANSWER_CODE_TO_GENDER.get(answers["genderIdentity"]))

        if consent_language:
            expected.update({"primaryLanguage": consent_language})
        else:
            expected.update({"primaryLanguage": "UNSET"})

        if patient_statuses:
            expected["patientStatus"] = patient_statuses

        expected.update(
            {
                "enrollmentStatus": "INTERESTED",
                "signUpTime": participant["signUpTime"],
                "biobankId": participant["biobankId"],
                "questionnaireOnTheBasicsTime": TIME_1.isoformat(),
                "questionnaireOnTheBasicsAuthored": TIME_1.isoformat(),
                "consentForCABoR": "SUBMITTED",
                "consentForCABoRTime": TIME_1.isoformat(),
                "consentForCABoRAuthored": TIME_1.isoformat(),
                "participantId": participant["participantId"],
                "hpoId": "PITT",
                "awardee": "PITT",
                "consentForStudyEnrollmentTime": TIME_1.isoformat(),
                "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
                "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
                "ageRange": "35-44",
                "email": self.email,
                "consentCohort": "COHORT_1",
                "cohort2PilotFlag": "UNSET",
                "patientStatus": patient_statuses or [],
                "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
                "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
            }
        )

        return expected

    def post_demographics_questionnaire(
        self, participant_id, questionnaire_id, cabor_signature_string=False, time=TIME_1, **kwargs
    ):
        """POSTs answers to the demographics questionnaire for the participant"""
        answers = {
            "code_answers": [],
            "string_answers": [],
            "date_answers": [("dateOfBirth", kwargs.get("dateOfBirth"))],
        }
        if cabor_signature_string:
            answers["string_answers"].append(("CABoRSignature", kwargs.get("CABoRSignature")))
        else:
            answers["uri_answers"] = [("CABoRSignature", kwargs.get("CABoRSignature"))]

        for link_id in self.code_link_ids:
            if link_id in kwargs:
                concept = Concept(PPI_SYSTEM, kwargs[link_id])
                answers["code_answers"].append((link_id, concept))

        for link_id in self.string_link_ids:
            code = kwargs.get(link_id)
            answers["string_answers"].append((link_id, code))

        response_data = self.make_questionnaire_response_json(participant_id, questionnaire_id, **answers)

        with FakeClock(time):
            url = "Participant/%s/QuestionnaireResponse" % participant_id
            return self.send_post(url, request_data=response_data)

    def test_modified_api(self):

        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": PMI_SKIP_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }
        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        last_modified = summary["lastModified"]

        results = self.send_get("ParticipantSummary/Modified")
        self.assertEqual(len(results), 1)

        rec = results[0]
        self.assertEqual(participant_id, rec["participantId"])
        self.assertEqual(last_modified, rec["lastModified"])

        results = self.send_get("ParticipantSummary/Modified?awardee=PITT")

        rec = results[0]
        self.assertEqual(participant_id, rec["participantId"])
        self.assertEqual(last_modified, rec["lastModified"])

    def test_check_login(self):
        participant_one = self.data_generator.create_database_participant()
        participant_two = self.data_generator.create_database_participant()
        participant_two.withdrawalStatus = 2

        participant_summary_one = self.data_generator \
            .create_database_participant_summary(participant=participant_one)
        participant_summary_one.loginPhoneNumber = '444-123-4567'
        participant_summary_one.email = self.fake.email()
        self.ps_dao.update(participant_summary_one)

        participant_summary_two = self.data_generator \
            .create_database_participant_summary(participant=participant_two)

        one_real_email_result = self.send_post("ParticipantSummary/CheckLogin",
                                               {"email": participant_summary_one.email})
        one_real_phone_result = self.send_post("ParticipantSummary/CheckLogin",
                                               {"login_phone_number": participant_summary_one.loginPhoneNumber})
        one_real_combo_result = self.send_post("ParticipantSummary/CheckLogin",
                                               {"email": participant_summary_one.email,
                                                "login_phone_number": participant_summary_one.loginPhoneNumber})

        two_real_email_result = self.send_post("ParticipantSummary/CheckLogin",
                                               {"email": participant_summary_two.email})
        two_real_phone_result = self.send_post("ParticipantSummary/CheckLogin",
                                               {"login_phone_number": participant_summary_two.loginPhoneNumber})
        two_real_combo_result = self.send_post("ParticipantSummary/CheckLogin",
                                               {"email": participant_summary_two.email,
                                                "login_phone_number": participant_summary_two.loginPhoneNumber})

        self.assertEqual(len(one_real_email_result), 1)
        self.assertEqual(len(one_real_phone_result), 1)
        self.assertEqual(len(one_real_combo_result), 1)

        self.assertEqual(one_real_email_result['status'], 'IN_USE')
        self.assertEqual(one_real_phone_result['status'], 'IN_USE')
        self.assertEqual(one_real_combo_result['status'], 'IN_USE')

        self.assertEqual(len(two_real_email_result), 1)
        self.assertEqual(len(two_real_phone_result), 1)
        self.assertEqual(len(two_real_combo_result), 1)

        self.assertEqual(two_real_email_result['status'], 'NOT_IN_USE')
        self.assertEqual(two_real_phone_result['status'], 'NOT_IN_USE')
        self.assertEqual(two_real_combo_result['status'], 'NOT_IN_USE')

        fake_email = self.fake.email()
        fake_phone = '123-456-7890'
        fake_first_name = self.fake.first_name()
        fake_last_name = self.fake.last_name()
        fake_street_address = self.fake.street_address()

        fake_email_result = self.send_post("ParticipantSummary/CheckLogin",
                                           {"email": fake_email})
        fake_phone_result = self.send_post("ParticipantSummary/CheckLogin",
                                           {"login_phone_number": fake_phone})
        fake_combo_result = self.send_post("ParticipantSummary/CheckLogin",
                                           {"email": fake_email,
                                            "login_phone_number": fake_phone})

        self.assertEqual(fake_email_result['status'], 'NOT_IN_USE')
        self.assertEqual(fake_phone_result['status'], 'NOT_IN_USE')
        self.assertEqual(fake_combo_result['status'], 'NOT_IN_USE')

        bad_key_email_result = self.send_post("ParticipantSummary/CheckLogin",
                                              {"bad_email_key": fake_email},
                                              expected_status=http.client.BAD_REQUEST)
        bad_key_phone_result = self.send_post("ParticipantSummary/CheckLogin",
                                              {"bad_phone_key": fake_phone},
                                              expected_status=http.client.BAD_REQUEST)
        null_key_result = self.send_post("ParticipantSummary/CheckLogin",
                                         expected_status=http.client.BAD_REQUEST)

        self.assertEqual(bad_key_email_result.status_code, 400)
        self.assertEqual(bad_key_phone_result.status_code, 400)
        self.assertEqual(null_key_result.status_code, 400)

        self.assertEqual(bad_key_email_result.json['message'],
                         'Only email or login_phone_number are allowed in request')
        self.assertEqual(bad_key_phone_result.json['message'],
                         'Only email or login_phone_number are allowed in request')

        not_allowed_key = self.send_post("ParticipantSummary/CheckLogin",
                                         {"first_name": fake_first_name,
                                          "last_name": fake_last_name,
                                          "email": fake_email},
                                         expected_status=http.client.BAD_REQUEST)
        another_not_allowed_key = self.send_post("ParticipantSummary/CheckLogin",
                                                 {"street_address": fake_street_address},
                                                 expected_status=http.client.BAD_REQUEST)

        self.assertEqual(not_allowed_key.status_code, 400)
        self.assertEqual(another_not_allowed_key.status_code, 400)

        self.assertEqual(not_allowed_key.json['message'],
                         'Only email or login_phone_number are allowed in request')
        self.assertEqual(another_not_allowed_key.json['message'],
                         'Only email or login_phone_number are allowed in request')

        null_email_result = self.send_post("ParticipantSummary/CheckLogin",
                                           {"email": ''},
                                           expected_status=http.client.BAD_REQUEST)
        null_phone_result = self.send_post("ParticipantSummary/CheckLogin",
                                           {"login_phone_number": ''},
                                           expected_status=http.client.BAD_REQUEST)
        null_combo_result = self.send_post("ParticipantSummary/CheckLogin",
                                           {"email": '',
                                            "login_phone_number": ''},
                                           expected_status=http.client.BAD_REQUEST)

        self.assertEqual(null_email_result.status_code, 400)
        self.assertEqual(null_phone_result.status_code, 400)
        self.assertEqual(null_combo_result.status_code, 400)

        self.assertEqual(null_email_result.json['message'],
                         'Missing email or login_phone_number in request')

    def test_invalid_filters_return(self):
        num_summary = 10
        first_name = "Testy"
        for num in range(num_summary):
            if num == 1:
                self.data_generator \
                    .create_database_participant_summary(
                    firstName=first_name,
                    lastName="Tester"
                )
            else:
                self.data_generator \
                    .create_database_participant_summary()

        response_good_bad_filter = self.send_get(f"ParticipantSummary?foobarbaz=1&firstName={first_name}")
        self.assertEqual(len(response_good_bad_filter['entry']), 1)
        resource = response_good_bad_filter['entry'][0]['resource']
        self.assertEqual(resource['firstName'], 'Testy')
        self.assertEqual(resource['lastName'], 'Tester')

        response_good_filter = self.send_get(f"ParticipantSummary?firstName={first_name}")
        self.assertEqual(len(response_good_filter['entry']), 1)
        resource = response_good_filter['entry'][0]['resource']
        self.assertEqual(resource['firstName'], 'Testy')
        self.assertEqual(resource['lastName'], 'Tester')

        response_bad_filter = self.send_get(
            "ParticipantSummary?foobarbaz=1",
            expected_status=http.client.BAD_REQUEST
        )
        self.assertEqual(response_bad_filter.status_code, 400)
        self.assertEqual(response_bad_filter.json['message'], 'No valid fields were provided')

        response_no_filter = self.send_get("ParticipantSummary")
        self.assertEqual(len(response_no_filter['entry']), num_summary)

    def test_access_with_curation_role(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)

        self.overwrite_test_user_roles([CURATION])
        response = self.send_get("ParticipantSummary")
        self.assertEqual(len(response['entry']), 1)

    def test_constraints_dob_and_lastname(self):
        num_summary = 3
        _date = datetime.date(1978, 10, 9)
        last_name = "Tester_1"

        for num in range(num_summary):
            self.data_generator \
                .create_database_participant_summary(
                firstName=f"Testy_{num}",
                lastName=f"Tester_{num}",
                dateOfBirth=_date,
            )

        response_only_dob = self.send_get(
            f"ParticipantSummary?dateOfBirth={_date}",
            expected_status=http.client.BAD_REQUEST
        )
        self.assertEqual(response_only_dob.status_code, 400)
        self.assertEqual(response_only_dob.json['message'], 'Argument lastName is required with dateOfBirth')

        response_only_last_name = self.send_get(
            f"ParticipantSummary?lastName={last_name}",
            expected_status=http.client.BAD_REQUEST
        )
        self.assertEqual(response_only_last_name.status_code, 400)
        self.assertEqual(response_only_last_name.json['message'], 'Argument dateOfBirth is required with lastName')

        response_dob_last_name = self.send_get(f"ParticipantSummary?dateOfBirth={_date}&lastName={last_name}")
        self.assertEqual(len(response_dob_last_name['entry']), 1)
        resource = response_dob_last_name['entry'][0]['resource']
        self.assertEqual(resource['lastName'], last_name)
        self.assertEqual(resource['dateOfBirth'], '1978-10-09')

        response_no_filter = self.send_get("ParticipantSummary")
        self.assertEqual(len(response_no_filter['entry']), num_summary)

        response_only_dob_correct_role_and_awardee = self.send_get(
            f"ParticipantSummary?hpoId=UNSET&dateOfBirth={_date}")
        self.assertEqual(len(response_only_dob_correct_role_and_awardee['entry']), 3)
        resource = response_only_dob_correct_role_and_awardee['entry'][0]['resource']
        self.assertEqual(resource['dateOfBirth'], '1978-10-09')
        self.assertEqual(resource['hpoId'], 'UNSET')

        response_only_lastname_correct_role_and_awardee = self.send_get(
            f"ParticipantSummary?hpoId=UNSET&lastName={last_name}")
        self.assertEqual(len(response_only_lastname_correct_role_and_awardee['entry']), 1)
        resource = response_only_lastname_correct_role_and_awardee['entry'][0]['resource']
        self.assertEqual(resource['lastName'], last_name)
        self.assertEqual(resource['hpoId'], 'UNSET')

        response_dob_and_lastname_correct_role_and_awardee = self.send_get(
            f"ParticipantSummary?hpoId=UNSET&dateOfBirth={_date}&lastName={last_name}")
        self.assertEqual(len(response_dob_and_lastname_correct_role_and_awardee['entry']), 1)
        resource = response_dob_and_lastname_correct_role_and_awardee['entry'][0]['resource']
        self.assertEqual(resource['lastName'], last_name)
        self.assertEqual(resource['hpoId'], 'UNSET')
        self.assertEqual(resource['dateOfBirth'], '1978-10-09')

        self.overwrite_test_user_roles([PTC])

        response_only_dob = self.send_get(
            f"ParticipantSummary?dateOfBirth={_date}",
        )
        self.assertEqual(len(response_only_dob['entry']), 3)
        resource = response_only_dob['entry'][0]['resource']
        self.assertEqual(resource['dateOfBirth'], '1978-10-09')

        response_only_last_name = self.send_get(
            f"ParticipantSummary?lastName={last_name}",
        )

        self.assertEqual(len(response_only_last_name['entry']), 1)
        resource = response_only_last_name['entry'][0]['resource']
        self.assertEqual(resource['lastName'], last_name)

    def test_query_none_records_with_unset_for_enum_column(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        dao = ParticipantSummaryDao()
        # consent_for_genomics_ror will be set to default 0, update it to null for testing
        sql = """
            update participant_summary set consent_for_genomics_ror=null
            where rdr.participant_summary.participant_id={}
            """.format(participant_id[1:])
        with dao.session() as session:
            session.execute(sql)
            session.commit()
        response = self.send_get(f"ParticipantSummary?consentForGenomicsROR=UNSET&_includeTotal=true")
        self.assertEqual(len(response['entry']), 1)
        self.assertEqual(response['total'], 1)

    def test_hpro_consents(self):
        num_summary, first_pid, first_path = 2, None, None

        consents_map = {
            ConsentType.PRIMARY: 'consentForStudyEnrollment',
            ConsentType.CABOR: 'consentForCABoR',
            ConsentType.EHR: 'consentForElectronicHealthRecords',
            ConsentType.GROR: 'consentForGenomicsROR',
            ConsentType.PRIMARY_RECONSENT: 'reconsentForStudyEnrollment'
        }

        for num in range(num_summary):
            ps = self.data_generator.create_database_participant_summary(
                firstName=f"Testy_{num}",
                lastName=f"Tester_{num}",
                dateOfBirth=datetime.date(1978, 10, 9),
            )
            if num == 1:
                first_pid = ps.participantId

            for key, value in consents_map.items():
                consent_file = self.data_generator.create_database_consent_file(
                    file_path=f'test_file_path/{num}',
                    participant_id=ps.participantId,
                    file_exists=1,
                    type=key
                )

                if num == 1:
                    first_path = f'test_two_file_path/{num}'

                self.data_generator.create_database_hpro_consent(
                    consent_file_id=consent_file.id,
                    file_path=f'test_two_file_path/{num}',
                    participant_id=consent_file.participant_id
                )

        self.overwrite_test_user_roles([HEALTHPRO])

        first_summary = self.send_get(f"Participant/P{first_pid}/Summary")

        first_count = 0
        for key, value in consents_map.items():
            first_count += 1
            file_path = f'{value}FilePath'
            self.assertTrue(file_path in first_summary.keys())
            self.assertIsNotNone(first_summary.get(file_path))
            self.assertEqual(first_summary[file_path], first_path)

        self.assertEqual(first_count, len(consents_map.keys()))

        self.overwrite_test_user_roles([PTC])

        first_summary = self.send_get(f"Participant/P{first_pid}/Summary")

        for key, value in consents_map.items():
            file_path = f'{value}FilePath'
            self.assertFalse(file_path in first_summary.keys())
            self.assertIsNone(first_summary.get(file_path))
            self.assertNotEqual(first_summary.get(file_path), first_path)

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_get(f"ParticipantSummary?_sort=lastModified")

        self.assertEqual(len(response['entry']), num_summary)

        for entry in response['entry']:
            for key, value in consents_map.items():
                file_path = f'{value}FilePath'
                self.assertTrue(file_path in entry['resource'].keys())
                self.assertIsNotNone(entry['resource'].get(file_path))

        self.overwrite_test_user_roles([PTC])

        response = self.send_get(f"ParticipantSummary?_sort=lastModified")

        self.assertEqual(len(response['entry']), num_summary)

        for entry in response['entry']:
            for key, value in consents_map.items():
                file_path = f'{value}FilePath'
                self.assertFalse(file_path in entry['resource'].keys())
                self.assertIsNone(entry['resource'].get(file_path))

    def test_hpro_participant_incentives(self):
        num_summary, first_pid, second_pid = 3, None, None

        site = self.site_dao.get(1)

        for num in range(num_summary):
            self.data_generator.create_database_participant_summary(
                firstName=f"Testy_{num}",
                lastName=f"Tester_{num}",
                dateOfBirth=datetime.date(1978, 10, 9),
            )

        current_summaries = self.ps_dao.get_all()

        incentives_pids = []

        first_pid = current_summaries[0].participantId
        second_pid = current_summaries[1].participantId
        third_pid = current_summaries[2].participantId

        date_given = "2022-02-07 21:15:35"
        cancelled_date = "2022-02-07 21:15:35"

        for num in range(4):
            if num != 3:
                self.data_generator.create_database_participant_incentives(
                    participantId=first_pid if num % 2 != 0 else second_pid,
                    createdBy="Test User",
                    site=site.siteId,
                    dateGiven=date_given,
                    occurrence="one_time",
                    incentiveType="cash",
                    amount=25,
                    notes="example_notes",
                    declined=1 if num % 2 != 0 else 0
                )
            else:
                self.data_generator.create_database_participant_incentives(
                    participantId=first_pid,
                    createdBy="Test User",
                    site=site.siteId,
                    dateGiven=date_given,
                    incentiveType="cash",
                    occurrence="one_time",
                    amount=25,
                    notes="example_notes",
                    cancelled=1,
                    cancelledBy="Test CancelUser",
                    cancelledDate=cancelled_date
                )

        self.overwrite_test_user_roles([PTC])

        first_summary = self.send_get(f"Participant/P{first_pid}/Summary")
        self.assertIsNone(first_summary.get('participantIncentives'))

        second_summary = self.send_get(f"Participant/P{second_pid}/Summary")
        self.assertIsNone(second_summary.get('participantIncentives'))

        third_summary = self.send_get(f"Participant/P{third_pid}/Summary")
        self.assertIsNone(third_summary.get('participantIncentives'))

        self.overwrite_test_user_roles([HEALTHPRO])

        first_summary = self.send_get(f"Participant/P{first_pid}/Summary")
        first_incentives = first_summary.get('participantIncentives')

        self.assertIsNotNone(first_incentives)
        self.assertEqual(len(first_incentives), 2)
        self.assertTrue(all(obj['participantId'] == f'P{first_pid}' for obj in first_incentives))

        # should be one
        self.assertTrue(any(obj['declined'] is True for obj in first_incentives))

        # should be one
        self.assertTrue(any(obj['cancelled'] is True for obj in first_incentives))
        self.assertTrue(any(obj['cancelledBy'] == 'Test CancelUser' for obj in first_incentives))
        self.assertTrue(any(obj['cancelledDate'] == cancelled_date for obj in first_incentives))

        incentives_pids.append(first_pid)

        second_summary = self.send_get(f"Participant/P{second_pid}/Summary")
        second_incentives = second_summary.get('participantIncentives')

        self.assertIsNotNone(second_incentives)
        self.assertEqual(len(second_incentives), 2)
        self.assertTrue(all(obj['participantId'] == f'P{second_pid}' for obj in second_incentives))

        # should be both
        self.assertTrue(all(obj['cancelled'] is False for obj in second_incentives))
        self.assertTrue(all(obj['cancelledBy'] == 'UNSET' for obj in second_incentives))
        self.assertTrue(all(obj['cancelledDate'] == 'UNSET' for obj in second_incentives))

        incentives_pids.append(second_pid)

        third_summary = self.send_get(f"Participant/P{third_pid}/Summary")
        self.assertIsNone(third_summary.get('participantIncentives'))

        self.overwrite_test_user_roles([PTC])

        response = self.send_get(f"ParticipantSummary?_sort=lastModified")

        entries = response['entry']
        resources = [obj.get('resource') for obj in entries]

        self.assertEqual(len(entries), len(current_summaries))
        self.assertTrue(all(obj.get('participantIncentives') is None for obj in resources))

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_get(f"ParticipantSummary?_sort=lastModified")
        entries = response['entry']
        resources = [obj.get('resource') for obj in entries]

        for pid in incentives_pids:
            per_pid_incentives = list(filter(lambda x: int(x['participantId'].split('P')[1]) == pid, resources))[0]
            self.assertEqual(len(per_pid_incentives['participantIncentives']), 2)

        self.assertEqual(len(entries), len(current_summaries))
        self.assertTrue(all(obj.get('participantIncentives') is not None for obj in resources))

    def test_get_aian_flag(self):
        for num in range(3):
            self.data_generator.create_database_participant_summary(
                firstName=f"Testy_{num}",
                lastName=f"Tester_{num}",
                dateOfBirth=datetime.date(1978, 10, 9),
                aian=1 if num < 2 else 0
            )

        current_summaries = self.ps_dao.get_all()
        first_pid = current_summaries[0].participantId
        second_pid = current_summaries[1].participantId
        third_pid = current_summaries[2].participantId

        first_summary = self.send_get(f"Participant/P{first_pid}/Summary")
        self.assertIsNotNone(first_summary.get('aian'))
        self.assertEqual(first_summary.get('aian'), True)

        second_summary = self.send_get(f"Participant/P{second_pid}/Summary")
        self.assertIsNotNone(second_summary.get('aian'))
        self.assertEqual(second_summary.get('aian'), True)

        third_summary = self.send_get(f"Participant/P{third_pid}/Summary")
        self.assertIsNotNone(third_summary.get('aian'))
        self.assertEqual(third_summary.get('aian'), False)

        response = self.send_get(f"ParticipantSummary?_sort=lastModified")
        entries = response['entry']
        resources = [obj.get('resource') for obj in entries]

        self.assertTrue(all(obj.get('aian') in (True, False) for obj in resources))

    def test_pairing_summary(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        path = "Participant/%s" % participant_id
        participant["awardee"] = "PITT"
        participant_update = self.send_put(path, participant, headers={"If-Match": 'W/"1"'})
        self.assertEqual(participant_update["awardee"], participant["awardee"])
        participant["organization"] = "AZ_TUCSON_BANNER_HEALTH"
        participant_update_2 = self.send_put(path, participant, headers={"If-Match": 'W/"2"'})
        self.assertEqual(participant_update_2["organization"], participant["organization"])
        self.assertEqual(participant_update_2["awardee"], "AZ_TUCSON")

    def test_admin_withdrawal_returns_right_info(self):
        with FakeClock(TIME_1):
            self.setup_codes(
                ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
                code_type=CodeType.ANSWER,
            )
            participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
            participant_id = participant["participantId"]
            with FakeClock(TIME_1):
                self.send_consent(participant_id)
            questionnaire_id = self.create_questionnaire("questionnaire3.json")

            # Populate some answers to the questionnaire
            answers = {
                "race": RACE_WHITE_CODE,
                "genderIdentity": "male",
                "firstName": self.fake.first_name(),
                "middleName": self.fake.first_name(),
                "lastName": self.fake.last_name(),
                "zipCode": "78751",
                "state": "PIIState_VA",
                "streetAddress": "1234 Main Street",
                "streetAddress2": "APT C",
                "city": "Austin",
                "sex": "male_sex",
                "sexualOrientation": "straight",
                "phoneNumber": "512-555-5555",
                "recontactMethod": "email_code",
                "education": "highschool",
                "income": "lotsofmoney",
                "dateOfBirth": datetime.date(1978, 10, 9),
                "CABoRSignature": "signature.pdf",
            }

            self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        with FakeClock(TIME_2):
            path = "Participant/%s" % participant_id
            participant["withdrawalStatus"] = "NO_USE"
            participant["withdrawalReason"] = "DUPLICATE"
            participant["withdrawalTimeStamp"] = 1563907344169
            participant["suspensionStatus"] = "NO_CONTACT"
            participant["withdrawalReasonJustification"] = "IT WAS A DUPLICATE"
            self.send_put(path, participant, headers={"If-Match": 'W/"1"'})
            response = self.send_get("ParticipantSummary", participant)
            self.assertGreater(len(response['entry']), 0)

        with FakeClock(TIME_3):
            response = self.send_get("Participant/%s/Summary" % participant_id)
            del answers["CABoRSignature"]
            # all fields available 24 hours after withdraw.
            for key in list(answers.keys()):
                self.assertIn(key, response)

        with FakeClock(TIME_5):
            response = self.send_get("Participant/%s/Summary" % participant_id)
            self.assertNotIn("city", response)
            self.assertNotIn("streetAddress", response)
            self.assertEqual(response["genderIdentity"], UNSET)
            self.assertEqual(response["withdrawalStatus"], "NO_USE")
            self.assertEqual(response["withdrawalReason"], "DUPLICATE")
            self.assertEqual(response["withdrawalAuthored"], "2019-07-23T18:42:24")
            self.assertEqual(response["withdrawalReasonJustification"], "IT WAS A DUPLICATE")

        response = self.send_get("ParticipantSummary?suspensionStatus=NO_CONTACT")
        self.assertEqual(len(response['entry']), 0)

    def test_suspension_status_returns_right_info(self):
        with FakeClock(TIME_1):
            self.setup_codes(
                ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
                code_type=CodeType.ANSWER,
            )
            participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
            participant_id = participant["participantId"]
            with FakeClock(TIME_1):
                self.send_consent(participant_id)
            questionnaire_id = self.create_questionnaire("questionnaire3.json")

            # Populate some answers to the questionnaire
            answers = {
                "race": RACE_WHITE_CODE,
                "genderIdentity": "male",
                "firstName": self.fake.first_name(),
                "middleName": self.fake.first_name(),
                "lastName": self.fake.last_name(),
                "zipCode": "78751",
                "state": "PIIState_VA",
                "streetAddress": "1234 Main Street",
                "streetAddress2": "APT C",
                "city": "Austin",
                "sex": "male_sex",
                "sexualOrientation": "straight",
                "phoneNumber": "512-555-5555",
                "recontactMethod": "email_code",
                "language": "en",
                "education": "highschool",
                "income": "lotsofmoney",
                "dateOfBirth": datetime.date(1978, 10, 9),
                "CABoRSignature": "signature.pdf",
            }

            self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        with FakeClock(TIME_2):
            path = "Participant/%s" % participant_id
            participant["suspensionStatus"] = "NO_CONTACT"
            self.send_put(path, participant, headers={"If-Match": 'W/"1"'})

        with FakeClock(TIME_3):
            response = self.send_get("Participant/%s/Summary" % participant_id)
            self.assertNotEqual(response["email"], "UNSET")  # email is random so just make sure it's something
            self.assertEqual(response["city"], "Austin")
            self.assertEqual(response["streetAddress"], "1234 Main Street")
            self.assertEqual(response["zipCode"], "78751")
            self.assertEqual(response["phoneNumber"], "512-555-5555")
            self.assertEqual(response["recontactMethod"], "NO_CONTACT")
            self.assertEqual(response["language"], "en")
            self.assertEqual(response["education"], "highschool")
            self.assertEqual(response["income"], "lotsofmoney")
            self.assertEqual(response["dateOfBirth"], "1978-10-09")

    def test_no_justification_fails(self):
        with FakeClock(TIME_1):
            self.setup_codes(
                ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
                code_type=CodeType.ANSWER,
            )
            participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
            participant_id = participant["participantId"]
            with FakeClock(TIME_1):
                self.send_consent(participant_id)
            questionnaire_id = self.create_questionnaire("questionnaire3.json")

            # Populate some answers to the questionnaire
            answers = {
                "race": RACE_WHITE_CODE,
                "genderIdentity": "male",
                "firstName": self.fake.first_name(),
                "middleName": self.fake.first_name(),
                "lastName": self.fake.last_name(),
                "zipCode": "78751",
                "state": "PIIState_VA",
                "streetAddress": "1234 Main Street",
                "streetAddress2": "APT C",
                "city": "Austin",
                "sex": "male_sex",
                "sexualOrientation": "straight",
                "phoneNumber": "512-555-5555",
                "recontactMethod": "email_code",
                "education": "highschool",
                "income": "lotsofmoney",
                "dateOfBirth": datetime.date(1978, 10, 9),
                "CABoRSignature": "signature.pdf",
            }

            self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        with FakeClock(TIME_2):
            path = "Participant/%s" % participant_id
            participant["withdrawalStatus"] = "NO_USE"
            participant["withdrawalReason"] = "DUPLICATE"
            # no withdrawalReasonJustification should fail.
            self.send_put(path, participant, headers={"If-Match": 'W/"1"'}, expected_status=http.client.BAD_REQUEST)

    def testQuery_noParticipants(self):
        self.send_get("Participant/P1/Summary", expected_status=http.client.NOT_FOUND)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([], response)

    def test_zero_participant_id(self):
        self.send_get("Participant/P000/Summary", expected_status=http.client.NOT_FOUND)

    def submit_questionnaire_response(
        self,
        participant_id,
        questionnaire_id,
        race_code,
        gender_code,
        first_name,
        middle_name,
        last_name,
        zip_code,
        state_code,
        street_address,
        street_address2,
        city,
        sex_code,
        login_phone_number,
        sexual_orientation_code,
        phone_number,
        recontact_method_code,
        language_code,
        education_code,
        income_code,
        date_of_birth,
        cabor_signature_uri,
        time=TIME_1,
    ):
        code_answers = []
        _add_code_answer(code_answers, "race", race_code)
        _add_code_answer(code_answers, "genderIdentity", gender_code)
        _add_code_answer(code_answers, "state", state_code)
        _add_code_answer(code_answers, "sex", sex_code)
        _add_code_answer(code_answers, "sexualOrientation", sexual_orientation_code)
        _add_code_answer(code_answers, "recontactMethod", recontact_method_code)
        _add_code_answer(code_answers, "language", language_code)
        _add_code_answer(code_answers, "education", education_code)
        _add_code_answer(code_answers, "income", income_code)

        qr = self.make_questionnaire_response_json(
            participant_id,
            questionnaire_id,
            code_answers=code_answers,
            string_answers=[
                ("firstName", first_name),
                ("middleName", middle_name),
                ("lastName", last_name),
                ("streetAddress", street_address),
                ("streetAddress2", street_address2),
                ("city", city),
                ("phoneNumber", phone_number),
                ("loginPhoneNumber", login_phone_number),
                ("zipCode", zip_code),
            ],
            date_answers=[("dateOfBirth", date_of_birth)],
            uri_answers=[("CABoRSignature", cabor_signature_uri)],
        )
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def testQuery_noSummaries(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        self.send_get("Participant/%s/Summary" % participant_id, expected_status=http.client.NOT_FOUND)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([], response)

    def test_last_modified_sync(self):
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)
        questionnaire_id = self.create_demographics_questionnaire()
        t1 = TIME_1
        t2 = TIME_1 + datetime.timedelta(seconds=200)
        t3 = t2 + datetime.timedelta(seconds=30)
        t4 = t3 + datetime.timedelta(seconds=30)
        # 1 minute buffer
        t5 = t4 + datetime.timedelta(seconds=40)

        def setup_participant(when, providerLink=self.provider_link):
            # Set up participant, questionnaire, and consent
            with FakeClock(when):
                participant = self.send_post("Participant", {"providerLink": [providerLink]})
                participant_id = participant["participantId"]
                self.send_consent(participant_id)
                # Populate some answers to the questionnaire
                answers = {
                    "race": RACE_WHITE_CODE,
                    "genderIdentity": PMI_SKIP_CODE,
                    "firstName": self.fake.first_name(),
                    "middleName": self.fake.first_name(),
                    "lastName": self.fake.last_name(),
                    "zipCode": "78751",
                    "state": PMI_SKIP_CODE,
                    "streetAddress": "1234 Main Street",
                    "city": "Austin",
                    "sex": PMI_SKIP_CODE,
                    "sexualOrientation": PMI_SKIP_CODE,
                    "phoneNumber": "512-555-5555",
                    "recontactMethod": PMI_SKIP_CODE,
                    "language": PMI_SKIP_CODE,
                    "education": PMI_SKIP_CODE,
                    "income": PMI_SKIP_CODE,
                    "dateOfBirth": datetime.date(1978, 10, 9),
                    "CABoRSignature": "signature.pdf",
                }
            self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
            return participant

        # Create the first batch and fetch their summaries
        first_batch = [setup_participant(t1) for _ in range(5)]
        first_batch.extend([setup_participant(t2) for _ in range(2)])
        first_batch.extend([setup_participant(t3) for _ in range(3)])
        url = "ParticipantSummary?_sort=lastModified&_sync=true&awardee=PITT"
        response = self.send_get(url)
        # We have the same number of participants as summaries
        self.assertEqual(len(response["entry"]), len(first_batch))
        last_modified_list = list()
        first_batch_list = list()
        for i in response["entry"]:
            last_modified_list.append(i["resource"]["lastModified"])
        for i in first_batch:
            first_batch_list.append(i["lastModified"])

        self.assertListEqual(last_modified_list, sorted(first_batch_list))
        # With the same ID's (they're the same participants)
        self.assertEqual(
            sorted([p["participantId"] for p in first_batch]),
            sorted([p["resource"]["participantId"] for p in response["entry"]]),
        )
        self.assertEqual(
            sorted([p["resource"]["lastModified"] for p in response["entry"]]),
            [p["resource"]["lastModified"] for p in response["entry"]],
        )

        t1_list_ids = list()
        t2_list_ids = list()
        t3_list_ids = list()
        response_list_1 = list()
        response_list_2 = list()
        response_list_3 = list()

        for participant in first_batch:
            if participant["lastModified"] == t1.strftime("%Y" "-" "%m" "-" "%d" "T" "%X"):
                t1_list_ids.append(participant["participantId"])
            elif participant["lastModified"] == t2.strftime("%Y" "-" "%m" "-" "%d" "T" "%X"):
                t2_list_ids.append(participant["participantId"])
            else:
                t3_list_ids.append(participant["participantId"])

        for i in response["entry"][:5]:
            response_list_1.append(i["resource"]["participantId"])

        for i in response["entry"][5:7]:
            response_list_2.append(i["resource"]["participantId"])

        for i in response["entry"][7:]:
            response_list_3.append(i["resource"]["participantId"])

        self.assertEqual(sorted(response_list_1), sorted(t1_list_ids))
        self.assertEqual(sorted(response_list_2), sorted(t2_list_ids))
        self.assertEqual(sorted(response_list_3), sorted(t3_list_ids))

        self.assertEqual(
            response["entry"][0]["resource"]["lastModified"], t1.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response["entry"][1]["resource"]["lastModified"], t1.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response["entry"][5]["resource"]["lastModified"], t2.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response["entry"][7]["resource"]["lastModified"], t3.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )

        # Get the next chunk with the sync url
        # Verify that this is, in fact, a sync URL - not a next
        sync_url = response["link"][0]["url"]
        index = sync_url.find("ParticipantSummary")
        self.assertEqual(response["link"][0]["relation"], "sync")

        # Verify that the next sync has results from t2 and t3 (within BUFFER).
        response2 = self.send_get(sync_url[index:])
        self.assertEqual(len(response2["entry"]), 5)
        self.assertEqual(
            response2["entry"][0]["resource"]["lastModified"], t2.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response2["entry"][1]["resource"]["lastModified"], t2.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response2["entry"][2]["resource"]["lastModified"], t3.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response2["entry"][3]["resource"]["lastModified"], t3.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )
        self.assertEqual(
            response2["entry"][4]["resource"]["lastModified"], t3.strftime("%Y" "-" "%m" "-" "%d" "T" "%X")
        )

        # verify adding '_backfill=false' returns no records.
        url2 = sync_url[index:].replace("_sync=true", "_backfill=false&_sync=true")
        response2A = self.send_get(url2)
        self.assertEqual(len(response2A["entry"]), 0)

        # Create a second batch
        second_batch = [setup_participant(t4) for _ in range(10)]
        response3 = self.send_get(sync_url[index:])
        # We have the same number of participants as summaries
        self.assertEqual(len(response3["entry"]), len(second_batch) + len(response2["entry"]))

        no_count_url = "ParticipantSummary?lastModified=lt%s&_sync=true&awardee=PITT" % TIME_4
        no_count_response = self.send_get(no_count_url)
        total_count = len(no_count_response["entry"])
        self.assertEqual(total_count, 20)
        url = "ParticipantSummary?lastModified=lt%s&_count=10&_sync=true&awardee=PITT" % TIME_4
        response = self.send_get(url)
        self.assertEqual(len(response["entry"]), 10)
        next_url = response["link"][0]["url"]
        next_10 = self.send_get(next_url[index:])
        self.assertEqual(len(next_10["entry"]), 10)

        sort_by_lastmodified = "ParticipantSummary?_sync=true&awardee=PITT&_sort=lastModified"
        sort_lm_response = self.send_get(sort_by_lastmodified)
        self.assertEqual(len(sort_lm_response["entry"]), 20)
        self.assertEqual(sort_lm_response["link"][0]["relation"], "sync")
        # ensure same participants are returned before 5 min. buffer
        sync_url = sort_lm_response["link"][0]["url"]
        setup_participant(t5)

        # az_provider_link should not be returned.
        setup_participant(t5, self.az_provider_link)
        sync_again = self.send_get(sync_url[index:])
        self.send_get(sort_by_lastmodified)
        self.assertGreaterEqual(len(sync_again["entry"]), 14)
        # The last 14 participants from sort_lm_response should be equal to the sync_again response.
        # self.assertEqual(sort_lm_response["entry"][7:], sync_again["entry"][:13])

        one_min_modified = list()
        for i in sync_again["entry"]:
            one_min_modified.append(
                datetime.datetime.strptime(i["resource"]["lastModified"], "%Y" "-" "%m" "-" "%d" "T" "%X")
            )

        # Everything should be within 60 seconds.
        margin = datetime.timedelta(seconds=60)
        self.assertTrue(one_min_modified[0] + margin <= t5)
        self.assertTrue(t5 - margin >= one_min_modified[0])
        self.assertTrue(one_min_modified[-1] <= t5)
        # TODO: this occasionally fails (flaky)
        # out_of_range_margin = datetime.timedelta(seconds=61)
        # self.assertFalse(one_min_modified[0] + out_of_range_margin <= t5)

        # participants with az_tucson still dont show up in sync.
        setup_participant(t5, self.az_provider_link)
        sync_again = self.send_get(sync_url[index:])
        self.assertGreaterEqual(len(sync_again["entry"]), 14)

    def test_filter_summary_by_unset_org(self):
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)
        questionnaire_id = self.create_demographics_questionnaire()
        participant = self.send_post("Participant", {"providerLink": []})
        participant_id = participant["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": PMI_SKIP_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }
        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)
        url = "ParticipantSummary?_sort=lastModified&organization=UNSET"
        response = self.send_get(url)
        self.assertEqual(len(response.get('entry')), 1)
        self.assertEqual(response.get('entry')[0].get('resource').get('organization'), 'UNSET')

    def test_get_summary_list_returns_total(self):
        page_size = 10
        num_participants = 20
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)
        questionnaire_id = self.create_demographics_questionnaire()

        # Prove that no results means a total of zero (if requested)
        response = self.send_get("ParticipantSummary?_count=%d&_includeTotal=true" % page_size)
        self.assertEqual(0, response["total"])
        # ... but ONLY if requested
        response = self.send_get("ParticipantSummary?_count=%d" % page_size)
        self.assertIsNone(response.get("total"))

        # generate participants to count
        for _ in range(num_participants):
            # Set up participant, questionnaire, and consent
            participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
            participant_id = participant["participantId"]
            with FakeClock(TIME_1):
                self.send_consent(participant_id)
            # Populate some answers to the questionnaire
            answers = {
                "race": RACE_WHITE_CODE,
                "genderIdentity": PMI_SKIP_CODE,
                "firstName": self.fake.first_name(),
                "middleName": self.fake.first_name(),
                "lastName": self.fake.last_name(),
                "zipCode": "78751",
                "state": PMI_SKIP_CODE,
                "streetAddress": self.streetAddress,
                "streetAddress2": self.streetAddress2,
                "city": "Austin",
                "sex": PMI_SKIP_CODE,
                "sexualOrientation": PMI_SKIP_CODE,
                "phoneNumber": "512-555-5555",
                "recontactMethod": PMI_SKIP_CODE,
                "language": PMI_SKIP_CODE,
                "education": PMI_SKIP_CODE,
                "income": PMI_SKIP_CODE,
                "dateOfBirth": datetime.date(1978, 10, 9),
                "CABoRSignature": "signature.pdf",
            }
            self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        # Prove that without the query param, no total is returned
        response = self.send_get("ParticipantSummary?_count=%d" % page_size)
        self.assertIsNone(response.get("total"))
        # Prove that the count and page are accurate even when the page size is larger than the total
        url = "ParticipantSummary?_count=%d&_includeTotal=true" % (num_participants * 2)
        response = self.send_get(url)
        self.assertEqual(response["total"], len(response["entry"]))
        self.assertEqual(response["total"], num_participants)

        # Prove that the 'total' key is correct
        response = self.send_get("ParticipantSummary?_count=%d&_includeTotal=true" % page_size)
        self.assertEqual(num_participants, response["total"])
        # Prove that we're still only returning what's on a single page
        self.assertEqual(page_size, len(response["entry"]))

        # Prove that the total remains consistent across pages
        next_url = response["link"][0]["url"]
        # Shave off the front so send_get actually sends the right thing
        index = next_url.find("ParticipantSummary")
        response2 = self.send_get(next_url[index:])
        # Check that the total has remained the same and that it is still the total # participants
        self.assertEqual(response2["total"], response["total"])
        self.assertEqual(response2["total"], num_participants)

    def test_get_summary_list_returns_offset_results(self):
        num_participants = 10
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)
        questionnaire_id = self.create_demographics_questionnaire()

        # generate participants to count
        for _ in range(num_participants):
            # Set up participant, questionnaire, and consent
            participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
            participant_id = participant["participantId"]
            with FakeClock(TIME_1):
                self.send_consent(participant_id)
            # Populate some answers to the questionnaire
            answers = {
                "race": RACE_WHITE_CODE,
                "genderIdentity": PMI_SKIP_CODE,
                "firstName": self.fake.first_name(),
                "middleName": self.fake.first_name(),
                "lastName": self.fake.last_name(),
                "zipCode": "78751",
                "state": PMI_SKIP_CODE,
                "streetAddress": self.streetAddress,
                "streetAddress2": self.streetAddress2,
                "city": "Austin",
                "sex": PMI_SKIP_CODE,
                "sexualOrientation": PMI_SKIP_CODE,
                "phoneNumber": "512-555-5555",
                "recontactMethod": PMI_SKIP_CODE,
                "language": PMI_SKIP_CODE,
                "education": PMI_SKIP_CODE,
                "income": PMI_SKIP_CODE,
                "dateOfBirth": datetime.date(1978, 10, 9),
                "CABoRSignature": "signature.pdf",
            }
            self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        response = self.send_get("ParticipantSummary?_count=10")
        # Pass offset query parameter
        response2 = self.send_get("ParticipantSummary?_count=5&_offset=2")

        # Verify offset results
        # Prove that first and last entry in response2 matches 3rd and 7th entry in response
        self.assertEqual(response2["entry"][0], response["entry"][2])
        self.assertEqual(response2["entry"][4], response["entry"][6])
        # Verify participants count for response2
        self.assertEqual(len(response2["entry"]), 5)

        response3 = self.send_get("ParticipantSummary?_count=5&_offset=6")
        # Prove that only available participants are returned even if the count is greater than available participants after offset
        self.assertEqual(len(response3["entry"]), 4)

        response4 = self.send_get("ParticipantSummary?_count=5&_offset=10")
        response5 = self.send_get("ParticipantSummary?_count=5&_offset=12")
        # Prove that zero participants are returned if offset is greater than or equal to total number of available participants
        self.assertEqual(len(response4["entry"]), 0)
        self.assertEqual(len(response5["entry"]), 0)

    def test_get_summary_with_skip_codes(self):
        # Set up the codes so they are mapped later.
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)

        # Set up participant, questionnaire, and consent
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_demographics_questionnaire()

        with FakeClock(TIME_1):
            self.send_consent(participant_id)

        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": PMI_SKIP_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        # Read the answers back via ParticipantSummary
        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        # copies the dictionary - some of these are altered slightly in transmission but most should be
        # the same
        expected = self.create_expected_response(participant, answers)

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)

    def test_get_summary_with_skip_code_for_race(self):
        # Set up the codes so they are mapped later.
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)

        # Set up participant, questionnaire, and consent
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_demographics_questionnaire()

        with FakeClock(TIME_1):
            self.send_consent(participant_id)

        # Populate some answers to the questionnaire
        answers = {
            "race": PMI_SKIP_CODE,
            "genderIdentity": PMI_SKIP_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        # Read the answers back via ParticipantSummary
        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        # copies the dictionary - some of these are altered slightly in transmission but most should be
        # the same
        expected = self.create_expected_response(participant, answers)

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)

    def test_get_summary_with_primary_language(self):
        # Set up the codes so they are mapped later.
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)

        # Set up participant, questionnaire, and consent
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_demographics_questionnaire()

        with FakeClock(TIME_1):
            self.send_consent(participant_id, language="es")

        # Populate some answers to the questionnaire
        answers = {
            "race": PMI_SKIP_CODE,
            "genderIdentity": PMI_SKIP_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        # Read the answers back via ParticipantSummary
        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        # copies the dictionary - some of these are altered slightly in transmission but most should be
        # the same
        expected = self.create_expected_response(participant, answers, consent_language="es")

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)

    def test_get_summary_with_patient_status(self):
        # Set up the codes so they are mapped later.
        self.setup_codes([PMI_SKIP_CODE], code_type=CodeType.ANSWER)

        # Set up participant, questionnaire, and consent
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_demographics_questionnaire()

        with FakeClock(TIME_1):
            self.send_consent(participant_id, language="es")

        # set up patient status
        status_org_name = "PITT_BANNER_HEALTH"
        patient_status_dict = {
            "subject": "Patient/{}".format(participant_id),
            "awardee": "PITT",
            "organization": status_org_name,
            "patient_status": "YES",
            "user": "john.doe@pmi-ops.org",
            "site": "hpo-site-monroeville",
            "authored": "2019-04-26T12:11:41Z",
            "comment": "This is comment",
        }
        summary_status_dict = {"organization": status_org_name, "status": "YES"}
        status_post_url = "/".join(["PatientStatus", participant_id, "Organization", status_org_name])
        self.send_post(status_post_url, patient_status_dict, expected_status=http.client.CREATED)

        # Populate some answers to the questionnaire
        answers = {
            "race": PMI_SKIP_CODE,
            "genderIdentity": PMI_SKIP_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        # Read the answers back via ParticipantSummary
        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        # copies the dictionary - some of these are altered slightly in transmission but most should be
        # the same
        expected = self.create_expected_response(
            participant, answers, consent_language="es", patient_statuses=[summary_status_dict]
        )

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)

    def testQuery_oneParticipant(self):
        # Set up the codes so they are mapped later.
        self.setup_codes(
            ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
            code_type=CodeType.ANSWER,
        )
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        questionnaire_id = self.create_questionnaire("questionnaire3.json")

        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": GENDER_MAN_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": "PIIState_VA",
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": "male_sex",
            "sexualOrientation": "straight",
            "phoneNumber": "512-555-5555",
            "recontactMethod": "email_code",
            "language": "en",
            "education": "highschool",
            "income": "lotsofmoney",
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        expected = self.create_expected_response(participant, answers)

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)

    def testQuery_oneParticipantStringConsent(self):
        # Set up the codes so they are mapped later.
        self.setup_codes(
            ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
            code_type=CodeType.ANSWER,
        )
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        questionnaire_id = self.create_questionnaire("questionnaire3.json")

        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": GENDER_MAN_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": "PIIState_VA",
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": "male_sex",
            "sexualOrientation": "straight",
            "phoneNumber": "512-555-5555",
            "recontactMethod": "email_code",
            "language": "en",
            "education": "highschool",
            "income": "lotsofmoney",
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, cabor_signature_string=True, **answers)

        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        expected = self.create_expected_response(participant, answers)

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)

    def _send_next(self, next_link):
        prefix_index = next_link.index(main.API_PREFIX)
        return self.send_get(next_link[prefix_index + len(main.API_PREFIX):])

    def assertResponses(self, initial_query, summaries_list):
        response = self.send_get(initial_query)
        for i in range(0, len(summaries_list)):
            summaries = summaries_list[i]
            next_url = self.assertBundle(
                [_make_entry(ps) for ps in summaries], response, has_next=i < len(summaries_list) - 1
            )
            if next_url:
                response = self._send_next(next_url)
            else:
                break

    def _submit_consent_questionnaire_response(
        self, participant_id, questionnaire_id, ehr_consent_answer, time=TIME_1, authored=None
    ):
        code_answers = []
        _add_code_answer(code_answers, "ehrConsent", ehr_consent_answer)
        qr = self.make_questionnaire_response_json(
            participant_id, questionnaire_id, code_answers=code_answers, authored=authored
        )
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def _submit_empty_questionnaire_response(self, participant_id, questionnaire_id, time=TIME_1):
        qr = self.make_questionnaire_response_json(participant_id, questionnaire_id)
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def _send_biobank_order(self, participant_id, order, time=TIME_1):
        with FakeClock(time):
            self.send_post("Participant/%s/BiobankOrder" % participant_id, order)

    def _store_biobank_sample(self, participant, test_code, time=TIME_1):
        BiobankStoredSampleDao().insert(
            BiobankStoredSample(
                biobankStoredSampleId="s" + participant["participantId"] + test_code,
                biobankId=participant["biobankId"][1:],
                test=test_code,
                biobankOrderIdentifier="KIT",
                confirmed=time,
            )
        )

    def testQuery_ehrConsent(self):
        questionnaire_id = self.create_questionnaire("all_consents_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        self.send_consent(participant_id_1, authored=datetime.datetime(2015, 11, 8))
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("UNSET", ps_1["consentForElectronicHealthRecords"])
        self.assertEqual(None, ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id, "NOPE",
            authored=datetime.datetime(2015, 11, 10)
        )
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("SUBMITTED_NO_CONSENT", ps_1["consentForElectronicHealthRecords"])
        self.assertEqual(None, ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))

        # DA-2732:  Make sure the test assigns a more recent timestamp for the YES response.  An identical authored
        # time for both YES/NO responses will not guarantee the consent status changes
        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id, CONSENT_PERMISSION_YES_CODE, time=TIME_2
        )
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("SUBMITTED", ps_1["consentForElectronicHealthRecords"])
        self.assertEqual(TIME_2.isoformat(), ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))

    def _submit_dvehr_consent_questionnaire_response(
        self, participant_id, questionnaire_id, dvehr_consent_answer, time=TIME_1, authored=None
    ):
        code_answers = []
        _add_code_answer(code_answers, "DVEHRSharing_AreYouInterested", dvehr_consent_answer)
        qr = self.make_questionnaire_response_json(
            participant_id,
            questionnaire_id,
            code_answers=code_answers,
            authored=authored
        )
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def test_dvehr_consent(self):
        questionnaire_id = self.create_questionnaire("all_consents_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        self.send_consent(participant_id_1, authored=datetime.datetime.utcnow() - datetime.timedelta(days=20))
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("UNSET", ps_1["consentForDvElectronicHealthRecordsSharing"])

        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id, DVEHRSHARING_CONSENT_CODE_NO, time=TIME_1,
            authored=datetime.datetime.utcnow() - datetime.timedelta(days=18)
        )
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("SUBMITTED_NO_CONSENT", ps_1["consentForDvElectronicHealthRecordsSharing"])

        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id, DVEHRSHARING_CONSENT_CODE_YES, time=TIME_2,
            authored=datetime.datetime.utcnow() - datetime.timedelta(days=16)
        )
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("SUBMITTED", ps_1["consentForDvElectronicHealthRecordsSharing"])

        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id, DVEHRSHARING_CONSENT_CODE_NOT_SURE, time=TIME_3,
            authored=datetime.datetime.utcnow() - datetime.timedelta(days=14)
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("SUBMITTED_NOT_SURE", ps_1["consentForDvElectronicHealthRecordsSharing"])

        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id, "", time=TIME_4,
            authored=datetime.datetime.utcnow() - datetime.timedelta(days=12)
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("SUBMITTED_NO_CONSENT", ps_1["consentForDvElectronicHealthRecordsSharing"])

    def testWithdrawThenPair(self):
        participant_1 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id_1)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("NOT_WITHDRAWN", ps_1["withdrawalStatus"])
        self.assertEqual("PITT", ps_1["awardee"])
        self.assertIsNone(ps_1.get("withdrawalTime"))

        with FakeClock(TIME_2):
            participant_1["withdrawalStatus"] = "NO_USE"
            self.send_put("Participant/%s" % participant_id_1, participant_1, headers={"If-Match": 'W/"1"'})

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("NO_USE", ps_1["withdrawalStatus"])
        self.assertEqual("PITT", ps_1["awardee"])
        self.assertEqual(TIME_2.isoformat(), ps_1.get("withdrawalTime"))

        with FakeClock(TIME_3):
            participant_1["providerLink"] = [self.az_provider_link]
            self.send_put("Participant/%s" % participant_id_1, participant_1, headers={"If-Match": 'W/"2"'})

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("AZ_TUCSON", ps_1["awardee"])
        self.assertEqual("NO_USE", ps_1["withdrawalStatus"])
        self.assertEqual(TIME_2.isoformat(), ps_1.get("withdrawalTime"))

    def test_ehr_consent_after_dv_consent(self):
        questionnaire_id_1 = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("ehr_consent_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]

        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        # submit dv consent only, the enrollmentStatusParticipantPlusEhrV3_1Time should be TIME_1
        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, DVEHRSHARING_CONSENT_CODE_YES, time=TIME_1
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_1.isoformat(), ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))
        self.assertEqual("SUBMITTED", ps_1["consentForDvElectronicHealthRecordsSharing"])
        self.assertEqual(TIME_1.isoformat(), ps_1["consentForDvElectronicHealthRecordsSharingTime"])

        # submit ehr consent after dv consent at TIME_2
        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_2, CONSENT_PERMISSION_YES_CODE, time=TIME_2
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        # the enrollmentStatusParticipantPlusEhrV3_1Time should still be TIME_1
        self.assertEqual(TIME_1.isoformat(), ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))

    def test_dv_consent_after_ehr_consent(self):
        questionnaire_id_1 = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("ehr_consent_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]

        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        # submit ehr consent only
        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_2, CONSENT_PERMISSION_YES_CODE, time=TIME_1
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        # the enrollmentStatusParticipantPlusEhrV3_1Time should still be TIME_1
        self.assertEqual(TIME_1.isoformat(), ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))

        # submit dv consent after ehr consent at TIME_2
        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, DVEHRSHARING_CONSENT_CODE_YES, time=TIME_2
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        # the enrollmentStatusParticipantPlusEhrV3_1Time should still be TIME_1
        self.assertEqual(TIME_1.isoformat(), ps_1.get("enrollmentStatusParticipantPlusEhrV3_1Time"))

    def test_dv_consent_withdraw_ehr_consent(self):
        questionnaire_id_1 = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("ehr_consent_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]

        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        # submit dv consent only, the enrollmentStatusMemberTime should be TIME_1
        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, DVEHRSHARING_CONSENT_CODE_YES, time=TIME_1
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_1.isoformat(), ps_1.get("enrollmentStatusMemberTime"))

        # withdraw ehr consent after dv consent at TIME_2
        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_2, CONSENT_PERMISSION_NO_CODE, time=TIME_2
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        # Participants that attain MEMBER status shouldn't lose the enrollment status
        self.assertEqual(TIME_1.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("MEMBER", ps_1.get("enrollmentStatus"))
        self.assertEqual("SUBMITTED_NO_CONSENT", ps_1.get("consentForElectronicHealthRecords"))

    def test_enrollment_status_history(self):
        primary_consent_datetime = datetime.datetime(2022, 3, 17)
        ehr_consent_datetime = datetime.datetime(2022, 4, 1)

        # Create a participant and submit primary consent for them
        participant_response = self.send_post("Participant", {})
        participant_id_str = participant_response["participantId"]
        with FakeClock(primary_consent_datetime):
            self.send_consent(participant_id_str)

        # Send EHR consent, upgrading them from INTERESTED to MEMBER
        ehr_questionnaire_id = self.create_questionnaire("ehr_consent_questionnaire.json")
        self._submit_consent_questionnaire_response(
            participant_id_str, ehr_questionnaire_id, CONSENT_PERMISSION_YES_CODE, time=ehr_consent_datetime
        )

        status_history = self.session.query(EnrollmentStatusHistory).filter(
            EnrollmentStatusHistory.participant_id == from_client_participant_id(participant_id_str),
            EnrollmentStatusHistory.version == '3.1'
        ).one()
        self.assertEqual('PARTICIPANT_PLUS_EHR', status_history.status)
        self.assertEqual(ehr_consent_datetime, status_history.timestamp)

    def test_member_ordered_stored_times_for_multi_biobank_order_with_only_dv_consent(self):
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_dvehr_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, DVEHRSHARING_CONSENT_CODE_YES, time=TIME_6
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json, time=TIME_1)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            "male",
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
            time=TIME_2,
        )
        # Send an empty questionnaire response for another questionnaire for participant 1,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_1, questionnaire_id_2)
        # Send physical measurements for participants 1
        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send another biobank order for participant 1 with a different timestamp
        order_json2 = load_biobank_order_json(int(participant_id_1[1:]), filename="biobank_order_3.json")
        self._send_biobank_order(participant_id_1, order_json2, time=TIME_2)
        # make sure enrollmentStatusCoreOrderedSampleTime is not changed
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

    def test_member_ordered_stored_times_for_multi_biobank_order(self):
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json, time=TIME_1)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            "male",
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
            time=TIME_2,
        )
        # Send an empty questionnaire response for another questionnaire for participant 1,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_1, questionnaire_id_2)
        # Send physical measurements for participants 1
        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send another biobank order for participant 1 with a different timestamp
        order_json2 = load_biobank_order_json(int(participant_id_1[1:]), filename="biobank_order_3.json")
        self._send_biobank_order(participant_id_1, order_json2, time=TIME_2)
        # make sure enrollmentStatusCoreOrderedSampleTime is not changed
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

    def test_member_ordered_stored_times_for_biobank_order_cancel(self):
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json, time=TIME_1)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            "male",
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
            time=TIME_2,
        )
        # Send an empty questionnaire response for another questionnaire for participant 1,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_1, questionnaire_id_2)
        # Send physical measurements for participants 1
        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Store samples for DNA for participants 1
        self._store_biobank_sample(participant_1, "1SAL", time=TIME_4)
        self._store_biobank_sample(participant_1, "2ED10", time=TIME_5)
        # Update participant summaries based on these changes.
        self.ps_dao.update_from_biobank_stored_samples(biobank_ids=[from_client_biobank_id(participant_1['biobankId'])])

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertEqual(TIME_4.isoformat(), ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # cancel a biobank order
        biobank_order_id = order_json["identifier"][1]["value"]
        path = (
            "Participant/%s/BiobankOrder" % to_client_participant_id(int(participant_id_1[1:]))
            + "/"
            + biobank_order_id
        )
        request_data = {
            "amendedReason": "Its all wrong",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "fred@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }
        self.send_patch(path, request_data=request_data, headers={"If-Match": 'W/"1"'})

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("sampleOrderStatus2ED10Time"))
        self.assertEqual(ps_1.get("sampleOrderStatus2ED10"), "UNSET")
        self.assertEqual(ps_1.get("biospecimenFinalizedSite"), "UNSET")
        self.assertEqual(ps_1.get("biospecimenCollectedSite"), "UNSET")

    def test_member_ordered_stored_times_for_consent_withdraw(self):
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json, time=TIME_1)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            "male",
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
            time=TIME_2,
        )
        # Send an empty questionnaire response for another questionnaire for participant 1,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_1, questionnaire_id_2)
        # Send physical measurements for participants 1
        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Store samples for DNA for participants 1
        self._store_biobank_sample(participant_1, "1SAL", time=TIME_4)
        self._store_biobank_sample(participant_1, "2ED10", time=TIME_5)
        # Update participant summaries based on these changes.
        self.ps_dao.update_from_biobank_stored_samples(biobank_ids=[from_client_biobank_id(participant_1['biobankId'])])

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertEqual(TIME_4.isoformat(), ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # test withdraws
        with FakeClock(TIME_3):
            participant_1["withdrawalStatus"] = "NO_USE"
            participant_1["withdrawalReason"] = "DUPLICATE"
            participant_1["withdrawalReasonJustification"] = "Duplicate."
            self.send_put("Participant/%s" % participant_id_1, participant_1, headers={"If-Match": 'W/"2"'})
        # one day after withdraw
        with FakeClock(TIME_4):
            ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertEqual(TIME_4.isoformat(), ps_1.get("enrollmentStatusCoreStoredSampleTime"))
        # two days after withdraw
        with FakeClock(TIME_5):
            ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertIsNone(ps_1.get("enrollmentStatusMemberTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

    def test_member_ordered_stored_times_for_physical_measurement_cancel(self):
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json, time=TIME_1)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            "male",
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
            time=TIME_2,
        )
        # Send an empty questionnaire response for another questionnaire for participant 1,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_1, questionnaire_id_2)
        # Send physical measurements for participants 1
        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            pm_response = self.send_post(path, measurements_1)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertIsNone(ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # Store samples for DNA for participants 1
        self._store_biobank_sample(participant_1, "1SAL", time=TIME_4)
        self._store_biobank_sample(participant_1, "2ED10", time=TIME_5)
        # Update participant summaries based on these changes.
        self.ps_dao.update_from_biobank_stored_samples(biobank_ids=[from_client_biobank_id(participant_1['biobankId'])])

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(TIME_6.isoformat(), ps_1.get("enrollmentStatusMemberTime"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertEqual(TIME_4.isoformat(), ps_1.get("enrollmentStatusCoreStoredSampleTime"))

        # cancel a physical measurement ([DA-1623] core status and dates should remain)
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        path = path + "/" + pm_response["id"]
        cancel_info = self.get_restore_or_cancel_info()
        self.send_patch(path, cancel_info)
        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("CANCELLED", ps_1.get("clinicPhysicalMeasurementsStatus"))
        self.assertEqual("2016-01-04T10:55:41", ps_1.get("enrollmentStatusCoreOrderedSampleTime"))
        self.assertEqual(TIME_4.isoformat(), ps_1.get("enrollmentStatusCoreStoredSampleTime"))

    def test_physical_measurement_status(self):
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        measurements_2 = load_measurement_json(participant_id_1, TIME_2.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)
        with FakeClock(TIME_2):
            pm_response2 = self.send_post(path, measurements_2)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("COMPLETED", ps_1.get("clinicPhysicalMeasurementsStatus"))

        self.ps_dao.update_from_biobank_stored_samples(biobank_ids=[from_client_biobank_id(participant_1['biobankId'])])

        # cancel a physical measurement
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        path = path + "/" + pm_response2["id"]
        cancel_info = self.get_restore_or_cancel_info()
        self.send_patch(path, cancel_info)

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        # status should still be completed because participant has another valid PM
        self.assertEqual("COMPLETED", ps_1.get("clinicPhysicalMeasurementsStatus"))
        self.assertEqual(ps_1.get("clinicPhysicalMeasurementsFinalizedTime"), TIME_1.isoformat())
        self.assertEqual(ps_1.get("clinicPhysicalMeasurementsTime"), TIME_1.isoformat())
        self.assertEqual(ps_1.get("clinicPhysicalMeasurementsCreatedSite"), "hpo-site-monroeville")
        self.assertEqual(ps_1.get("clinicPhysicalMeasurementsFinalizedSite"), "hpo-site-bannerphoenix")

    def test_participant_summary_returns_latest_pm(self):
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        participant_1 = self.send_post("Participant", {})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id_1)

        self._submit_consent_questionnaire_response(
            participant_id_1, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )

        measurements_1 = load_measurement_json(participant_id_1, TIME_1.isoformat())
        measurements_2 = load_measurement_json(participant_id_1, TIME_2.isoformat(), alternate=True)
        path = "Participant/%s/PhysicalMeasurements" % participant_id_1
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)
        with FakeClock(TIME_2):
            self.send_post(path, measurements_2)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedTime"], TIME_2.isoformat())
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsTime"], TIME_2.isoformat())
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedSite"], "hpo-site-clinic-phoenix")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsCreatedSite"], "hpo-site-bannerphoenix")

    def test_switch_to_test_account(self):
        self.setup_codes(
            ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
            code_type=CodeType.ANSWER,
        )

        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        participant_1 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id_1 = participant_1["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id_1)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_WHITE_CODE,
            "male",
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            "1234 Main Street",
            "APT C",
            "Austin",
            "male_sex",
            "215-222-2222",
            "straight",
            "512-555-5555",
            "email_code",
            "en",
            "highschool",
            "lotsofmoney",
            datetime.date(1978, 10, 9),
            "signature.pdf",
        )

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
        self.assertEqual("215-222-2222", ps_1["loginPhoneNumber"])
        self.assertEqual("PITT", ps_1["hpoId"])

        # change login phone number to 444-222-2222
        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_WHITE_CODE,
            "male",
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            "1234 Main Street",
            "APT C",
            "Austin",
            "male_sex",
            "444-222-2222",
            "straight",
            "512-555-5555",
            "email_code",
            "en",
            "highschool",
            "lotsofmoney",
            datetime.date(1978, 10, 9),
            "signature.pdf",
        )

        ps_1_with_test_login_phone_number = self.send_get("Participant/%s/Summary" % participant_id_1)

        self.assertEqual("444-222-2222", ps_1_with_test_login_phone_number["loginPhoneNumber"])
        self.assertEqual("TEST", ps_1_with_test_login_phone_number["hpoId"])

    def testQuery_manyParticipants(self):
        self.setup_codes(
            ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
            code_type=CodeType.ANSWER,
        )

        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        questionnaire_id_3 = self.create_questionnaire("all_consents_questionnaire.json")
        participant_1 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id_1 = participant_1["participantId"]
        BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
        participant_2 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id_2 = participant_2["participantId"]
        BaseTestCase.switch_auth_user("example@example.com", 'example')
        participant_3 = self.send_post("Participant", {})
        participant_id_3 = participant_3["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id_1)
            self.send_consent(participant_id_3)
            BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
            self.send_consent(participant_id_2)
            BaseTestCase.switch_auth_user("example@example.com", 'example')

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_WHITE_CODE,
            GENDER_MAN_CODE,
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            self.streetAddress,
            self.streetAddress2,
            "Austin",
            "male_sex",
            "215-222-2222",
            "straight",
            "512-555-5555",
            "email_code",
            "en",
            "highschool",
            "lotsofmoney",
            datetime.date(1978, 10, 9),
            "signature.pdf",
        )

        BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
        self.submit_questionnaire_response(
            participant_id_2,
            questionnaire_id,
            None,
            GENDER_WOMAN_CODE,
            "Mary",
            "Q",
            "Jones",
            "78751",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 8),
            None,
        )
        BaseTestCase.switch_auth_user("example@example.com", 'example')

        self.submit_questionnaire_response(
            participant_id_3,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            GENDER_NONBINARY_CODE,
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
        )
        # Send a questionnaire response for the consent questionnaire for participants 2 and 3
        BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
        self._submit_consent_questionnaire_response(participant_id_2, questionnaire_id_3, CONSENT_PERMISSION_YES_CODE)
        BaseTestCase.switch_auth_user("example@example.com", 'example')
        self._submit_consent_questionnaire_response(participant_id_3, questionnaire_id_3, CONSENT_PERMISSION_YES_CODE)

        # Send an empty questionnaire response for another questionnaire for participant 3,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_3, questionnaire_id_2)

        # Send physical measurements for participants 2 and 3
        measurements_2 = load_measurement_json(participant_id_2, TIME_1.isoformat())
        measurements_3 = load_measurement_json(participant_id_3, TIME_1.isoformat())
        path_2 = "Participant/%s/PhysicalMeasurements" % participant_id_2
        path_3 = "Participant/%s/PhysicalMeasurements" % participant_id_3
        with FakeClock(TIME_2):
            BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
            self.send_post(path_2, measurements_2)
            BaseTestCase.switch_auth_user("example@example.com", 'example')
            # This pairs participant 3 with PITT and updates their version.
            self.send_post(path_3, measurements_3)

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json)

        # Store samples for DNA for participants 1 and 3
        self._store_biobank_sample(participant_1, "1ED10")
        self._store_biobank_sample(participant_1, "1SAL2")
        self._store_biobank_sample(participant_3, "1SAL")
        self._store_biobank_sample(participant_3, "2ED10")
        # Update participant summaries based on these changes.
        self.ps_dao.update_from_biobank_stored_samples(biobank_ids=[
            from_client_biobank_id(participant_json['biobankId'])
            for participant_json in [participant_1, participant_3]
        ])
        # Update version for participant 3, which has changed.
        participant_3 = self.send_get("Participant/%s" % participant_id_3)

        with FakeClock(TIME_3):
            participant_2["withdrawalStatus"] = "NO_USE"
            participant_2["withdrawalReason"] = "DUPLICATE"
            participant_2["withdrawalReasonJustification"] = "Duplicate."
            participant_3["suspensionStatus"] = "NOT_SUSPENDED"
            participant_3["site"] = "hpo-site-monroeville"
            BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
            self.send_put("Participant/%s" % participant_id_2, participant_2, headers={"If-Match": 'W/"2"'})
            BaseTestCase.switch_auth_user("example@example.com", 'example')
            self.send_put(
                "Participant/%s" % participant_id_3,
                participant_3,
                headers={"If-Match": participant_3["meta"]["versionId"]},
            )

        with FakeClock(TIME_4):
            ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
            ps_3 = self.send_get("Participant/%s/Summary" % participant_id_3)

            BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
            ps_2 = self.send_get("Participant/%s/Summary" % participant_id_2)
            BaseTestCase.switch_auth_user("example@example.com", 'example')

        self.assertEqual(1, ps_1["numCompletedBaselinePPIModules"])
        self.assertEqual(1, ps_1["numBaselineSamplesArrived"])
        self.assertEqual("RECEIVED", ps_1["sampleStatus1ED10"])
        self.assertEqual(TIME_1.isoformat(), ps_1["sampleStatus1ED10Time"])
        self.assertEqual("UNSET", ps_1["sampleStatus1SAL"])
        self.assertEqual("UNSET", ps_1["sampleStatus2ED10"])
        self.assertEqual("RECEIVED", ps_1["sampleStatus1SAL2"])
        self.assertEqual("RECEIVED", ps_1["samplesToIsolateDNA"])
        self.assertEqual("PARTICIPANT", ps_1["enrollmentStatusV3_1"])
        self.assertEqual("UNSET", ps_1["clinicPhysicalMeasurementsStatus"])
        self.assertIsNone(ps_1.get("clinicPhysicalMeasurementsTime"))
        self.assertEqual("GenderIdentity_Man", ps_1["genderIdentity"])
        self.assertEqual("NOT_WITHDRAWN", ps_1["withdrawalStatus"])
        self.assertEqual("NOT_SUSPENDED", ps_1["suspensionStatus"])
        self.assertEqual("email_code", ps_1["recontactMethod"])
        self.assertIsNone(ps_1.get("withdrawalTime"))
        self.assertIsNone(ps_1.get("suspensionTime"))
        self.assertEqual("UNSET", ps_1["clinicPhysicalMeasurementsCreatedSite"])
        self.assertEqual("UNSET", ps_1["clinicPhysicalMeasurementsFinalizedSite"])
        self.assertIsNone(ps_1.get("clinicPhysicalMeasurementsTime"))
        self.assertIsNone(ps_1.get("clinicPhysicalMeasurementsFinalizedTime"))
        self.assertEqual("FINALIZED", ps_1["biospecimenStatus"])
        self.assertEqual("2016-01-04T09:40:21", ps_1["biospecimenOrderTime"])
        self.assertEqual("hpo-site-monroeville", ps_1["biospecimenSourceSite"])
        self.assertEqual("hpo-site-monroeville", ps_1["biospecimenCollectedSite"])
        self.assertEqual("hpo-site-monroeville", ps_1["biospecimenProcessedSite"])
        self.assertEqual("hpo-site-bannerphoenix", ps_1["biospecimenFinalizedSite"])
        self.assertEqual("UNSET", ps_1["sampleOrderStatus1ED04"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1ED10"])
        self.assertEqual("2016-01-04T10:55:41", ps_1["sampleOrderStatus1ED10Time"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1PST8"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus2ED10"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1SST8"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1HEP4"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1UR10"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1SAL"])
        self.assertEqual("215-222-2222", ps_1["loginPhoneNumber"])
        self.assertEqual("example", ps_1["participantOrigin"])

        # One day after participant 2 withdraws, their fields are still all populated.
        self.assertEqual(1, ps_2["numCompletedBaselinePPIModules"])
        self.assertEqual(0, ps_2["numBaselineSamplesArrived"])
        self.assertEqual("UNSET", ps_2["sampleStatus1ED10"])
        self.assertEqual("UNSET", ps_2["sampleStatus1SAL"])
        self.assertEqual("UNSET", ps_2["sampleStatus2ED10"])
        self.assertEqual("UNSET", ps_2["samplesToIsolateDNA"])
        self.assertEqual("PARTICIPANT_PLUS_EHR", ps_2["enrollmentStatusV3_1"])
        self.assertEqual("COMPLETED", ps_2["clinicPhysicalMeasurementsStatus"])
        self.assertEqual(TIME_2.isoformat(), ps_2["clinicPhysicalMeasurementsTime"])
        self.assertEqual("GenderIdentity_Woman", ps_2["genderIdentity"])
        self.assertEqual("NO_USE", ps_2["withdrawalStatus"])
        self.assertEqual("DUPLICATE", ps_2["withdrawalReason"])
        self.assertEqual("NOT_SUSPENDED", ps_2["suspensionStatus"])
        self.assertEqual("NO_CONTACT", ps_2["recontactMethod"])
        self.assertIsNotNone(ps_2["withdrawalTime"])
        self.assertEqual("hpo-site-monroeville", ps_2["clinicPhysicalMeasurementsCreatedSite"])
        self.assertEqual("hpo-site-bannerphoenix", ps_2["clinicPhysicalMeasurementsFinalizedSite"])
        self.assertEqual(TIME_2.isoformat(), ps_2["clinicPhysicalMeasurementsTime"])
        self.assertEqual(TIME_1.isoformat(), ps_2["clinicPhysicalMeasurementsFinalizedTime"])
        self.assertEqual("UNSET", ps_2["biospecimenStatus"])
        self.assertIsNone(ps_2.get("biospecimenOrderTime"))
        self.assertEqual("UNSET", ps_2["biospecimenSourceSite"])
        self.assertEqual("UNSET", ps_2["biospecimenCollectedSite"])
        self.assertEqual("UNSET", ps_2["biospecimenProcessedSite"])
        self.assertEqual("UNSET", ps_2["biospecimenFinalizedSite"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1ED04"])
        self.assertIsNone(ps_2.get("sampleOrderStatus1ED10Time"))
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1ED10"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1PST8"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus2ED10"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1SST8"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1HEP4"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1UR10"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1SAL"])
        self.assertEqual("vibrent", ps_2["participantOrigin"])

        self.assertIsNone(ps_2.get("suspensionTime"))
        self.assertEqual(3, ps_3["numCompletedBaselinePPIModules"])
        self.assertEqual(1, ps_3["numBaselineSamplesArrived"])
        self.assertEqual("UNSET", ps_3["sampleStatus1ED10"])
        self.assertEqual("RECEIVED", ps_3["sampleStatus1SAL"])
        self.assertEqual(TIME_1.isoformat(), ps_3["sampleStatus1SALTime"])
        self.assertEqual("RECEIVED", ps_3["sampleStatus2ED10"])
        self.assertEqual(TIME_1.isoformat(), ps_3["sampleStatus2ED10Time"])
        self.assertEqual("RECEIVED", ps_3["samplesToIsolateDNA"])
        self.assertEqual("CORE_PARTICIPANT", ps_3["enrollmentStatusV3_1"])
        self.assertEqual("COMPLETED", ps_3["clinicPhysicalMeasurementsStatus"])
        self.assertEqual(TIME_2.isoformat(), ps_3["clinicPhysicalMeasurementsTime"])
        self.assertEqual("GenderIdentity_NonBinary", ps_3["genderIdentity"])
        self.assertEqual("NOT_WITHDRAWN", ps_3["withdrawalStatus"])
        self.assertEqual("NOT_SUSPENDED", ps_3["suspensionStatus"])
        self.assertEqual("UNSET", ps_3["recontactMethod"])
        self.assertEqual("hpo-site-monroeville", ps_3["site"])
        self.assertIsNone(ps_3.get("withdrawalTime"))
        self.assertEqual("example", ps_3["participantOrigin"])

        # One day after participant 2 withdraws, the participant is still returned.
        with FakeClock(TIME_4):
            BaseTestCase.switch_auth_user("example@hpro.com", 'hpro')
            response = self.send_get("ParticipantSummary")
            self.assertBundle([_make_entry(ps_1), _make_entry(ps_2), _make_entry(ps_3)], response)

            self.assertResponses("ParticipantSummary?_count=2", [[ps_1, ps_2], [ps_3]])
            # Test sorting on fields of different types.
            self.assertResponses("ParticipantSummary?_count=2&_sort=firstName", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:asc=firstName", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=firstName", [[ps_2, ps_3], [ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=dateOfBirth", [[ps_2, ps_1], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=dateOfBirth", [[ps_3, ps_1], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=genderIdentity", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=genderIdentity", [[ps_3, ps_2], [ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=questionnaireOnTheBasics", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=hpoId", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=hpoId", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=awardee", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=organization", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:asc=site", [[ps_2, ps_1], [ps_3]])
            # Test filtering on fields.
            self.assertResponses("ParticipantSummary?_count=2&firstName=Mary", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&site=hpo-site-monroeville", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&awardee=PITT", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&organization=AZ_TUCSON_BANNER_HEALTH", [])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q", [[ps_1, ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&zipCode=78752", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&loginPhoneNumber=215-222-2222", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&hpoId=PITT", [[ps_1, ps_2], [ps_3]])
            self.assertResponses(
                "ParticipantSummary?_count=2&streetAddress={0}".format(self.streetAddress), [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&streetAddress2={0}".format(self.streetAddress2), [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&hpoId=UNSET", [[]])
            self.assertResponses("ParticipantSummary?_count=2&genderIdentity=GenderIdentity_Man", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&race=WHITE", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&race=OTHER_RACE", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q&race=WHITE", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q&race=WHITE&zipCode=78752", [[]])
            self.assertResponses(
                "ParticipantSummary?_count=2&questionnaireOnTheBasics=SUBMITTED", [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&consentForStudyEnrollment=SUBMITTED", [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&consentForCABoR=SUBMITTED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=UNSET", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=COMPLETED", [[ps_2, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&enrollmentStatusV3_1=PARTICIPANT", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&enrollmentStatusV3_1=PARTICIPANT_PLUS_EHR", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&enrollmentStatusV3_1=CORE_PARTICIPANT", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalStatus=NOT_WITHDRAWN", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalStatus=NO_USE", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=lt2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=ge2016-01-03", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionStatus=NOT_SUSPENDED", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionStatus=NO_CONTACT", [[]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionTime=lt2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionTime=ge2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsCreatedSite=UNSET", [[ps_1]])
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "clinicPhysicalMeasurementsCreatedSite=hpo-site-monroeville", [[ps_2, ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsFinalizedSite=UNSET", [[ps_1]])
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "clinicPhysicalMeasurementsFinalizedSite=hpo-site-bannerphoenix",
                [[ps_2, ps_3]],
            )
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=UNSET", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=COMPLETED", [[ps_2, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&biospecimenStatus=FINALIZED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&biospecimenOrderTime=ge2016-01-04", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&biospecimenOrderTime=lt2016-01-04", [[]])
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenSourceSite=hpo-site-monroeville", [[ps_1]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenCollectedSite=hpo-site-monroeville", [[ps_1]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenProcessedSite=hpo-site-monroeville", [[ps_1]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenFinalizedSite=hpo-site-bannerphoenix", [[ps_1]]
            )
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED04=UNSET", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED10=FINALIZED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED10Time=ge2016-01-04", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED10Time=lt2016-01-04", [[]])
            self.assertResponses("ParticipantSummary?_count=2&organization=PITT_BANNER_HEALTH", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&site=hpo-site-monroeville", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&participantOrigin=example", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?participantOrigin=vibrent", [[ps_2]])
            BaseTestCase.switch_auth_user("example@example.com", 'example')
        # Two days after participant 2 withdraws, their fields are not set for anything but
        # participant ID, HPO ID, withdrawal status, withdrawal time, and enrollment status
        with FakeClock(TIME_5):
            new_ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
            new_ps_3 = self.send_get("Participant/%s/Summary" % participant_id_3)

            BaseTestCase.switch_auth_user("example@spellman.com", 'vibrent')
            new_ps_2 = self.send_get("Participant/%s/Summary" % participant_id_2)
            BaseTestCase.switch_auth_user("example@example.com", 'example')

        self.assertEqual(ps_1, new_ps_1)
        self.assertEqual(ps_3, new_ps_3)
        self.assertEqual("Mary", new_ps_2["firstName"])
        self.assertEqual("Q", new_ps_2["middleName"])
        self.assertEqual("Jones", new_ps_2["lastName"])
        self.assertIsNone(new_ps_2.get("numCompletedBaselinePPIModules"))
        self.assertIsNone(new_ps_2.get("numBaselineSamplesArrived"))
        self.assertEqual("UNSET", new_ps_2["sampleStatus1ED10"])
        self.assertEqual("UNSET", new_ps_2["sampleStatus1SAL"])
        self.assertEqual("UNSET", new_ps_2["samplesToIsolateDNA"])
        self.assertEqual("PARTICIPANT_PLUS_EHR", new_ps_2["enrollmentStatusV3_1"])
        self.assertEqual("UNSET", new_ps_2["clinicPhysicalMeasurementsStatus"])
        self.assertEqual("SUBMITTED", new_ps_2["consentForStudyEnrollment"])
        self.assertIsNotNone(new_ps_2["consentForStudyEnrollmentAuthored"])
        self.assertEqual("SUBMITTED", new_ps_2["consentForElectronicHealthRecords"])
        self.assertIsNotNone(new_ps_2["consentForElectronicHealthRecordsAuthored"])
        self.assertIsNone(new_ps_2.get("clinicPhysicalMeasurementsTime"))
        self.assertEqual("UNSET", new_ps_2["genderIdentity"])
        self.assertEqual("NO_USE", new_ps_2["withdrawalStatus"])
        self.assertEqual(ps_2["biobankId"], new_ps_2["biobankId"])
        self.assertEqual("UNSET", new_ps_2["suspensionStatus"])
        self.assertEqual("NO_CONTACT", new_ps_2["recontactMethod"])
        self.assertEqual("PITT", new_ps_2["hpoId"])
        self.assertEqual("UNSET", new_ps_2["organization"])
        self.assertEqual("UNSET", new_ps_2["site"])
        self.assertEqual(participant_id_2, new_ps_2["participantId"])
        self.assertIsNotNone(ps_2["withdrawalTime"])
        self.assertIsNone(new_ps_2.get("suspensionTime"))
        # Queries that filter on fields not returned for withdrawn participants no longer return
        # participant 2; queries that filter on fields that are returned for withdrawn participants
        # include it; queries that ask for withdrawn participants get back participant 2 only.
        # Sort order does not affect whether withdrawn participants are included.
        with FakeClock(TIME_5):
            BaseTestCase.switch_auth_user("example@hpro.com", 'hpro')
            self.assertResponses("ParticipantSummary?_count=2&_sort=firstName", [[ps_1, ps_3], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:asc=firstName", [[ps_1, ps_3], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=firstName", [[new_ps_2, ps_3], [ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=dateOfBirth", [[new_ps_2, ps_1], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=dateOfBirth", [[ps_3, ps_1], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=genderIdentity", [[ps_1, new_ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=genderIdentity", [[ps_3, new_ps_2], [ps_1]])
            self.assertResponses(
                "ParticipantSummary?_count=2&_sort=questionnaireOnTheBasics", [[ps_1, new_ps_2], [ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&_sort=hpoId", [[ps_1, new_ps_2], [new_ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=hpoId", [[ps_1, new_ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&firstName=Mary", [[new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q", [[ps_1, new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&hpoId=PITT", [[ps_1, new_ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalStatus=NO_USE", [[new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=lt2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=ge2016-01-03", [[new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionStatus=NOT_SUSPENDED", [[ps_1, ps_3]])

            self.assertResponses("ParticipantSummary?_count=2&lastModified=lt2016-01-04", [[ps_3]])
            BaseTestCase.switch_auth_user("example@example.com", 'example')

    def testQuery_manyParticipants_dv_consent_only(self):
        self.setup_codes(
            ["PIIState_VA", "male_sex", "male", "straight", "email_code", "en", "highschool", "lotsofmoney"],
            code_type=CodeType.ANSWER,
        )

        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        questionnaire_id_3 = self.create_questionnaire("dv_ehr_share_consent_questionnaire.json")
        participant_1 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id_1 = participant_1["participantId"]
        participant_2 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id_2 = participant_2["participantId"]
        participant_3 = self.send_post("Participant", {})
        participant_id_3 = participant_3["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id_1)
            self.send_consent(participant_id_2)
            self.send_consent(participant_id_3)

        self.submit_questionnaire_response(
            participant_id_1,
            questionnaire_id,
            RACE_WHITE_CODE,
            "GenderIdentity_Man",
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            self.streetAddress,
            self.streetAddress2,
            "Austin",
            "male_sex",
            "215-222-2222",
            "straight",
            "512-555-5555",
            "email_code",
            "en",
            "highschool",
            "lotsofmoney",
            datetime.date(1978, 10, 9),
            "signature.pdf",
        )
        self.submit_questionnaire_response(
            participant_id_2,
            questionnaire_id,
            None,
            "GenderIdentity_Woman",
            "Mary",
            "Q",
            "Jones",
            "78751",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 8),
            None,
        )
        self.submit_questionnaire_response(
            participant_id_3,
            questionnaire_id,
            RACE_NONE_OF_THESE_CODE,
            "GenderIdentity_Man",
            "Fred",
            "T",
            "Smith",
            "78752",
            None,
            self.streetAddress,
            self.streetAddress2,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            datetime.date(1978, 10, 10),
            None,
        )
        # Send a questionnaire response for only the dv consent questionnaire for participants 2 and 3
        self._submit_dvehr_consent_questionnaire_response(
            participant_id_2, questionnaire_id_3, DVEHRSHARING_CONSENT_CODE_YES
        )
        self._submit_dvehr_consent_questionnaire_response(
            participant_id_3, questionnaire_id_3, DVEHRSHARING_CONSENT_CODE_YES
        )

        # Send an empty questionnaire response for another questionnaire for participant 3,
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id_3, questionnaire_id_2)

        # Send physical measurements for participants 2 and 3
        measurements_2 = load_measurement_json(participant_id_2, TIME_1.isoformat())
        measurements_3 = load_measurement_json(participant_id_3, TIME_1.isoformat())
        path_2 = "Participant/%s/PhysicalMeasurements" % participant_id_2
        path_3 = "Participant/%s/PhysicalMeasurements" % participant_id_3
        with FakeClock(TIME_2):
            self.send_post(path_2, measurements_2)
            # This pairs participant 3 with PITT and updates their version.
            self.send_post(path_3, measurements_3)

        # Send a biobank order for participant 1
        order_json = load_biobank_order_json(int(participant_id_1[1:]))
        self._send_biobank_order(participant_id_1, order_json)

        # Store samples for DNA for participants 1 and 3
        self._store_biobank_sample(participant_1, "1ED10")
        self._store_biobank_sample(participant_1, "1SAL2")
        self._store_biobank_sample(participant_3, "1SAL")
        self._store_biobank_sample(participant_3, "2ED10")
        # Update participant summaries based on these changes.
        self.ps_dao.update_from_biobank_stored_samples(biobank_ids=[
            from_client_biobank_id(participant_json['biobankId'])
            for participant_json in [participant_1, participant_3]
        ])

        ps_2 = self.send_get("Participant/%s/Summary" % participant_id_2)
        self.assertEqual("SUBMITTED", ps_2["consentForDvElectronicHealthRecordsSharing"])
        self.assertIsNotNone(ps_2["consentForDvElectronicHealthRecordsSharingTime"])

        # Update version for participant 3, which has changed.
        participant_3 = self.send_get("Participant/%s" % participant_id_3)

        with FakeClock(TIME_3):
            participant_2["withdrawalStatus"] = "NO_USE"
            participant_2["withdrawalReason"] = "DUPLICATE"
            participant_2["withdrawalReasonJustification"] = "Duplicate."
            participant_3["suspensionStatus"] = "NOT_SUSPENDED"
            participant_3["site"] = "hpo-site-monroeville"
            self.send_put("Participant/%s" % participant_id_2, participant_2, headers={"If-Match": 'W/"2"'})
            self.send_put(
                "Participant/%s" % participant_id_3,
                participant_3,
                headers={"If-Match": participant_3["meta"]["versionId"]},
            )

        with FakeClock(TIME_4):
            ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
            ps_2 = self.send_get("Participant/%s/Summary" % participant_id_2)
            ps_3 = self.send_get("Participant/%s/Summary" % participant_id_3)

        self.assertEqual(1, ps_1["numCompletedBaselinePPIModules"])
        self.assertEqual(1, ps_1["numBaselineSamplesArrived"])
        self.assertEqual("RECEIVED", ps_1["sampleStatus1ED10"])
        self.assertEqual(TIME_1.isoformat(), ps_1["sampleStatus1ED10Time"])
        self.assertEqual("UNSET", ps_1["sampleStatus1SAL"])
        self.assertEqual("UNSET", ps_1["sampleStatus2ED10"])
        self.assertEqual("RECEIVED", ps_1["sampleStatus1SAL2"])
        self.assertEqual("RECEIVED", ps_1["samplesToIsolateDNA"])
        self.assertEqual("INTERESTED", ps_1["enrollmentStatus"])
        self.assertEqual("UNSET", ps_1["clinicPhysicalMeasurementsStatus"])
        self.assertIsNone(ps_1.get("clinicPhysicalMeasurementsTime"))
        self.assertEqual("GenderIdentity_Man", ps_1["genderIdentity"])
        self.assertEqual("NOT_WITHDRAWN", ps_1["withdrawalStatus"])
        self.assertEqual("NOT_SUSPENDED", ps_1["suspensionStatus"])
        self.assertEqual("email_code", ps_1["recontactMethod"])
        self.assertIsNone(ps_1.get("withdrawalTime"))
        self.assertIsNone(ps_1.get("suspensionTime"))
        self.assertEqual("UNSET", ps_1["clinicPhysicalMeasurementsCreatedSite"])
        self.assertEqual("UNSET", ps_1["clinicPhysicalMeasurementsFinalizedSite"])
        self.assertIsNone(ps_1.get("clinicPhysicalMeasurementsTime"))
        self.assertIsNone(ps_1.get("clinicPhysicalMeasurementsFinalizedTime"))
        self.assertEqual("FINALIZED", ps_1["biospecimenStatus"])
        self.assertEqual("2016-01-04T09:40:21", ps_1["biospecimenOrderTime"])
        self.assertEqual("hpo-site-monroeville", ps_1["biospecimenSourceSite"])
        self.assertEqual("hpo-site-monroeville", ps_1["biospecimenCollectedSite"])
        self.assertEqual("hpo-site-monroeville", ps_1["biospecimenProcessedSite"])
        self.assertEqual("hpo-site-bannerphoenix", ps_1["biospecimenFinalizedSite"])
        self.assertEqual("UNSET", ps_1["sampleOrderStatus1ED04"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1ED10"])
        self.assertEqual("2016-01-04T10:55:41", ps_1["sampleOrderStatus1ED10Time"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1PST8"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus2ED10"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1SST8"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1HEP4"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1UR10"])
        self.assertEqual("FINALIZED", ps_1["sampleOrderStatus1SAL"])
        self.assertEqual("215-222-2222", ps_1["loginPhoneNumber"])

        # One day after participant 2 withdraws, their fields are still all populated.
        self.assertEqual(1, ps_2["numCompletedBaselinePPIModules"])
        self.assertEqual(0, ps_2["numBaselineSamplesArrived"])
        self.assertEqual("UNSET", ps_2["sampleStatus1ED10"])
        self.assertEqual("UNSET", ps_2["sampleStatus1SAL"])
        self.assertEqual("UNSET", ps_2["sampleStatus2ED10"])
        self.assertEqual("UNSET", ps_2["samplesToIsolateDNA"])
        self.assertEqual("MEMBER", ps_2["enrollmentStatus"])
        self.assertEqual("COMPLETED", ps_2["clinicPhysicalMeasurementsStatus"])
        self.assertEqual(TIME_2.isoformat(), ps_2["clinicPhysicalMeasurementsTime"])
        self.assertEqual("GenderIdentity_Woman", ps_2["genderIdentity"])
        self.assertEqual("NO_USE", ps_2["withdrawalStatus"])
        self.assertEqual("DUPLICATE", ps_2["withdrawalReason"])
        self.assertEqual("NOT_SUSPENDED", ps_2["suspensionStatus"])
        self.assertEqual("NO_CONTACT", ps_2["recontactMethod"])
        self.assertIsNotNone(ps_2["withdrawalTime"])
        self.assertEqual("hpo-site-monroeville", ps_2["clinicPhysicalMeasurementsCreatedSite"])
        self.assertEqual("hpo-site-bannerphoenix", ps_2["clinicPhysicalMeasurementsFinalizedSite"])
        self.assertEqual(TIME_2.isoformat(), ps_2["clinicPhysicalMeasurementsTime"])
        self.assertEqual(TIME_1.isoformat(), ps_2["clinicPhysicalMeasurementsFinalizedTime"])
        self.assertEqual("UNSET", ps_2["biospecimenStatus"])
        self.assertIsNone(ps_2.get("biospecimenOrderTime"))
        self.assertEqual("UNSET", ps_2["biospecimenSourceSite"])
        self.assertEqual("UNSET", ps_2["biospecimenCollectedSite"])
        self.assertEqual("UNSET", ps_2["biospecimenProcessedSite"])
        self.assertEqual("UNSET", ps_2["biospecimenFinalizedSite"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1ED04"])
        self.assertIsNone(ps_2.get("sampleOrderStatus1ED10Time"))
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1ED10"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1PST8"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus2ED10"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1SST8"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1HEP4"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1UR10"])
        self.assertEqual("UNSET", ps_2["sampleOrderStatus1SAL"])

        self.assertIsNone(ps_2.get("suspensionTime"))
        self.assertEqual(3, ps_3["numCompletedBaselinePPIModules"])
        self.assertEqual(1, ps_3["numBaselineSamplesArrived"])
        self.assertEqual("UNSET", ps_3["sampleStatus1ED10"])
        self.assertEqual("RECEIVED", ps_3["sampleStatus1SAL"])
        self.assertEqual(TIME_1.isoformat(), ps_3["sampleStatus1SALTime"])
        self.assertEqual("RECEIVED", ps_3["sampleStatus2ED10"])
        self.assertEqual(TIME_1.isoformat(), ps_3["sampleStatus2ED10Time"])
        self.assertEqual("RECEIVED", ps_3["samplesToIsolateDNA"])
        self.assertEqual("FULL_PARTICIPANT", ps_3["enrollmentStatus"])
        self.assertEqual("COMPLETED", ps_3["clinicPhysicalMeasurementsStatus"])
        self.assertEqual(TIME_2.isoformat(), ps_3["clinicPhysicalMeasurementsTime"])
        self.assertEqual("GenderIdentity_Man", ps_3["genderIdentity"])
        self.assertEqual("NOT_WITHDRAWN", ps_3["withdrawalStatus"])
        self.assertEqual("NOT_SUSPENDED", ps_3["suspensionStatus"])
        self.assertEqual("UNSET", ps_3["recontactMethod"])
        self.assertEqual("hpo-site-monroeville", ps_3["site"])
        self.assertIsNone(ps_3.get("withdrawalTime"))

        # One day after participant 2 withdraws, the participant is still returned.
        with FakeClock(TIME_4):
            response = self.send_get("ParticipantSummary")
            self.assertBundle([_make_entry(ps_1), _make_entry(ps_2), _make_entry(ps_3)], response)

            self.assertResponses("ParticipantSummary?_count=2", [[ps_1, ps_2], [ps_3]])
            # Test sorting on fields of different types.
            self.assertResponses("ParticipantSummary?_count=2&_sort=firstName", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:asc=firstName", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=firstName", [[ps_2, ps_3], [ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=dateOfBirth", [[ps_2, ps_1], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=dateOfBirth", [[ps_3, ps_1], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=genderIdentity", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=genderIdentity", [[ps_2, ps_1], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=questionnaireOnTheBasics", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=hpoId", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=hpoId", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=awardee", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=organization", [[ps_1, ps_3], [ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:asc=site", [[ps_2, ps_1], [ps_3]])
            # Test filtering on fields.
            self.assertResponses("ParticipantSummary?_count=2&firstName=Mary", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&site=hpo-site-monroeville", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&awardee=PITT", [[ps_1, ps_2], [ps_3]])
            self.assertResponses(
                "ParticipantSummary?_count=2&streetAddress={0}".format(self.streetAddress), [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&streetAddress2={0}".format(self.streetAddress2), [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&organization=AZ_TUCSON_BANNER_HEALTH", [])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q", [[ps_1, ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&zipCode=78752", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&loginPhoneNumber=215-222-2222", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&hpoId=PITT", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&hpoId=UNSET", [[]])
            self.assertResponses("ParticipantSummary?_count=2&genderIdentity=GenderIdentity_Man", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&race=WHITE", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&race=OTHER_RACE", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q&race=WHITE", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q&race=WHITE&zipCode=78752", [[]])
            self.assertResponses(
                "ParticipantSummary?_count=2&questionnaireOnTheBasics=SUBMITTED", [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&consentForStudyEnrollment=SUBMITTED", [[ps_1, ps_2], [ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&consentForCABoR=SUBMITTED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=UNSET", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=COMPLETED", [[ps_2, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&enrollmentStatus=INTERESTED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&enrollmentStatus=MEMBER", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&enrollmentStatus=FULL_PARTICIPANT", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalStatus=NOT_WITHDRAWN", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalStatus=NO_USE", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=lt2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=ge2016-01-03", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionStatus=NOT_SUSPENDED", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionStatus=NO_CONTACT", [[]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionTime=lt2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionTime=ge2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsCreatedSite=UNSET", [[ps_1]])
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "clinicPhysicalMeasurementsCreatedSite=hpo-site-monroeville", [[ps_2, ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsFinalizedSite=UNSET", [[ps_1]])
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "clinicPhysicalMeasurementsFinalizedSite=hpo-site-bannerphoenix",
                [[ps_2, ps_3]],
            )
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=UNSET", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&clinicPhysicalMeasurementsStatus=COMPLETED", [[ps_2, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&biospecimenStatus=FINALIZED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&biospecimenOrderTime=ge2016-01-04", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&biospecimenOrderTime=lt2016-01-04", [[]])
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenSourceSite=hpo-site-monroeville", [[ps_1]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenCollectedSite=hpo-site-monroeville", [[ps_1]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenProcessedSite=hpo-site-monroeville", [[ps_1]]
            )
            self.assertResponses(
                "ParticipantSummary?_count=2&" + "biospecimenFinalizedSite=hpo-site-bannerphoenix", [[ps_1]]
            )
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED04=UNSET", [[ps_1, ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED10=FINALIZED", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED10Time=ge2016-01-04", [[ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&sampleOrderStatus1ED10Time=lt2016-01-04", [[]])
            self.assertResponses("ParticipantSummary?_count=2&organization=PITT_BANNER_HEALTH", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&site=hpo-site-monroeville", [[ps_1, ps_3]])

            self.overwrite_test_user_roles([PTC])

            self.assertResponses("ParticipantSummary?_count=2&lastName=Smith", [[ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&dateOfBirth=1978-10-08", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&dateOfBirth=gt1978-10-08", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&dateOfBirth=lt1978-10-08", [[]])
            self.assertResponses("ParticipantSummary?_count=2&dateOfBirth=le1978-10-08", [[ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&dateOfBirth=ge1978-10-08", [[ps_1, ps_2], [ps_3]])
            self.assertResponses(
                "ParticipantSummary?_count=2&dateOfBirth=ge1978-10-08&" "dateOfBirth=le1978-10-09", [[ps_1, ps_2]]
            )
            self.assertResponses("ParticipantSummary?_count=2&dateOfBirth=ne1978-10-09", [[ps_2, ps_3]])

        # Two days after participant 2 withdraws, their fields are not set for anything but
        # participant ID, HPO ID, withdrawal status, and withdrawal time
        with FakeClock(TIME_5):
            new_ps_1 = self.send_get("Participant/%s/Summary" % participant_id_1)
            new_ps_2 = self.send_get("Participant/%s/Summary" % participant_id_2)
            new_ps_3 = self.send_get("Participant/%s/Summary" % participant_id_3)
        self.assertEqual(ps_1, new_ps_1)
        self.assertEqual(ps_3, new_ps_3)
        self.assertEqual("Mary", new_ps_2["firstName"])
        self.assertEqual("Q", new_ps_2["middleName"])
        self.assertEqual("Jones", new_ps_2["lastName"])
        self.assertIsNone(new_ps_2.get("numCompletedBaselinePPIModules"))
        self.assertIsNone(new_ps_2.get("numBaselineSamplesArrived"))
        self.assertEqual("UNSET", new_ps_2["sampleStatus1ED10"])
        self.assertEqual("UNSET", new_ps_2["sampleStatus1SAL"])
        self.assertEqual("UNSET", new_ps_2["samplesToIsolateDNA"])
        self.assertEqual("MEMBER", new_ps_2["enrollmentStatus"])
        self.assertEqual("UNSET", new_ps_2["clinicPhysicalMeasurementsStatus"])
        self.assertEqual("SUBMITTED", new_ps_2["consentForStudyEnrollment"])
        self.assertIsNotNone(new_ps_2["consentForStudyEnrollmentAuthored"])
        self.assertEqual("UNSET", new_ps_2["consentForElectronicHealthRecords"])
        self.assertIsNone(new_ps_2.get("consentForElectronicHealthRecordsAuthored"))
        self.assertEqual("UNSET", new_ps_2["consentForDvElectronicHealthRecordsSharing"])
        self.assertIsNone(new_ps_2.get("consentForDvElectronicHealthRecordsSharingTime"))
        self.assertIsNone(new_ps_2.get("clinicPhysicalMeasurementsTime"))
        self.assertEqual("UNSET", new_ps_2["genderIdentity"])
        self.assertEqual("NO_USE", new_ps_2["withdrawalStatus"])
        self.assertEqual(ps_2["biobankId"], new_ps_2["biobankId"])
        self.assertEqual("UNSET", new_ps_2["suspensionStatus"])
        self.assertEqual("NO_CONTACT", new_ps_2["recontactMethod"])
        self.assertEqual("PITT", new_ps_2["hpoId"])
        self.assertEqual("UNSET", new_ps_2["organization"])
        self.assertEqual("UNSET", new_ps_2["site"])
        self.assertEqual(participant_id_2, new_ps_2["participantId"])
        self.assertIsNotNone(ps_2["withdrawalTime"])
        self.assertIsNone(new_ps_2.get("suspensionTime"))
        self.assertIsNone(new_ps_2.get("city"))
        self.assertIsNone(new_ps_2.get("streetAddress"))
        self.assertIsNone(new_ps_2.get("streetAddress2"))
        # Queries that filter on fields not returned for withdrawn participants no longer return
        # participant 2; queries that filter on fields that are returned for withdrawn participants
        # include it; queries that ask for withdrawn participants get back participant 2 only.
        # Sort order does not affect whether withdrawn participants are included.
        with FakeClock(TIME_5):
            self.assertResponses("ParticipantSummary?_count=2&_sort=firstName", [[ps_1, ps_3], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:asc=firstName", [[ps_1, ps_3], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=firstName", [[new_ps_2, ps_3], [ps_1]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=dateOfBirth", [[new_ps_2, ps_1], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=dateOfBirth", [[ps_3, ps_1], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort=genderIdentity", [[ps_1, ps_3], [new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=genderIdentity", [[new_ps_2, ps_1], [ps_3]])
            self.assertResponses(
                "ParticipantSummary?_count=2&_sort=questionnaireOnTheBasics", [[ps_1, new_ps_2], [ps_3]]
            )
            self.assertResponses("ParticipantSummary?_count=2&_sort=hpoId", [[ps_1, new_ps_2], [new_ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&_sort:desc=hpoId", [[ps_1, new_ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&firstName=Mary", [[new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&middleName=Q", [[ps_1, new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&hpoId=PITT", [[ps_1, new_ps_2], [ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalStatus=NO_USE", [[new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=lt2016-01-03", [[]])
            self.assertResponses("ParticipantSummary?_count=2&withdrawalTime=ge2016-01-03", [[new_ps_2]])
            self.assertResponses("ParticipantSummary?_count=2&suspensionStatus=NOT_SUSPENDED", [[ps_1, ps_3]])
            self.assertResponses("ParticipantSummary?_count=2&lastModified=lt2016-01-04", [[ps_3]])

    @unittest.skip("Only used for manual testing, should not be included in automated test suite")
    def testQuery_patient_status(self):
        """
    Test that patient status queries filter as expected
    """
        participant_1 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_1_id = participant_1["participantId"]

        participant_2 = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_2_id = participant_2["participantId"]

        with FakeClock(TIME_1):
            self.send_consent(participant_1_id, language="es")
        with FakeClock(TIME_2):
            self.send_consent(participant_2_id, language="es")

        # set up patient status for participant 1
        status_org_name = "PITT_BANNER_HEALTH"
        patient_status_dict = {
            "subject": "Patient/{}".format(participant_1_id),
            "awardee": "PITT",
            "organization": status_org_name,
            "patient_status": "YES",
            "user": "john.doe@pmi-ops.org",
            "site": "hpo-site-monroeville",
            "authored": "2019-04-26T12:11:41Z",
            "comment": "This is comment",
        }
        status_post_url = "/".join(["PatientStatus", participant_1_id, "Organization", status_org_name])
        self.send_post(status_post_url, patient_status_dict, expected_status=http.client.CREATED)

        # set up patient status for participant 2
        patient_status_dict = {
            "subject": "Patient/{}".format(participant_2_id),
            "awardee": "PITT",
            "organization": status_org_name,
            "patient_status": "NO_ACCESS",
            "user": "john.doe@pmi-ops.org",
            "site": "hpo-site-monroeville",
            "authored": "2019-04-26T12:11:41Z",
            "comment": "This is comment",
        }
        status_post_url = "/".join(["PatientStatus", participant_2_id, "Organization", status_org_name])
        self.send_post(status_post_url, patient_status_dict, expected_status=http.client.CREATED)

        # confirm queries
        summary_1 = self.send_get("Participant/{}/Summary".format(participant_1_id))
        summary_2 = self.send_get("Participant/{}/Summary".format(participant_2_id))

        default_query_params = {"_sort": "lastModified"}

        self.assertBundle(
            list(map(_make_entry, [summary_1, summary_2])),
            self.send_get("ParticipantSummary?{}".format(urlencode(default_query_params))),
        )

        query_params = dict(default_query_params, **{"patientStatus": "{}:YES".format(status_org_name)})
        url = "ParticipantSummary?{}".format(urlencode(query_params))
        self.assertBundle(list(map(_make_entry, [summary_1])), self.send_get(url))

        query_params = dict(default_query_params, **{"patientStatus": "{}:NO_ACCESS".format(status_org_name)})
        url = "ParticipantSummary?{}".format(urlencode(query_params))
        self.assertBundle(list(map(_make_entry, [summary_2])), self.send_get(url))

        query_params = list(default_query_params.items()) + [
            ("patientStatus", "{}:YES".format(status_org_name)),
            ("patientStatus", "{}:NO_ACCESS".format(status_org_name)),
        ]
        url = "ParticipantSummary?{}".format(urlencode(query_params))
        self.assertBundle([], self.send_get(url))

        query_params = dict(default_query_params, **{"patientStatus": "{}:UNSET".format(status_org_name)})
        url = "ParticipantSummary?{}".format(urlencode(query_params))
        self.assertBundle([], self.send_get(url))

        query_params = dict(default_query_params, **{"patientStatus": "AZ_TUCSON_BANNER_HEALTH:UNSET"})
        url = "ParticipantSummary?{}".format(urlencode(query_params))
        self.assertBundle(list(map(_make_entry, [summary_1, summary_2])), self.send_get(url))

    def test_gender_identity_pmi_skip(self):
        self.setup_codes(
            ["PIIState_VA", "male_sex", PMI_SKIP_CODE, "straight", "email_code", "en", "highschool", "lotsofmoney"],
            code_type=CodeType.ANSWER,
        )
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        questionnaire_id = self.create_questionnaire("questionnaire3.json")

        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": "PMI_Skip",
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": "PIIState_VA",
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": "male_sex",
            "sexualOrientation": "straight",
            "phoneNumber": "512-555-5555",
            "recontactMethod": "email_code",
            "language": "en",
            "education": "highschool",
            "income": "lotsofmoney",
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }

        self.post_demographics_questionnaire(participant_id, questionnaire_id, cabor_signature_string=True, **answers)

        with FakeClock(TIME_2):
            actual = self.send_get("Participant/%s/Summary" % participant_id)

        expected = self.create_expected_response(participant, answers)

        self.assertJsonResponseMatches(expected, actual)
        response = self.send_get("ParticipantSummary")
        self.assertBundle([_make_entry(actual)], response)
        self.assertEqual(actual["genderIdentity"], "PMI_Skip")

    def test_gender_prefer_not_to_answer(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": GENDER_PREFER_NOT_TO_ANSWER_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }
        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary["genderIdentity"], "PMI_PreferNotToAnswer")

    def test_origin_returns_only_origin(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        # Populate some answers to the questionnaire
        answers = {
            "race": RACE_WHITE_CODE,
            "genderIdentity": GENDER_PREFER_NOT_TO_ANSWER_CODE,
            "firstName": self.fake.first_name(),
            "middleName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "zipCode": "78751",
            "state": PMI_SKIP_CODE,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "city": "Austin",
            "sex": PMI_SKIP_CODE,
            "sexualOrientation": PMI_SKIP_CODE,
            "phoneNumber": "512-555-5555",
            "recontactMethod": PMI_SKIP_CODE,
            "language": PMI_SKIP_CODE,
            "education": PMI_SKIP_CODE,
            "income": PMI_SKIP_CODE,
            "dateOfBirth": datetime.date(1978, 10, 9),
            "CABoRSignature": "signature.pdf",
        }
        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        # switch user
        BaseTestCase.switch_auth_user("example@sabrina.com", "vibrent")
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
        self.post_demographics_questionnaire(participant_id, questionnaire_id, **answers)

        response = self.send_get("ParticipantSummary?_includeTotal=true")
        self.assertEqual(response['total'], 1)
        BaseTestCase.switch_auth_user("example@example.com", None)  # simulate an awardee GET
        response = self.send_get("ParticipantSummary?_includeTotal=true")
        BaseTestCase.switch_auth_user("example@example.com", "example")
        self.assertEqual(response['total'], 1)

    def test_query_by_enrollment_site(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        path = "Participant/%s" % participant_id
        self.send_consent(participant_id)

        participant["site"] = "hpo-site-bannerphoenix"
        update_p = self.send_put(path, participant, headers={"If-Match": 'W/"1"'})
        self.assertEqual(update_p["site"], "hpo-site-bannerphoenix")
        self.assertEqual(update_p["enrollmentSite"], "hpo-site-bannerphoenix")

        ps = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(ps['enrollmentSite'], "hpo-site-bannerphoenix")

        ps = self.send_get("ParticipantSummary?enrollmentSite=hpo-site-bannerphoenix")
        self.assertEqual(ps['entry'][0]['resource']['enrollmentSite'], 'hpo-site-bannerphoenix')

        ps = self.send_get("ParticipantSummary?enrollmentSite=UNSET")
        self.assertEqual(len(ps['entry']), 0)

    @patch('rdr_service.api.base_api.DEFAULT_MAX_RESULTS', 1)
    def test_parameter_pagination(self):
        # Duplicated parameters should appear in the next link when paging results

        # Force a paged response
        self.data_generator.create_database_participant_summary(consentForStudyEnrollmentAuthored='2019-04-01')
        self.data_generator.create_database_participant_summary(consentForStudyEnrollmentAuthored='2019-04-01')

        response = self.send_get("ParticipantSummary?"
                                 "consentForStudyEnrollmentAuthored=lt2020-01-01T00:00:00"
                                 "&consentForStudyEnrollmentAuthored=gt2019-01-01T00:00:00")

        next_url = response['link'][0]['url']
        self.assertIn('Authored=lt2020', next_url)
        self.assertIn('Authored=gt2019', next_url)

    def test_enum_status_parameters(self):
        # Unrecognized enum values should give descriptive error messages rather than 500s
        self.send_get("ParticipantSummary?enrollmentStatus=MEMBER|FULL_PARTICIPANT", expected_status=400)
        self.send_get("ParticipantSummary?withdrawalStatus=test", expected_status=400)
        self.send_get("ParticipantSummary?suspensionStatus=test", expected_status=400)

    def test_ehr_field_mapping(self):
        """Check that the new set of EHR data availability fields are present on the summary"""
        first_receipt_time = datetime.datetime(2020, 3, 27)
        latest_receipt_time = datetime.datetime(2020, 8, 4)

        participant_summary = self.data_generator.create_database_participant_summary(
            hpoId=2,
            ehrStatus=EhrStatus.PRESENT,
            isEhrDataAvailable=True,
            ehrReceiptTime=first_receipt_time,
            ehrUpdateTime=latest_receipt_time
        )
        response = self.send_get(f'Participant/P{participant_summary.participantId}/Summary')

        self.assertTrue(response['isEhrDataAvailable'])
        self.assertTrue(response['wasEhrDataAvailable'])
        self.assertEqual(first_receipt_time.isoformat(), response['firstEhrReceiptTime'])
        self.assertEqual(latest_receipt_time.isoformat(), response['latestEhrReceiptTime'])

        response = self.send_get(f'ParticipantSummary?_count=1&_sort=lastModified&awardee=PITT&_sync=false')
        self.assertEqual(first_receipt_time.isoformat(), response['entry'][0]['resource']['firstEhrReceiptTime'])
        self.assertEqual(latest_receipt_time.isoformat(), response['entry'][0]['resource']['latestEhrReceiptTime'])

        response = self.send_get(f'ParticipantSummary?_count=1&_sort=lastModified&awardee=PITT&_sync=true')
        self.assertEqual(first_receipt_time.isoformat(), response['entry'][0]['resource']['firstEhrReceiptTime'])
        self.assertEqual(latest_receipt_time.isoformat(), response['entry'][0]['resource']['latestEhrReceiptTime'])

    def test_digital_health_sharing(self):
        """Check that the new set of Digital Health Sharing data availability fields are present on the summary"""
        first_receipt_time = datetime.datetime(2020, 3, 27)
        latest_receipt_time = datetime.datetime(2020, 8, 4)

        sharing_summary = self.data_generator.create_database_participant_summary(
            hpoId=2,  # PITT
            ehrStatus=EhrStatus.PRESENT,
            isEhrDataAvailable=True,
            ehrReceiptTime=first_receipt_time,
            ehrUpdateTime=latest_receipt_time
        )
        not_sharing_summary = self.data_generator.create_database_participant_summary(
            hpoId=2,  # PITT
            ehrStatus=EhrStatus.NOT_PRESENT
        )

        # Check fields on participant that is sharing
        response = self.send_get(f'Participant/P{sharing_summary.participantId}/Summary')
        self.assertEqual('CURRENTLY_SHARING', response['healthDataStreamSharingStatusV3_1'])
        self.assertEqual(latest_receipt_time.isoformat(), response['healthDataStreamSharingStatusV3_1Time'])

        # Check fields on participant that is NOT sharing
        response = self.send_get(f'Participant/P{not_sharing_summary.participantId}/Summary')
        self.assertEqual('NEVER_SHARED', response['healthDataStreamSharingStatusV3_1'])
        self.assertNotIn('healthDataStreamSharingStatusV3_1Time', response)

        # Check the ordering of participants based on status
        response = self.send_get(f'ParticipantSummary?_sort=healthDataStreamSharingStatusV3_1&awardee=PITT&_sync=false')
        participant_id_list = [
            from_client_participant_id(entry['resource']['participantId'])
            for entry in response['entry']
        ]
        self.assertEqual(not_sharing_summary.participantId, participant_id_list[0])
        self.assertEqual(sharing_summary.participantId, participant_id_list[1])

        # Add in another participant and check the ordering based on the sharing date
        later_shared_summary = self.data_generator.create_database_participant_summary(
            hpoId=2,  # PITT
            ehrStatus=EhrStatus.PRESENT,
            isEhrDataAvailable=False,
            ehrReceiptTime=datetime.datetime(2021, 8, 4),
            ehrUpdateTime=datetime.datetime(2021, 8, 4)
        )
        response = self.send_get(
            f'ParticipantSummary?_sort=healthDataStreamSharingStatusV3_1Time&awardee=PITT&_sync=false'
        )
        participant_id_list = [
            from_client_participant_id(entry['resource']['participantId'])
            for entry in response['entry']
        ]
        self.assertEqual(not_sharing_summary.participantId, participant_id_list[0])
        self.assertEqual(sharing_summary.participantId, participant_id_list[1])
        self.assertEqual(later_shared_summary.participantId, participant_id_list[2])

    def test_disabling_data_glossary_3_fields(self):
        """Check that the 3.x enrollment statuses and digital health sharing fields are disabled by default"""
        summary = self.data_generator.create_database_participant_summary()

        # Override the default config, disabling the fields on the API
        self.temporarily_override_config_setting(config.ENABLE_ENROLLMENT_STATUS_3, False)
        self.temporarily_override_config_setting(config.ENABLE_HEALTH_SHARING_STATUS_3, False)

        # Check that the new fields are hidden
        api_response = self.send_get(f'Participant/P{summary.participantId}/Summary')
        self.assertNotIn('enrollmentStatusV3_0', api_response)
        self.assertNotIn('enrollmentStatusV3_1', api_response)
        self.assertNotIn('healthDataStreamSharingStatusV3_1', api_response)

    def test_blank_demographics_data_mapped_to_skip(self):
        # Create a participant summary that doesn't use skip codes for the demographics questions that weren't answered.
        # Some early summaries show this, we should map to displaying skip to have a more consistent output.
        participant_summary = self.data_generator.create_database_participant_summary(
            questionnaireOnTheBasics=QuestionnaireStatus.SUBMITTED,
            genderIdentityId=None,
            sexId=None,
            sexualOrientationId=None,
            race=None,
            educationId=None,
            incomeId=None
        )

        # Verify that the UNSET demographic fields are mapped to skip codes
        response = self.send_get(f'Participant/P{participant_summary.participantId}/Summary')
        self.assertEqual(PMI_SKIP_CODE, response['genderIdentity'])
        self.assertEqual(PMI_SKIP_CODE, response['sex'])
        self.assertEqual(PMI_SKIP_CODE, response['sexualOrientation'])
        self.assertEqual(PMI_SKIP_CODE, response['race'])
        self.assertEqual(PMI_SKIP_CODE, response['education'])
        self.assertEqual(PMI_SKIP_CODE, response['income'])

    def test_access_unset_participants_for_hoa_lite(self):
        participant = self.send_post("Participant", {"providerLink": [self.provider_link]})
        participant_id = participant["participantId"]
        participant2 = self.send_post("Participant", {"providerLink": []})
        participant_id2 = participant2["participantId"]
        with FakeClock(TIME_1):
            self.send_consent(participant_id)
            self.send_consent(participant_id2)

        config.override_setting(config.HPO_LITE_AWARDEE, ["PITT"])
        self.overwrite_test_user_awardee('PITT', ['awardee_sa'])
        self.send_get("ParticipantSummary?_count=10&awardee=AZ_TUCSON", expected_status=403)
        ps = self.send_get("ParticipantSummary?_count=10&awardee=PITT")
        self.assertEqual(len(ps['entry']), 1)
        self.assertEqual(ps['entry'][0]['resource']['hpoId'], 'PITT')
        ps = self.send_get("ParticipantSummary?_count=10&awardee=UNSET")
        self.assertEqual(len(ps['entry']), 1)
        self.assertEqual(ps['entry'][0]['resource']['hpoId'], 'UNSET')

    def test_api_sort_and_filter_with_aliased_fields(self):
        """Check that the aliased fields can be used as a sort argument through the API"""

        def generate_participant_with_first_receipt_time(timestamp):
            summary = self.data_generator.create_database_participant_summary(
                ehrReceiptTime=timestamp
            )
            return to_client_participant_id(summary.participantId)

        may_participant_id = generate_participant_with_first_receipt_time(datetime.datetime(2020, 5, 1))
        jan_participant_id = generate_participant_with_first_receipt_time(datetime.datetime(2020, 1, 1))
        oct_participant_id = generate_participant_with_first_receipt_time(datetime.datetime(2020, 10, 1))
        feb_participant_id = generate_participant_with_first_receipt_time(datetime.datetime(2020, 2, 1))
        mar_participant_id = generate_participant_with_first_receipt_time(datetime.datetime(2020, 3, 1))

        response = self.send_get('ParticipantSummary?_sort=firstEhrReceiptTime')
        response_ids = [entry['resource']['participantId'] for entry in response['entry']]
        self.assertEqual([
            jan_participant_id,
            feb_participant_id,
            mar_participant_id,
            may_participant_id,
            oct_participant_id
        ], response_ids)

        response = self.send_get('ParticipantSummary?_sort=firstEhrReceiptTime&firstEhrReceiptTime=lt2020-03-10')
        response_ids = [entry['resource']['participantId'] for entry in response['entry']]
        self.assertEqual([
            jan_participant_id,
            feb_participant_id,
            mar_participant_id
        ], response_ids)

    def test_api_sort_with_state_field(self):
        """Check that state field can be used as a sort argument through the API"""
        code_dao = CodeDao()

        with FakeClock(TIME_1):
            self.setup_codes(
                ["Washington", "Montana", "Missouri", "Mississippi", "Massachusetts"],
                code_type=CodeType.ANSWER,
            )

        def generate_participant_with_state(state):
            summary = self.data_generator.create_database_participant_summary(
                stateId=code_dao.get_code(PPI_SYSTEM, state).codeId
            )
            return to_client_participant_id(summary.participantId)

        wa_participant_id = generate_participant_with_state("Washington")
        mo_participant_id = generate_participant_with_state("Missouri")
        ma_participant_id = generate_participant_with_state("Massachusetts")
        mt_participant_id = generate_participant_with_state("Montana")
        ms_participant_id = generate_participant_with_state("Mississippi")
        null_state_participant_id = to_client_participant_id(
            self.data_generator.create_database_participant_summary().participantId)

        response = self.send_get('ParticipantSummary?_sort=state')
        response_ids = [entry['resource']['participantId'] for entry in response['entry']]
        self.assertEqual([
            null_state_participant_id,
            ma_participant_id,
            ms_participant_id,
            mo_participant_id,
            mt_participant_id,
            wa_participant_id
        ], response_ids)

    #### begin POST to ParticipantSummary API
    def test_response_for_pid_not_found_in_post(self):
        bad_pid = 'P12345'

        response = self.send_post(f'Participant/{bad_pid}/Summary', expected_status=http.client.NOT_FOUND)

        self.assertEqual(response.status_code, 404)

        bad_message = f'Participant {bad_pid} was not found'
        self.assertEqual(bad_message, response.json['message'])

    def test_response_for_correct_roles_post(self):
        participant_one = self.send_post("Participant", {})
        prefix_pid = participant_one["participantId"]

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_post(f'Participant/{prefix_pid}/Summary',
                                  expected_status=403)
        self.assertEqual(response.status_code, 403)

        bad_message = "You don't have the permission to access the " \
                      "requested resource. It is either read-protected or " \
                      "not readable by the server."

        self.assertEqual(bad_message, response.json['message'])

        self.overwrite_test_user_roles([PTC])

        response = self.send_post(f'Participant/{prefix_pid}/Summary', {})
        self.assertIsNotNone(response)
        self.assertEqual(response['participantId'], prefix_pid)

    def test_summary_created_on_post_if_doesnt_exist(self):
        participant_one = self.send_post("Participant", {})

        prefix_pid = participant_one["participantId"]
        pid = prefix_pid.split('P')[1]

        participant_summary = self.ps_dao.get_by_participant_id(pid)
        self.assertIsNone(participant_summary)

        response = self.send_post(f'Participant/{prefix_pid}/Summary', {})
        self.assertIsNotNone(response)

        participant_summary = self.ps_dao.get_by_participant_id(pid)
        self.assertIsNotNone(participant_summary)
        self.assertEqual(int(pid), participant_summary.participantId)

    def test_insert_defaults_not_overwritten_post(self):
        participant_one = self.send_post("Participant", {})

        prefix_pid = participant_one["participantId"]
        pid = prefix_pid.split('P')[1]
        biobank_id = participant_one["biobankId"]

        has_summary = self.ps_dao.get_by_participant_id(pid)
        self.assertIsNone(has_summary)

        post_payload = {
            "participantId": 12344543,
            "biobankId": 12344543,
        }

        response = self.send_post(f'Participant/{prefix_pid}/Summary', post_payload)

        self.assertIsNotNone(response)
        self.assertEqual(response['participantId'], prefix_pid)
        self.assertEqual(response['biobankId'], biobank_id)

    def test_payload_gets_inserted_into_values(self):
        participant_one = self.send_post("Participant", {})

        prefix_pid = participant_one["participantId"]
        pid = prefix_pid.split('P')[1]

        has_summary = self.ps_dao.get_by_participant_id(pid)
        self.assertIsNone(has_summary)

        first_name = self.fake.first_name()
        last_name = self.fake.first_name()
        email = self.fake.email()
        zip_code = '73097'

        post_payload = {
            "firstName": first_name,
            "lastName": last_name,
            "email": email,
            "zipCode": zip_code,
            "suspensionStatus": "NO_CONTACT"
        }

        response = self.send_post(
            f'Participant/{prefix_pid}/Summary',
            post_payload
        )
        self.assertIsNotNone(response)
        self.assertEqual(response['firstName'], post_payload['firstName'])
        self.assertEqual(response['lastName'], post_payload['lastName'])
        self.assertEqual(response['email'], post_payload['email'])
        self.assertEqual(response['zipCode'], post_payload['zipCode'])
        self.assertEqual(response['suspensionStatus'], post_payload['suspensionStatus'])

    def test_reinsert_throws_exception(self):
        participant_one = self.send_post("Participant", {})

        prefix_pid = participant_one["participantId"]
        pid = prefix_pid.split('P')[1]
        post_payload = {}

        has_summary = self.ps_dao.get_by_participant_id(pid)
        self.assertIsNone(has_summary)

        response = self.send_post(
            f'Participant/{prefix_pid}/Summary',
            post_payload
        )
        self.assertIsNotNone(response)

        has_summary = self.ps_dao.get_by_participant_id(pid)
        self.assertIsNotNone(has_summary)

        bad_message = f"Participant Summary for {prefix_pid} already exists, updates are not allowed."

        response = self.send_post(
            f'Participant/{prefix_pid}/Summary',
            post_payload,
            expected_status=400
        )

        self.assertEqual(bad_message, response.json['message'])
        self.assertEqual(response.status_code, 400)

    def test_site_in_roles_gives_correct_response(self):
        google_group = 'hpo-site-monroeville'
        monroe = self.site_dao.get_by_google_group(google_group)

        num_summary, first_name, second_pid = 10, "Testy", None

        for num in range(num_summary):
            ps = self.data_generator \
                .create_database_participant_summary(
                    firstName=first_name,
                    lastName="Tester",
                    siteId=monroe.siteId if num % 2 == 0 else 3
                )

            if num == 2:
                second_pid = ps.participantId

        response = self.send_get(f"Participant/P{second_pid}/Summary")

        self.assertIsNotNone(response)
        self.assertEqual(response['site'], google_group)

        response = self.send_get(f"ParticipantSummary?firstName={first_name}")
        responses = response['entry']

        self.assertEqual(len(responses), num_summary)
        self.assertFalse(all(obj['resource']['site'] == monroe.googleGroup for obj in responses))

        self.overwrite_test_user_site(google_group)

        response = self.send_get(f"Participant/P{second_pid}/Summary")

        self.assertIsNotNone(response)
        self.assertEqual(response['site'], google_group)

        response = self.send_get(f"ParticipantSummary?firstName={first_name}")
        responses = response['entry']

        self.assertEqual(len(responses), num_summary // 2)
        self.assertTrue(all(obj['resource']['site'] == monroe.googleGroup for obj in responses))
        self.assertFalse(any(obj['resource']['site'] != monroe.googleGroup for obj in responses))

    def test_list_to_first_site_in_roles(self):
        google_group = 'hpo-site-monroeville'
        monroe = self.site_dao.get_by_google_group(google_group)

        num_summary, first_name = 10, "Testy"

        for _ in range(num_summary):
            self.data_generator \
                .create_database_participant_summary(
                    firstName=first_name,
                    lastName="Tester",
                    siteId=monroe.siteId
                )

        self.overwrite_test_user_site([google_group])

        response = self.send_get(f"ParticipantSummary?firstName={first_name}")
        responses = response['entry']
        self.assertEqual(len(responses), num_summary)

    def test_bad_site_in_roles_throws_exception(self):
        google_group, num_summary, first_name = 'fake-hpo-site', 10, "Testy"

        for _ in range(num_summary):
            self.data_generator \
                .create_database_participant_summary(
                    firstName=first_name,
                    lastName="Tester",
                    siteId=1
                )

        self.overwrite_test_user_site(google_group)

        bad_message = f"No site found with google group {google_group}, that is attached to request user"

        response = self.send_get(f"ParticipantSummary?firstName={first_name}",
                                 expected_status=http.client.BAD_REQUEST)

        self.assertEqual(bad_message, response.json['message'])
        self.assertEqual(response.status_code, 400)

    def test_single_participant_with_conflicting_sites_throws_exception(self):
        google_group = 'hpo-site-monroeville'

        ps = self.data_generator \
            .create_database_participant_summary(
                firstName="Testy",
                lastName="Tester",
                siteId=3
        )

        self.overwrite_test_user_site(google_group)

        bad_message = f"Site attached to the request user, {google_group} is forbidden from accessing this participant"

        response = self.send_get(f"Participant/P{ps.participantId}/Summary",
                                 expected_status=403)

        self.assertEqual(bad_message, response.json['message'])
        self.assertEqual(response.status_code, 403)

    def test_user_site_override_site_in_args(self):
        google_group_one = 'hpo-site-monroeville'
        google_group_two = 'hpo-site-bannerphoenix'
        num_summary, first_name = 10, "Testy"

        monroe = self.site_dao.get_by_google_group(google_group_one)
        phoenix = self.site_dao.get_by_google_group(google_group_two)

        self.overwrite_test_user_site(google_group_one)

        for num in range(num_summary):
            self.data_generator \
                .create_database_participant_summary(
                    firstName=first_name,
                    lastName="Tester",
                    siteId=monroe.siteId if num % 2 == 0 else phoenix.siteId
                )

        response = self.send_get(f"ParticipantSummary?firstName={first_name}&site={google_group_two}")
        responses = response['entry']

        self.assertEqual(len(responses), num_summary // 2)
        self.assertTrue(all(obj['resource']['site'] == monroe.googleGroup for obj in responses))
        self.assertFalse(any(obj['resource']['site'] == phoenix.googleGroup for obj in responses))

    def test_synthetic_pm_fields(self):
        questionnaire_id = self.create_questionnaire("all_consents_questionnaire.json")
        remote_pm_questionnaire_id = self.create_questionnaire("remote_pm_questionnaire.json")
        participant = self.send_post("Participant", {})
        participant_id = participant["participantId"]
        with FakeClock(TIME_6):
            self.send_consent(participant_id)

        self._submit_consent_questionnaire_response(
            participant_id, questionnaire_id, CONSENT_PERMISSION_YES_CODE, time=TIME_6
        )
        # test no pm submitted
        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)

        self.assertEqual(participant_summary["physicalMeasurementsStatus"], "UNSET")
        self.assertEqual(participant_summary["physicalMeasurementsFinalizedSite"], "UNSET")
        self.assertEqual(participant_summary["physicalMeasurementsCreatedSite"], "UNSET")
        self.assertEqual(participant_summary["physicalMeasurementsCollectType"], "UNSET")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsStatus"], "UNSET")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedSite"], "UNSET")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsCreatedSite"], "UNSET")

        measurements_1 = load_measurement_json(participant_id, TIME_1.isoformat())
        path = "Participant/%s/PhysicalMeasurements" % participant_id
        with FakeClock(TIME_1):
            self.send_post(path, measurements_1)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedTime"], TIME_1.isoformat())
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsTime"], TIME_1.isoformat())
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedSite"], "hpo-site-bannerphoenix")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsCreatedSite"], "hpo-site-monroeville")
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsStatus"],
                         participant_summary["physicalMeasurementsStatus"])
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedTime"],
                         participant_summary["physicalMeasurementsFinalizedTime"])
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsTime"],
                         participant_summary["physicalMeasurementsTime"])
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsFinalizedSite"],
                         participant_summary["physicalMeasurementsFinalizedSite"])
        self.assertEqual(participant_summary["clinicPhysicalMeasurementsCreatedSite"],
                         participant_summary["physicalMeasurementsCreatedSite"])

        resource = load_remote_measurement_json("remote_pm_response_metric.json", remote_pm_questionnaire_id,
                                                participant_id)
        remote_pm_path = "Participant/%s/QuestionnaireResponse" % participant_id
        with FakeClock(TIME_2):
            self.send_post(remote_pm_path, resource)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(participant_summary["selfReportedPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(participant_summary["selfReportedPhysicalMeasurementsAuthored"], "2022-06-01T18:26:08")
        self.assertEqual(participant_summary["selfReportedPhysicalMeasurementsStatus"],
                         participant_summary["physicalMeasurementsStatus"])
        self.assertEqual(participant_summary["selfReportedPhysicalMeasurementsAuthored"],
                         participant_summary["physicalMeasurementsFinalizedTime"])
        self.assertEqual(participant_summary["physicalMeasurementsFinalizedSite"], "UNSET")
        self.assertEqual(participant_summary["physicalMeasurementsCreatedSite"], "UNSET")

    def test_updated_since_parameter(self):
        """
        The API should have the ability to only return a set of
        participant's that have been modified since a specified date
        """
        cutoff_date = datetime.datetime(2022, 3, 15)
        expected_participant_list = [
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date + datetime.timedelta(days=5)
            ),
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date + datetime.timedelta(minutes=1)
            ),
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date + datetime.timedelta(days=20)
            ),
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date + datetime.timedelta(days=30)
            )
        ]
        unexpected_participant_list = [
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date - datetime.timedelta(days=5)
            ),
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date - datetime.timedelta(minutes=1)
            ),
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date - datetime.timedelta(days=400)
            ),
            self.data_generator.create_database_participant_summary(
                lastModified=cutoff_date - datetime.timedelta(days=30)
            )
        ]

        response = self.send_get('ParticipantSummary?updatedSince=2022-03-15')
        response_ids = [
            from_client_participant_id(participant_json['resource']['participantId'])
            for participant_json in response['entry']
        ]
        self.assertTrue(all(
            expected_participant.participantId in response_ids
            for expected_participant in expected_participant_list
        ))
        self.assertFalse(any(
            unexpected_participant.participantId in response_ids
            for unexpected_participant in unexpected_participant_list
        ))
