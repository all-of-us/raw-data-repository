import http.client
from datetime import datetime
from mock import patch
from copy import deepcopy

from tests.helpers.unittest_base import BaseTestCase

from rdr_service import config
from rdr_service.clock import FakeClock
from rdr_service.dao.awardee_insite_dao import AwardeeInSiteDao
from rdr_service.api_util import HEALTHPRO, AWARDEE, RDR, PPSC
from rdr_service.model.awardee_insite import AwardeeInSite


class AwardeeInSiteApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.awardee_insite_dao = AwardeeInSiteDao()
        self.pitt_org_name = "PITT_BANNER_HEALTH"  # hpo_name=PITT
        self.az_org_name = "AZ_TUCSON_BANNER_HEALTH"  # hpo_name=AZ_TUCSON

        self.awardee_insite_rows = [
            {
                "participantId": 1234,
                "firstName": "John",
                "middleName": "Sam",
                "lastName": "Doe",
                "zipCode": "77490",
                "state": "TX",
                "city": "Houston",
                "streetAddress": "123 Lake Dr",
                "streetAddress2": "Apt 34",
                "phoneNumber": "1234567890",
                "email": "john@example.com",
                "dateOfBirth": "1992-06-08",
                "organization": self.pitt_org_name,
                "awardee": "PITT",
            },
            {
                "participantId": 2299,
                "firstName": "Alex",
                "middleName": None,
                "lastName": "Smith",
                "zipCode": "24354",
                "state": "NY",
                "city": "Albany",
                "streetAddress": "123 Forrest Dr",
                "streetAddress2": None,
                "phoneNumber": "4327685938",
                "email": "alex_smith@example.com",
                "dateOfBirth": "1989-05-05",
                "organization": self.pitt_org_name,
                "awardee": "PITT",
            },
            {
                "participantId": 3000,
                "firstName": "Meed",
                "middleName": None,
                "lastName": "Jade",
                "zipCode": "36509",
                "state": "TN",
                "city": "Nashville",
                "streetAddress": "9832 Albany Dr",
                "streetAddress2": "Apt 5",
                "phoneNumber": "9874567653",
                "email": "meed@example.com",
                "dateOfBirth": "1984-05-08",
                "organization": self.pitt_org_name,
                "awardee": "PITT",
            },
            {
                "participantId": 4866,
                "firstName": "Jack",
                "middleName": None,
                "lastName": "Matt",
                "zipCode": "45490",
                "state": "LA",
                "city": "Lafayette",
                "streetAddress": "184 Knox Ln",
                "streetAddress2": "Apt 67",
                "phoneNumber": "9843685667",
                "email": "jack_ma@example.com",
                "dateOfBirth": "1990-12-18",
                "organization": self.az_org_name,
                "awardee": "AZ_TUCSON",
            },
            {
                "participantId": 5450,
                "firstName": "Ali",
                "middleName": None,
                "lastName": "Mo",
                "zipCode": "56491",
                "state": "Illinois",
                "city": "Chicago",
                "streetAddress": "50 Cart Ln",
                "streetAddress2": None,
                "phoneNumber": "7563974610",
                "email": "alimo@example.com",
                "dateOfBirth": "1970-02-09",
                "organization": self.pitt_org_name,
                "awardee": "PITT",
            },
        ]
        # Insert records in awardee_insite table
        for record in self.awardee_insite_rows:
            self.awardee_insite_dao.insert(AwardeeInSite(**record))

        # Get pids for org in a list
        self.pitt_org_pids = [
            record["participantId"]
            for record in self.awardee_insite_rows
            if record["organization"] == self.pitt_org_name
        ]
        self.az_org_pids = [
            record["participantId"]
            for record in self.awardee_insite_rows
            if record["organization"] == self.az_org_name
        ]

    def overwrite_test_user_awardee(
        self, roles: list, awardee: str | None = None
    ) -> None:
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info["example@example.com"]["roles"] = roles
        if awardee:  # Add awardee key only for Awardees, and not for rdr
            new_user_info["example@example.com"]["awardee"] = awardee
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def test_awardee_insite_caller_roles(self):
        """Make sure only RDR and AWARDEE roles can call the API"""
        self.overwrite_test_user_awardee(roles=[RDR])
        response = self.send_get("AwardeeInSite?awardee=PITT")
        self.assertTrue(response is not None)

        self.overwrite_test_user_awardee(roles=[AWARDEE], awardee="PITT")
        response = self.send_get("AwardeeInSite")
        self.assertTrue(response is not None)

        self.overwrite_test_user_awardee([HEALTHPRO])
        response = self.send_get("AwardeeInSite", expected_status=http.client.FORBIDDEN)
        self.assertTrue(response.status_code == 403)

    def test_rdr_requires_awardee_parameter(self):
        """RDR must pass awardee parameter to call the API"""
        self.overwrite_test_user_awardee(roles=[RDR])
        response = self.send_get("AwardeeInSite?awardee=PITT")
        results = response.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]
        self.assertTrue(response is not None)
        self.assertEqual(len(results), len(self.pitt_org_pids))
        self.assertEqual(results_pid, self.pitt_org_pids)

        # Not passing in an awardee query param to the endpoint
        response = self.send_get(
            "AwardeeInSite", expected_status=http.client.BAD_REQUEST
        )
        self.assertTrue(response.status_code == 400)

    def test_ppsc_requires_awardee_parameter(self):
        """PPSC must pass awardee parameter to call the API"""
        self.overwrite_test_user_awardee(roles=[PPSC])
        response = self.send_get("AwardeeInSite?awardee=PITT")
        results = response.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]
        self.assertTrue(response is not None)
        self.assertEqual(len(results), len(self.pitt_org_pids))
        self.assertEqual(results_pid, self.pitt_org_pids)

        # Not passing in an awardee query param to the endpoint
        response = self.send_get(
            "AwardeeInSite", expected_status=http.client.BAD_REQUEST
        )
        self.assertTrue(response.status_code == 400)

    def test_response_values(self):
        self.overwrite_test_user_awardee(["awardee_sa"], "AZ_TUCSON")

        pid_for_az_org = 4866  # defined in the setUp method
        with self.awardee_insite_dao.session() as session:
            id_ = (
                session.query(AwardeeInSite.id)
                .filter(AwardeeInSite.participantId == pid_for_az_org)
                .first()
            )

        awardee_insite_values = {
            "id": id_,
            "primaryLanguage": "en",
            "deactivationStatus": "deactivated",
            "deactivationTime": "2024-11-21T18:12:00",
            "consentForElectronicHealthRecords": "no",
            "consentForElectronicHealthRecordsAuthored": "2024-11-21T18:12:00",
            "firstEhrReceiptTime": "2024-11-25T18:12:00",
            "latestEhrReceiptTime": "2024-11-26T18:12:00",
            "consentForStudyEnrollment": "yes",
            "consentForStudyEnrollmentAuthored": "2024-11-21T18:12:00",
            "patientStatus": [],
            "enrollmentStatus": "registered",
            "genderIdentity": "GenderIdentity_Man",
            "isEhrDataAvailable": "no",
            "questionnaireOnOverallHealth": "submitted_complete",
            "questionnaireOnOverallHealthAuthored": "2024-11-28T18:12:00",

        }
        self.awardee_insite_dao.upsert(AwardeeInSite(**awardee_insite_values))

        expected_result = {
            "participantId": "P4866",
            "firstName": "Jack",
            "middleName": "UNSET",
            "lastName": "Matt",
            "zipCode": "45490",
            "streetAddress": "184 Knox Ln",
            "streetAddress2": "Apt 67",
            "phoneNumber": "9843685667",
            "dateOfBirth": "1990-12-18",
            "primaryLanguage": "en",
            "withdrawalStatus": "not_withdrawn",
            "withdrawalTime": "UNSET",
            "deactivationStatus": "deactivated",
            "deactivationTime": "2024-11-21T18:12:00",
            "deceasedStatus": "UNSET",
            "deceasedAuthored": "UNSET",
            "clinicPhysicalMeasurementsStatus": "UNSET",
            "clinicPhysicalMeasurementsFinalizedTime": "UNSET",
            "clinicPhysicalMeasurementsFinalizedSite": "UNSET",
            "selfReportedPhysicalMeasurementsStatus": "UNSET",
            "selfReportedPhysicalMeasurementsAuthored": "UNSET",
            "consentForElectronicHealthRecords": "no",
            "consentForElectronicHealthRecordsAuthored": "2024-11-21T18:12:00",
            "consentForElectronicHealthRecordsFirstYesAuthored": "UNSET",
            "firstEhrReceiptTime": "2024-11-25T18:12:00",
            "latestEhrReceiptTime": "2024-11-26T18:12:00",
            "consentForStudyEnrollment": "yes",
            "consentForStudyEnrollmentAuthored": "2024-11-21T18:12:00",
            "patientStatus": [],
            "enrollmentStatus": "registered",
            "biospecimenSourceSite": "UNSET",
            "biospecimenOrderTime": "UNSET",
            "biospecimenStatus": "UNSET",
            "sample1SAL2CollectionMethod": "UNSET",
            "sampleStatus1SAL2": "UNSET",
            "sampleOrderStatus1SAL2": "UNSET",
            "sampleOrderStatus1SAL2Time": "UNSET",
            "state": "LA",
            "city": "Lafayette",
            "email": "jack_ma@example.com",
            "organization": "AZ_TUCSON_BANNER_HEALTH",
            "genderIdentity": "GenderIdentity_Man",
            "awardee": "AZ_TUCSON",
            "isEhrDataAvailable": "no",
            "aian": "UNSET",
            "questionnaireOnOverallHealth": "submitted_complete",
            "questionnaireOnOverallHealthAuthored": "2024-11-28T18:12:00",
            "questionnaireOnLifestyle": "UNSET",
            "questionnaireOnLifestyleAuthored": "UNSET",
            "questionnaireOnTheBasics": "UNSET",
            "questionnaireOnTheBasicsAuthored": "UNSET",
            "questionnaireOnHealthcareAccess": "UNSET",
            "questionnaireOnHealthcareAccessAuthored": "UNSET",
            "questionnaireOnSocialDeterminantsOfHealth": "UNSET",
            "questionnaireOnSocialDeterminantsOfHealthAuthored": "UNSET",
            "questionnaireOnPersonalAndFamilyHealthHistory": "UNSET",
            "questionnaireOnPersonalAndFamilyHealthHistoryAuthored": "UNSET",
            "questionnaireOnLifeFunctioning": "UNSET",
            "questionnaireOnLifeFunctioningAuthored": "UNSET",
            "questionnaireOnEmotionalHealthHistoryAndWellBeing": "UNSET",
            "questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored": "UNSET",
            "questionnaireOnBehavioralHealthAndPersonality": "UNSET",
            "questionnaireOnBehavioralHealthAndPersonalityAuthored": "UNSET",
        }

        response = self.send_get("AwardeeInSite")
        result = response.get("entry")[0]["resource"]

        print(result)

        self.assertEqual(result, expected_result)


    @patch(
        "rdr_service.api.awardee_insite_api.AWARDEE_INSITE_PAGINATION_MAX_RESULTS", 10
    )
    def test_all_participants_for_awardee_are_returned(self):
        self.overwrite_test_user_awardee(["awardee_sa"], "PITT")

        response = self.send_get("AwardeeInSite")
        results = response.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]

        self.assertTrue(response is not None)
        self.assertEqual(len(results), len(self.pitt_org_pids))
        self.assertEqual(results_pid, self.pitt_org_pids)

        # Test it for another Awardee
        self.overwrite_test_user_awardee(["awardee_sa"], "AZ_TUCSON")
        response = self.send_get("AwardeeInSite")
        results = response.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]

        self.assertTrue(response is not None)
        self.assertEqual(len(results), len(self.az_org_pids))
        self.assertEqual(results_pid, self.az_org_pids)

    @patch(
        "rdr_service.api.awardee_insite_api.AWARDEE_INSITE_PAGINATION_MAX_RESULTS", 3
    )
    def test_pagination_with_max_results(self):
        self.overwrite_test_user_awardee(["awardee_sa"], "PITT")
        page_size = 3  # Make sure this matches patched value in the patch decorator

        response_page_1 = self.send_get("AwardeeInSite")
        next_url = response_page_1["link"][0]["url"]
        results = response_page_1.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]

        self.assertTrue(response_page_1 is not None)
        self.assertEqual(len(results_pid), len(self.pitt_org_pids[:page_size]))
        self.assertEqual(results_pid, self.pitt_org_pids[:page_size])
        self.assertTrue(
            next_url is not None, "Next URL should exist if there's a next page"
        )

        # Get the 2nd page
        token = next_url.split("token=")[-1]
        url_with_token = f"AwardeeInSite?_token={token}"
        response_page_2 = self.send_get(url_with_token)
        response_2_url = response_page_2.get("link")
        results = response_page_2.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]

        self.assertTrue(response_page_2 is not None)
        self.assertIsNone(response_2_url, "Next URL should not exist in the last page")
        self.assertEqual(
            len(results_pid), len(self.pitt_org_pids[page_size : page_size + page_size])
        )
        self.assertEqual(
            results_pid, self.pitt_org_pids[page_size : page_size + page_size]
        )

    @patch(
        "rdr_service.api.awardee_insite_api.AWARDEE_INSITE_PAGINATION_MAX_RESULTS", 3
    )
    def test_pagination_with_includeTotal_parameter(self):
        self.overwrite_test_user_awardee(["awardee_sa"], "PITT")

        response_page_1 = self.send_get("AwardeeInSite?_includeTotal=True")
        next_url = response_page_1["link"][0]["url"]
        total = response_page_1.get("total")

        self.assertTrue(total is not None, "response should contain total")
        self.assertEqual(len(self.pitt_org_pids), total)

        # Get the 2nd page
        token = next_url.split("token=")[-1]
        url_with_token = f"AwardeeInSite?_token={token}&_includeTotal=True"
        response_page_2 = self.send_get(url_with_token)
        total = response_page_2.get("total")

        self.assertTrue(total is not None, "response should contain total")
        self.assertEqual(len(self.pitt_org_pids), total)

    @patch(
        "rdr_service.api.awardee_insite_api.AWARDEE_INSITE_PAGINATION_MAX_RESULTS", 10
    )
    def test_updated_since_parameter(self):
        """
        The API should have the ability to only return a set of
        participant's that have been modified since a specified date
        """
        self.overwrite_test_user_awardee(["awardee_sa"], "PITT")

        # This record should not be returned
        pid = 8000
        records = [
            {
                "participantId": pid,
                "firstName": "Erling",
                "lastName": "Roe",
                "organization": self.pitt_org_name,
            },
        ]

        with FakeClock(datetime(2024, 1, 1)):
            for record in records:
                self.awardee_insite_dao.insert(AwardeeInSite(**record))

        response = self.send_get("AwardeeInSite?updatedSince=2024-05-01")
        results = response.get("entry")
        results_pid = [
            int(ele["resource"]["participantId"].replace("P", "")) for ele in results
        ]

        self.assertNotIn(pid, results_pid)

    def test_hpo_without_any_participant_returns_none(self):
        awardee = "TEST-1"  # Not linked to any existing participants
        self.overwrite_test_user_awardee(["awardee_sa"], awardee)

        response = self.send_get("AwardeeInSite")
        self.assertTrue(response is not None)
        self.assertTrue(len(response["entry"]) == 0)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.awardee_insite")
