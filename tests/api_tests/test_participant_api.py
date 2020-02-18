import datetime
import http.client

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.utils import from_client_participant_id
from rdr_service.clock import FakeClock
from rdr_service.code_constants import PPI_SYSTEM, RACE_WHITE_CODE
from rdr_service.concepts import Concept
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.model.hpo import HPO
from rdr_service.participant_enums import (
    OrganizationType,
    SuspensionStatus,
    TEST_HPO_ID,
    TEST_HPO_NAME,
    WithdrawalStatus,
)
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import load_biobank_order_json

TIME_1 = datetime.datetime(2018, 1, 1)
TIME_2 = datetime.datetime(2018, 1, 3)


class ParticipantApiTest(BaseTestCase):
    def setUp(self):
        super(ParticipantApiTest, self).setUp()
        provider_link = {"primary": False, "organization": {"reference": "columbia"}}
        self.participant = {"providerLink": [provider_link]}
        self.participant_2 = {"externalId": 12345}
        self.provider_link_2 = {"primary": True, "organization": {"reference": "Organization/PITT"}}
        self.summary_dao = ParticipantSummaryDao()

        # Needed by test_switch_to_test_account
        self.hpo_dao = HPODao()
        self.hpo_dao.insert(
            HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName="Test", organizationType=OrganizationType.UNSET)
        )
        self.order = BiobankOrderDao()

    def test_participant_id_out_of_range(self):
        response = self.send_get(f"Participant/P{12345678}", expected_status=404)
        self.assertEqual(None, response)

        response = self.send_get("Participant/P1234567890", expected_status=404)
        self.assertEqual(None, response)

    def test_insert(self):
        response = self.send_post("Participant", self.participant)
        participant_id = response["participantId"]
        get_response = self.send_get("Participant/%s" % participant_id)
        self.assertEqual(response, get_response)
        biobank_id = response["biobankId"]
        self.assertTrue(biobank_id.startswith("Z"))
        self.assertEqual(str(WithdrawalStatus.NOT_WITHDRAWN), response["withdrawalStatus"])
        self.assertEqual(str(SuspensionStatus.NOT_SUSPENDED), response["suspensionStatus"])
        for auto_generated in (
            "participantId",
            "externalId",
            "site",
            "organization",
            "awardee",
            "hpoId",
            "biobankId",
            "signUpTime",
            "lastModified",
            "withdrawalStatus",
            "withdrawalReason",
            "withdrawalAuthored",
            "withdrawalReasonJustification",
            "suspensionStatus",
        ):
            del response[auto_generated]

        self.assertJsonResponseMatches(self.participant, response)

    def test_insert_with_same_external_id_returns_existing_participant(self):
        response = self.send_post("Participant", self.participant_2)
        participant_id = response["participantId"]
        get_response = self.send_get("Participant/%s" % participant_id)
        self.assertEqual(get_response["externalId"], self.participant_2["externalId"])
        self.assertEqual(response, get_response)
        response_2 = self.send_post("Participant", self.participant_2)
        self.assertEqual(response, response_2)

    def test_update_no_ifmatch_specified(self):
        response = self.send_post("Participant", self.participant)

        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        path = "Participant/%s" % participant_id
        self.send_put(path, response, expected_status=http.client.BAD_REQUEST)

    def test_update_wrong_origin_fails(self):
        response = self.send_post("Participant", self.participant)

        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        path = "Participant/%s" % participant_id
        BaseTestCase.switch_auth_user('example@spellman.com', 'vibrent')
        self.send_put(path, response, headers={"If-Match": 'W/"1"'}, expected_status=http.client.BAD_REQUEST)
        BaseTestCase.switch_auth_user('example@example.com', 'example')

    def test_update_hpro_can_edit(self):
        response = self.send_post("Participant", self.participant)

        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        path = "Participant/%s" % participant_id
        BaseTestCase.switch_auth_user('example@spellman.com', 'hpro')
        self.send_put(path, response, headers={"If-Match": 'W/"1"'})

    def test_update_bad_ifmatch_specified(self):
        response = self.send_post("Participant", self.participant)

        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        path = "Participant/%s" % participant_id
        self.send_put(path, response, headers={"If-Match": "Blah"}, expected_status=http.client.BAD_REQUEST)

    def test_update_wrong_ifmatch_specified(self):
        response = self.send_post("Participant", self.participant)

        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        path = "Participant/%s" % participant_id
        self.send_put(path, response, headers={"If-Match": 'W/"123"'}, expected_status=http.client.PRECONDITION_FAILED)

    def test_update_right_ifmatch_specified(self):
        response = self.send_post("Participant", self.participant)
        self.assertEqual('W/"1"', response["meta"]["versionId"])
        # Change the provider link for the participant
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        response["withdrawalStatus"] = "NO_USE"
        response["suspensionStatus"] = "NO_CONTACT"
        response["site"] = "UNSET"
        response["organization"] = "UNSET"
        response["awardee"] = "PITT"
        response["hpoId"] = "PITT"
        path = "Participant/%s" % participant_id
        update_response = self.send_put(path, response, headers={"If-Match": 'W/"1"'})
        response["meta"]["versionId"] = 'W/"2"'
        response["withdrawalTime"] = update_response["lastModified"]
        response["suspensionTime"] = update_response["lastModified"]
        self.assertJsonResponseMatches(response, update_response)

    def test_update_right_suspension_status(self):
        response = self.send_post("Participant", self.participant)
        self.assertEqual('W/"1"', response["meta"]["versionId"])
        participant_id = response["participantId"]
        response["providerLink"] = [self.provider_link_2]
        response["suspensionStatus"] = "NO_CONTACT"
        response["site"] = "UNSET"
        response["organization"] = "UNSET"
        response["awardee"] = "PITT"
        response["hpoId"] = "PITT"
        path = "Participant/%s" % participant_id
        self.send_put(path, response, headers={"If-Match": 'W/"1"'})

        response["suspensionStatus"] = "NOT_SUSPENDED"
        response["meta"]["versionId"] = 'W/"3"'
        response["withdrawalTime"] = None
        response["withdrawalStatus"] = 'NOT_WITHDRAWN'

        with FakeClock(TIME_1):
            update_response = self.send_put(path, response, headers={"If-Match": 'W/"2"'})
        self.assertEqual(update_response['suspensionStatus'], 'NOT_SUSPENDED')
        self.assertEqual(update_response['withdrawalStatus'], 'NOT_WITHDRAWN')
        self.assertNotIn('suspensionTime', update_response)

    def test_change_pairing_awardee_and_site(self):
        participant = self.send_post("Participant", self.participant)
        participant["providerLink"] = [self.provider_link_2]
        participant_id = participant["participantId"]
        participant["awardee"] = "PITT"
        participant["site"] = "hpo-site-monroeville"
        path = "Participant/%s" % participant_id
        update_awardee = self.send_put(path, participant, headers={"If-Match": 'W/"1"'})
        self.assertEqual(participant["awardee"], update_awardee["awardee"])

    def test_change_pairing_for_org_then_site(self):
        participant = self.send_post("Participant", self.participant)
        participant["providerLink"] = [self.provider_link_2]
        participant_id = participant["participantId"]
        path = "Participant/%s" % participant_id

        update_1 = self.send_put(path, participant, headers={"If-Match": 'W/"1"'})
        participant["site"] = "hpo-site-bannerphoenix"
        update_2 = self.send_put(path, participant, headers={"If-Match": 'W/"2"'})
        self.assertEqual(update_1["site"], "UNSET")
        self.assertEqual(update_1["organization"], "UNSET")
        self.assertEqual(update_2["site"], "hpo-site-bannerphoenix")
        self.assertEqual(update_2["organization"], "PITT_BANNER_HEALTH")
        participant["organization"] = "AZ_TUCSON_BANNER_HEALTH"
        update_3 = self.send_put(path, participant, headers={"If-Match": 'W/"3"'})
        self.assertEqual(update_2["hpoId"], update_3["hpoId"])
        self.assertEqual(update_2["organization"], update_3["organization"])
        self.assertEqual(update_3["site"], "hpo-site-bannerphoenix")
        participant["site"] = "hpo-site-clinic-phoenix"
        update_4 = self.send_put(path, participant, headers={"If-Match": 'W/"4"'})
        self.assertEqual(update_4["site"], "hpo-site-clinic-phoenix")
        self.assertEqual(update_4["organization"], "AZ_TUCSON_BANNER_HEALTH")
        self.assertEqual(update_4["awardee"], "AZ_TUCSON")

    def test_repairing_after_biobank_order(self):
        participant = self.send_post("Participant", self.participant)
        participant["providerLink"] = [self.provider_link_2]
        participant_id = participant["participantId"]
        participant_path = "Participant/%s" % participant_id
        update_1 = self.send_put(participant_path, participant, headers={"If-Match": 'W/"1"'})
        self.assertEqual(update_1["site"], "UNSET")
        self.assertEqual(update_1["organization"], "UNSET")
        self.assertEqual(update_1["hpoId"], "PITT")

        participant["site"] = "hpo-site-bannerphoenix"
        update_2 = self.send_put(participant_path, participant, headers={"If-Match": 'W/"2"'})
        self.assertEqual(update_2["site"], "hpo-site-bannerphoenix")
        self.assertEqual(update_2["organization"], "PITT_BANNER_HEALTH")
        self.assertEqual(update_2["hpoId"], "PITT")

        self.send_consent(participant_id)
        bio_path = "Participant/%s/BiobankOrder" % participant_id
        order_json = load_biobank_order_json(from_client_participant_id(participant_id),
                                             filename="biobank_order_2.json")
        self.send_post(bio_path, order_json)

        participant["site"] = None
        participant["awardee"] = "AZ_TUCSON"
        participant["organization"] = None
        update_3 = self.send_put(participant_path, participant, headers={"If-Match": 'W/"4"'})
        self.assertEqual(update_3["site"], "UNSET")
        self.assertEqual(update_3["organization"], "UNSET")
        self.assertEqual(update_3["hpoId"], "AZ_TUCSON")

    def test_administrative_withdrawal(self):
        with FakeClock(TIME_1):
            response = self.send_post("Participant", self.participant)
            participant_id = response["participantId"]
            response["providerLink"] = [self.provider_link_2]
            response["withdrawalStatus"] = "NO_USE"
            response["suspensionStatus"] = "NO_CONTACT"
            response["withdrawalReason"] = "TEST"
            response["withdrawalReasonJustification"] = "This was a test account."
            path = "Participant/%s" % participant_id
            update_response = self.send_put(path, response, headers={"If-Match": 'W/"1"'})

        with FakeClock(TIME_2):
            response["meta"]["versionId"] = 'W/"2"'
            response["withdrawalTime"] = update_response["lastModified"]
            response["suspensionTime"] = update_response["lastModified"]
            response["awardee"] = "PITT"
            response["hpoId"] = "PITT"
            self.assertJsonResponseMatches(response, update_response)

    def test_administrative_withdrawal_with_authored_time(self):
        with FakeClock(TIME_1):
            response = self.send_post("Participant", self.participant)
            participant_id = response["participantId"]
            self.send_consent(participant_id)
            response["providerLink"] = [self.provider_link_2]
            response["withdrawalStatus"] = "NO_USE"
            response["suspensionStatus"] = "NO_CONTACT"
            response["withdrawalReason"] = "TEST"
            response["withdrawalTimeStamp"] = 1563907344000
            response["withdrawalReasonJustification"] = "This was a test account."

            path = "Participant/%s" % participant_id
            update_response = self.send_put(path, response, headers={"If-Match": 'W/"1"'})

        with FakeClock(TIME_2):
            del response["withdrawalTimeStamp"]
            response["meta"]["versionId"] = 'W/"2"'
            response["withdrawalTime"] = update_response["lastModified"]
            response["suspensionTime"] = update_response["lastModified"]
            response["withdrawalAuthored"] = update_response["withdrawalAuthored"]
            response["awardee"] = "PITT"
            response["hpoId"] = "PITT"
            self.assertJsonResponseMatches(response, update_response)

            p_response = self.send_get("Participant/%s" % participant_id)
            self.assertEqual(p_response["withdrawalAuthored"], update_response["withdrawalAuthored"])

            ps_response = self.send_get("Participant/%s/Summary" % participant_id)
            self.assertEqual(ps_response["withdrawalAuthored"], update_response["withdrawalAuthored"])

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

    def test_switch_to_test_account(self):
        with FakeClock(TIME_1):
            participant_1 = self.send_post("Participant", {"providerLink": [self.provider_link_2]})
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        participant_id_1 = participant_1["participantId"]
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

        p_1 = self.send_get("Participant/%s" % participant_id_1)
        self.assertEqual("PITT", p_1["hpoId"])
        self.assertEqual(TIME_1.strftime("%Y" "-" "%m" "-" "%d" "T" "%X"), p_1["lastModified"])
        self.assertEqual('W/"1"', p_1["meta"]["versionId"])

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
            TIME_2,
        )

        ps_1_with_test_login_phone_number = self.send_get("Participant/%s/Summary" % participant_id_1)

        self.assertEqual("444-222-2222", ps_1_with_test_login_phone_number["loginPhoneNumber"])
        self.assertEqual("TEST", ps_1_with_test_login_phone_number["hpoId"])
        self.assertEqual("1234 Main Street", ps_1_with_test_login_phone_number["streetAddress"])
        self.assertEqual("APT C", ps_1_with_test_login_phone_number["streetAddress2"])

        p_1 = self.send_get("Participant/%s" % participant_id_1)
        self.assertEqual("TEST", p_1["hpoId"])
        self.assertEqual(TIME_2.strftime("%Y" "-" "%m" "-" "%d" "T" "%X"), p_1["lastModified"])
        self.assertEqual('W/"2"', p_1["meta"]["versionId"])


def _add_code_answer(code_answers, link_id, code):
    if code:
        code_answers.append((link_id, Concept(PPI_SYSTEM, code)))
