import datetime
import http.client

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.utils import from_client_participant_id
from rdr_service.clock import FakeClock
from rdr_service.code_constants import CONSENT_PERMISSION_YES_CODE, CONSENT_PERMISSION_NO_CODE, PPI_SYSTEM,\
    RACE_WHITE_CODE, PMI_SKIP_CODE
from rdr_service.concepts import Concept
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.model.code import Code
from rdr_service.model.hpo import HPO
from rdr_service.model.participant import Participant
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer
from rdr_service.model.site import Site
from rdr_service.participant_enums import (
    OrganizationType,
    SuspensionStatus,
    TEST_HPO_ID,
    TEST_HPO_NAME,
    WithdrawalStatus,
)
# For validating PDR /resource data generators
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.resource.generators import ParticipantSummaryGenerator, PDRParticipantSummaryGenerator

from tests.helpers.unittest_base import BaseTestCase, QUESTIONNAIRE_NONE_ANSWER
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

        self._ehr_questionnaire_id = None

    def test_participant_id_out_of_range(self):
        self.send_get("Participant/P12345678", expected_status=404)
        self.send_get("Participant/P1234567890", expected_status=404)

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
            "enrollmentSite",
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

    def test_pairing_is_case_insensitive(self):
        # Set the participant up
        participant = self.send_post('Participant', self.participant)

        # Change the site pairing information
        participant_site_code_sent = 'hpo-site-Monroeville'
        del participant['providerLink']
        participant['site'] = participant_site_code_sent

        # Re-pair using API
        participant_id = from_client_participant_id(participant["participantId"])
        self.send_put(f'Participant/P{participant_id}', participant, headers={"If-Match": 'W/"1"'})

        # Verify that the participant is paired correctly
        participant_site: Site = self.session.query(Site).join(
            Participant,
            Participant.siteId == Site.siteId
        ).filter(
            Participant.participantId == participant_id
        ).one_or_none()
        self.assertEqual(participant_site_code_sent.lower(), participant_site.googleGroup,
                         "Expecting the participant to be paired to Monroeville")

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

    def test_enrollment_site(self):
        participant = self.send_post("Participant", self.participant)
        participant["providerLink"] = [self.provider_link_2]
        participant_id = participant["participantId"]
        path = "Participant/%s" % participant_id

        update_1 = self.send_put(path, participant, headers={"If-Match": 'W/"1"'})
        participant["site"] = "hpo-site-bannerphoenix"
        update_2 = self.send_put(path, participant, headers={"If-Match": 'W/"2"'})
        self.assertEqual(update_1["site"], "UNSET")
        self.assertEqual(update_1["enrollmentSite"], "UNSET")
        self.assertEqual(update_2["site"], "hpo-site-bannerphoenix")
        self.assertEqual(update_2["enrollmentSite"], "hpo-site-bannerphoenix")

        self.send_consent(participant_id)
        ps = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(ps['enrollmentSite'], "hpo-site-bannerphoenix")

        participant["site"] = "hpo-site-clinic-phoenix"
        update_3 = self.send_put(path, participant, headers={"If-Match": 'W/"3"'})
        self.assertEqual(update_3["site"], "hpo-site-clinic-phoenix")
        # enrollmentSite will not change
        self.assertEqual(update_2["enrollmentSite"], "hpo-site-bannerphoenix")
        ps = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(ps['enrollmentSite'], "hpo-site-bannerphoenix")

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

        participant = self.send_get(path)
        self.assertEqual(participant["withdrawalStatus"], "NO_USE")

    def test_early_out_withdrawal(self):
        """If a participant withdraws before consent/participant summary it is called early out."""
        with FakeClock(TIME_1):
            response = self.send_post("Participant", self.participant)
            participant_id = response["participantId"]
            response["providerLink"] = [self.provider_link_2]
            response["withdrawalStatus"] = "EARLY_OUT"
            response["withdrawalTimeStamp"] = 1563907344169
            response["suspensionStatus"] = "NOT_SUSPENDED"
            response["withdrawalReason"] = "TEST"
            response["withdrawalReasonJustification"] = "This was a test account."
            path = "Participant/%s" % participant_id
            self.send_put(path, response, headers={"If-Match": 'W/"1"'})
            participant = self.send_get(path)
            self.assertEqual(participant["withdrawalStatus"], "EARLY_OUT")
            self.assertEqual(participant["withdrawalTime"], '2018-01-01T00:00:00')
            self.assertEqual(participant["withdrawalAuthored"], '2019-07-23T18:42:24')

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

        string_answers = [
            ("firstName", first_name),
            ("middleName", middle_name),
            ("lastName", last_name),
            ("city", city),
            ("phoneNumber", phone_number),
            ("loginPhoneNumber", login_phone_number),
            ("zipCode", zip_code),
        ]
        if street_address is not None:
            string_answers.append(("streetAddress", street_address))
        if street_address2 is not None:
            if street_address2 == PMI_SKIP_CODE:
                _add_code_answer(code_answers, "streetAddress2", street_address2)
            else:
                string_answers.append(("streetAddress2", street_address2))
        qr = self.make_questionnaire_response_json(
            participant_id,
            questionnaire_id,
            code_answers=code_answers,
            string_answers=string_answers,
            date_answers=[("dateOfBirth", date_of_birth)],
            uri_answers=[("CABoRSignature", cabor_signature_uri)],
        )
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def _setup_initial_participant_data(self):
        with FakeClock(TIME_1):
            participant = self.send_post("Participant", {"providerLink": [self.provider_link_2]})
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        participant_id = participant["participantId"]
        self.send_consent(participant_id)
        self.submit_questionnaire_response(
            participant_id,
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

        return participant_id, questionnaire_id

    def test_switch_to_test_account(self):
        participant_id, questionnaire_id = self._setup_initial_participant_data()

        # To test all data generator results for correct test_participant setting:
        # BQParticipantSummaryGenerator:   bigquery_sync resource JSON generator for pdr_participant table in BQ
        # ParticipantSummaryGenerator:  Full participant (including PII) for resource_data table (type_name:
        # participant)
        # PDRParticipantSummaryGenerator:  PDR participant data for resource_data table (type_name: pdr_participant),
        # takes the ParticipantSummaryGenerator.make_resource() output as an input parameter
        # BQPDRQuestionnaireResponseGenerator:  bigquery_sync resource JSON generator for pdr_mod_* table in BQ (e.g.
        # pdr_mod_consentpii or pdr_mod_thebasics).  Note:  no resource_data table content yet for module data
        ps_bq_gen = BQParticipantSummaryGenerator()
        ps_rsrc_gen = ParticipantSummaryGenerator()
        pdr_rsrc_gen = PDRParticipantSummaryGenerator()
        pdr_mod_bq_gen = BQPDRQuestionnaireResponseGenerator()
        # Strip 'P' from the test participant ID string (data generators use the bigint value)
        p_id = participant_id[1:]

        ps_1 = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual("215-222-2222", ps_1["loginPhoneNumber"])
        self.assertEqual("PITT", ps_1["hpoId"])

        p_1 = self.send_get("Participant/%s" % participant_id)
        self.assertEqual("PITT", p_1["hpoId"])
        self.assertEqual(TIME_1.strftime("%Y" "-" "%m" "-" "%d" "T" "%X"), p_1["lastModified"])
        self.assertEqual('W/"1"', p_1["meta"]["versionId"])

        # Test all the PDR / resource generator results while participant not considered a test participant
        ps_bqs_data = ps_bq_gen.make_bqrecord(p_id).to_dict(serialize=True)
        ps_rsc = ps_rsrc_gen.make_resource(p_id)
        pdr_rsc_data = pdr_rsrc_gen.make_resource(p_id, ps_rsc)
        self.assertEqual(ps_bqs_data.get('test_participant'), 0)
        self.assertEqual(ps_rsc.get_data().get('test_participant'), 0)
        self.assertEqual(pdr_rsc_data.get_data().get('test_participant'), 0)

        table, pdr_mod_bqsr = pdr_mod_bq_gen.make_bqrecord(p_id, 'ConsentPII', latest=True)
        self.assertIsNotNone(table)
        self.assertEqual(pdr_mod_bqsr[0].test_participant, 0)
        table, pdr_mod_bqsr = pdr_mod_bq_gen.make_bqrecord(p_id, 'TheBasics', latest=True)
        self.assertIsNotNone(table)
        self.assertEqual(pdr_mod_bqsr[0].test_participant, 0)

        # change login phone number to 444-222-2222
        self.submit_questionnaire_response(
            participant_id,
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

        ps_1_with_test_login_phone_number = self.send_get("Participant/%s/Summary" % participant_id)

        self.assertEqual("444-222-2222", ps_1_with_test_login_phone_number["loginPhoneNumber"])
        self.assertEqual("TEST", ps_1_with_test_login_phone_number["hpoId"])
        self.assertEqual("1234 Main Street", ps_1_with_test_login_phone_number["streetAddress"])
        self.assertEqual("APT C", ps_1_with_test_login_phone_number["streetAddress2"])

        p_1 = self.send_get("Participant/%s" % participant_id)
        self.assertEqual("TEST", p_1["hpoId"])
        self.assertEqual(TIME_2.strftime("%Y" "-" "%m" "-" "%d" "T" "%X"), p_1["lastModified"])
        self.assertEqual('W/"2"', p_1["meta"]["versionId"])

        # Retest all the PDR / resource generator results after participant is updated with test participant data
        ps_bqs_data = ps_bq_gen.make_bqrecord(p_id).to_dict(serialize=True)
        ps_rsc = ps_rsrc_gen.make_resource(p_id)
        pdr_rsc_data = pdr_rsrc_gen.make_resource(p_id, ps_rsc)
        self.assertEqual(ps_bqs_data.get('test_participant'), 1)
        self.assertEqual(ps_rsc.get_data().get('test_participant'), 1)
        self.assertEqual(pdr_rsc_data.get_data().get('test_participant'), 1)

        table, pdr_mod_bqsr = pdr_mod_bq_gen.make_bqrecord(p_id, 'ConsentPII', latest=True)
        self.assertIsNotNone(table)
        self.assertEqual(pdr_mod_bqsr[0].test_participant, 1)
        table, pdr_mod_bqsr = pdr_mod_bq_gen.make_bqrecord(p_id, 'TheBasics', latest=True)
        self.assertIsNotNone(table)
        self.assertEqual(pdr_mod_bqsr[0].test_participant, 1)


    def test_street_address_two_clears_on_address_update(self):
        participant_id, questionnaire_id = self._setup_initial_participant_data()

        # Change street address to only have one line
        self.submit_questionnaire_response(
            participant_id,
            questionnaire_id,
            RACE_WHITE_CODE,
            "male",
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            "44 Hickory Lane",
            "",
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

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual("", participant_summary["streetAddress2"])

    def test_street_address_two_clears_on_no_answer(self):
        participant_id, questionnaire_id = self._setup_initial_participant_data()

        # We could see a submission to the street address line 2 without an answer included with it
        self.submit_questionnaire_response(
            participant_id,
            questionnaire_id,
            RACE_WHITE_CODE,
            "male",
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            "44 Hickory Lane",
            QUESTIONNAIRE_NONE_ANSWER,
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

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertNotIn("streetAddress2", participant_summary)

        # Make sure the street address 2 answer is set inactive too
        street_address_2_active_answer = self.session.query(QuestionnaireResponseAnswer)\
                                             .join(QuestionnaireQuestion)\
                                             .join(Code, Code.codeId == QuestionnaireQuestion.codeId)\
                                             .filter(Code.value == 'PIIAddress_StreetAddress2',
                                                     QuestionnaireResponseAnswer.endTime.is_(None))\
                                             .one_or_none()
        self.assertIsNone(street_address_2_active_answer)


    def test_street_address_two_clears_on_skip(self):
        participant_id, questionnaire_id = self._setup_initial_participant_data()

        # We could see a submission to the street address line 2 without an answer included with it
        self.submit_questionnaire_response(
            participant_id,
            questionnaire_id,
            RACE_WHITE_CODE,
            "male",
            "Bob",
            "Q",
            "Jones",
            "78751",
            "PIIState_VA",
            "44 Hickory Lane",
            PMI_SKIP_CODE,
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

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertNotIn("streetAddress2", participant_summary)

    def test_first_study_consent_time_set(self):
        with FakeClock(TIME_1):
            participant = self.send_post("Participant", {"providerLink": [self.provider_link_2]})
        participant_id = participant["participantId"]

        with FakeClock(datetime.datetime(2020, 6, 1)):
            self.send_consent(participant_id)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual('2020-06-01T00:00:00', participant_summary['consentForStudyEnrollmentFirstYesAuthored'])

    def test_first_study_consent_not_modified(self):
        with FakeClock(TIME_1):
            participant = self.send_post("Participant", {"providerLink": [self.provider_link_2]})
        participant_id = participant["participantId"]

        with FakeClock(datetime.datetime(2020, 6, 1)):
            self.send_consent(participant_id)
        with FakeClock(datetime.datetime(2020, 8, 1)):
            self.send_consent(participant_id)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual('2020-06-01T00:00:00', participant_summary['consentForStudyEnrollmentFirstYesAuthored'])

    def submit_ehr_questionnaire(self, participant_id, ehr_response_code):
        if not self._ehr_questionnaire_id:
            self._ehr_questionnaire_id = self.create_questionnaire("ehr_consent_questionnaire.json")

        code_answers = []
        _add_code_answer(code_answers, 'ehrConsent', ehr_response_code)
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            self._ehr_questionnaire_id,
            code_answers=code_answers,
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)

    def test_first_ehr_consent_time_set(self):
        participant_id, _ = self._setup_initial_participant_data()
        with FakeClock(datetime.datetime(2020, 3, 12)):
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_YES_CODE)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual('2020-03-12T00:00:00',
                         participant_summary['consentForElectronicHealthRecordsFirstYesAuthored'])

    def test_first_ehr_consent_not_modified(self):
        participant_id, _ = self._setup_initial_participant_data()

        with FakeClock(datetime.datetime(2020, 3, 12)):
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_YES_CODE)
        with FakeClock(datetime.datetime(2020, 9, 12)):
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_YES_CODE)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual('2020-03-12T00:00:00',
                         participant_summary['consentForElectronicHealthRecordsFirstYesAuthored'])

    def test_first_ehr_consent_not_set_on_no(self):
        participant_id, _ = self._setup_initial_participant_data()

        with FakeClock(datetime.datetime(2020, 3, 12)):
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_NO_CODE)

        participant_summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertNotIn('consentForElectronicHealthRecordsFirstYesAuthored', participant_summary)

    def test_pid_rid_mapping_api(self):
        self.send_post("Participant", {"providerLink": [self.provider_link_2]})
        result = self.send_get("ParticipantId/ResearchId/Mapping?signUpAfter=2020-01-01&sort=lastModified")
        self.assertEqual(len(result['data']), 1)
        self.assertTrue(isinstance(result['data'][0]['research_id'], int))
        self.assertEqual(len(str(result['data'][0]['research_id'])), 7)
        self.assertEqual(result['sort_by'], 'lastModified')

    def test_new_participant_with_test_participant_flag(self):
        org = self.data_generator.create_database_organization(externalId='test_org')
        site = self.data_generator.create_database_site(googleGroup='test_site')
        response = self.send_post("Participant", {
            'testParticipant': True,
            'organization': org.externalId,
            'site': site.googleGroup
        })
        participant_id = from_client_participant_id(response['participantId'])

        participant: Participant = self.session.query(Participant).filter(
            Participant.participantId == participant_id
        ).one()
        self.assertTrue(participant.isTestParticipant)

        # make sure the participant is paired correctly (that what was sent was ignored)
        self.assertEqual(TEST_HPO_ID, participant.hpoId)
        self.assertIsNone(participant.organizationId)
        self.assertIsNone(participant.siteId)

    def test_update_existing_participant_as_test_participant_flag(self):
        hpo = self.data_generator.create_database_hpo()
        org = self.data_generator.create_database_organization(externalId='test_org')
        site = self.data_generator.create_database_site(googleGroup='test_site')
        participant = self.data_generator.create_database_participant(
            siteId=site.siteId,
            organizationId=org.organizationId,
            hpoId=hpo.hpoId
        )

        # When updating as a test participant, only the testParticipant field should need to be sent
        self.send_put(f"Participant/P{participant.participantId}", {
            'testParticipant': True
        }, headers={"If-Match": 'W/"1"'})

        self.session.expire_all()
        updated_participant: Participant = self.session.query(Participant).filter(
            Participant.participantId == participant.participantId
        ).one()
        self.assertTrue(updated_participant.isTestParticipant)

        # make sure the participant is paired correctly (that the org and site are cleared, and the TEST hpo is used)
        self.assertEqual(TEST_HPO_ID, updated_participant.hpoId)
        self.assertIsNone(updated_participant.organizationId)
        self.assertIsNone(updated_participant.siteId)

def _add_code_answer(code_answers, link_id, code):
    if code:
        code_answers.append((link_id, Concept(PPI_SYSTEM, code)))
