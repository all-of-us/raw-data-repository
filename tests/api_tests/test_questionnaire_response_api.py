import datetime
from dateutil.parser import parse
import http.client
import json
import mock
import pytz
from sqlalchemy import or_
from sqlalchemy.orm.session import make_transient

from rdr_service.clock import FakeClock
from rdr_service import config
from rdr_service.code_constants import CONSENT_PERMISSION_YES_CODE, PPI_SYSTEM, \
    CONSENT_PERMISSION_NO_CODE, GENDER_MAN_CODE, GENDER_WOMAN_CODE, GENDER_TRANSGENDER_CODE
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_summary_dao import ParticipantGenderAnswersDao, ParticipantRaceAnswersDao, \
    ParticipantSummaryDao
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import from_client_biobank_id
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.questionnaire_dao import QuestionnaireDao
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseAnswerDao, QuestionnaireResponseDao
from rdr_service.model.code import Code
from rdr_service.model.consent_response import ConsentResponse, ConsentType
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer,\
    QuestionnaireResponseExtension
from rdr_service.model.participant_summary import ParticipantSummary, WithdrawalStatus
from rdr_service.model.utils import from_client_participant_id, to_client_participant_id
from rdr_service.participant_enums import QuestionnaireDefinitionStatus, QuestionnaireResponseStatus,\
    ParticipantCohort, ParticipantCohortPilotFlag


from tests.api_tests.test_participant_summary_api import participant_summary_default_values,\
    participant_summary_default_values_no_basics
from tests.test_data import data_path, load_biobank_order_json
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin, BiobankTestMixin
from rdr_service.concepts import Concept
from rdr_service.code_constants import RACE_NONE_OF_THESE_CODE

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_4 = datetime.datetime(2016, 1, 4)


def _questionnaire_response_url(participant_id):
    return "Participant/%s/QuestionnaireResponse" % participant_id


class QuestionnaireResponseApiTest(BaseTestCase, BiobankTestMixin, PDRGeneratorTestMixin):

    def setUp(self):
        super(QuestionnaireResponseApiTest, self).setUp()
        self._ehr_questionnaire_id = None
        self.dao = QuestionnaireResponseDao()

    def test_duplicate_consent_submission(self):
        """
    Submit duplicate study enrollment questionnaires, so we can make sure
    a duplicate submission doesn't error out and the authored timestamp will
    not be updated if the answer is not changed.
    """
        participant_id = self.create_participant()
        authored_1 = datetime.datetime(2019, 3, 16, 1, 39, 33, tzinfo=pytz.utc)
        created = datetime.datetime(2019, 3, 16, 1, 51, 22)
        with FakeClock(created):
            self.send_consent(participant_id, authored=authored_1)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(parse(summary["consentForStudyEnrollmentTime"]), created.replace(tzinfo=None))
        self.assertEqual(parse(summary["consentForStudyEnrollmentAuthored"]), authored_1.replace(tzinfo=None))

        # submit consent questionnaire again with new timestamps.
        authored_2 = datetime.datetime(2019, 3, 17, 1, 24, 16, tzinfo=pytz.utc)
        with FakeClock(datetime.datetime(2019, 3, 17, 1, 25, 58)):
            self.send_consent(participant_id, authored=authored_2)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        # created should remain the same as the first submission.
        self.assertEqual(parse(summary["consentForStudyEnrollmentTime"]), created.replace(tzinfo=None))
        self.assertEqual(parse(summary["consentForStudyEnrollmentAuthored"]), authored_1.replace(tzinfo=None))

    def test_consent_submission_requires_signature(self):
        """Consent questionnaire responses should only mark a participant as consented if they have consented"""
        # Set the config up to imitate a server environment enough for the dao to check for consent files
        previous_config_project_setting = config.GAE_PROJECT
        config.GAE_PROJECT = 'test-environment'

        participant_id = self.create_participant()
        self.send_consent(participant_id, authored=datetime.datetime.now(), string_answers=[
            ('firstName', 'Bob'),
            ('lastName', 'Smith'),
            ('email', 'email@example.com')
        ], expected_status=400, send_consent_file_extension=False)

        summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == from_client_participant_id(participant_id)
        ).one_or_none()
        self.assertIsNone(summary)

        # Set the config back so that the rest of the tests are ok
        config.GAE_PROJECT = previous_config_project_setting

    def test_basics_profile_update(self):
        """ Participant summary should not be updated with TheBasics details if it was a profile update payload """
        participant_id = self.create_participant()
        with FakeClock(TIME_1):
            self.send_consent(participant_id, authored=TIME_1)
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('questionnaireOnTheBasicsAuthored'), None)
        self.assertEqual(summary.get('numCompletedBaselinePPIModules'), 0)

        # Submit a payload that only contains profile update content
        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json("questionnaire_the_basics_profile_update_resp.json",
                                            questionnaire_id, participant_id)
        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        # Confirm participant_summary did not record a TheBasics response
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('questionnaireOnTheBasicsAuthored'), None)
        self.assertEqual(summary.get('numCompletedBaselinePPIModules'), 0)

        # Now submit a full TheBasics survey response and confirm participant_summary is updated with its details
        resource = self._load_response_json("questionnaire_the_basics_resp.json",
                                            questionnaire_id, participant_id)
        with FakeClock(TIME_3):
            resource["authored"] = TIME_3.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('questionnaireOnTheBasicsAuthored'), TIME_3.isoformat())
        self.assertEqual(summary.get('numCompletedBaselinePPIModules'), 1)

    def test_update_baseline_questionnaires_first_complete_authored(self):
        participant_id = self.create_participant()
        with FakeClock(TIME_1):
            self.send_consent(participant_id, authored=TIME_1)
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('baselineQuestionnairesFirstCompleteAuthored'), None)

        summary_dao = ParticipantSummaryDao()
        summary_obj = summary_dao.get(participant_id[1:])
        summary_obj.questionnaireOnLifestyleAuthored = TIME_1
        summary_obj.questionnaireOnOverallHealthAuthored = TIME_2
        summary_dao.update(summary_obj)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json("questionnaire_the_basics_resp.json", questionnaire_id, participant_id)
        with FakeClock(TIME_3):
            resource["authored"] = TIME_3.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('baselineQuestionnairesFirstCompleteAuthored'), TIME_3.isoformat())

    def test_remote_pm_imperial_response(self):
        questionnaire_id = self.create_questionnaire("questionnaire3.json")
        questionnaire_id_1 = self.create_questionnaire("all_consents_questionnaire.json")
        questionnaire_id_2 = self.create_questionnaire("questionnaire4.json")
        participant_1 = self.send_post("Participant", {})
        participant_id = participant_1["participantId"]
        authored_1 = datetime.datetime(2019, 3, 16, 1, 39, 33, tzinfo=pytz.utc)
        created = datetime.datetime(2019, 3, 16, 1, 51, 22)
        with FakeClock(created):
            self.send_consent(participant_id, authored=authored_1)

        self._submit_consent_questionnaire_response(
            participant_id, questionnaire_id_1, CONSENT_PERMISSION_YES_CODE, time=created
        )

        # Send a biobank order for participant
        order_json = load_biobank_order_json(int(participant_id[1:]))
        self._send_biobank_order(participant_id, order_json, time=TIME_1)

        self.submit_response(
            participant_id,
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
        # completing the baseline PPI modules.
        self._submit_empty_questionnaire_response(participant_id, questionnaire_id_2)
        # Store samples for DNA for participants
        self._store_biobank_sample(participant_1, "1SAL", time=TIME_1)
        self._store_biobank_sample(participant_1, "2ED10", time=TIME_1)
        # Update participant summaries based on these changes
        # So it could trigger the participant_summary_dao.calculate_max_core_sample_time to compare the
        # clinicPhysicalMeasurementsFinalizedTime with other times to cover the bug scenario:
        # TypeError("can't compare offset-naive and offset-aware datetimes")
        ps_dao = ParticipantSummaryDao()
        ps_dao.update_from_biobank_stored_samples(biobank_ids=[from_client_biobank_id(participant_1['biobankId'])])

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary["selfReportedPhysicalMeasurementsStatus"], 'UNSET')
        self.assertEqual(summary["enrollmentStatus"], 'CORE_MINUS_PM')
        remote_pm_questionnaire_id = self.create_questionnaire("remote_pm_questionnaire.json")

        resource = self._load_response_json("remote_pm_response_imperial.json", remote_pm_questionnaire_id,
                                            participant_id)
        with FakeClock(TIME_2):
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary['selfReportedPhysicalMeasurementsStatus'], 'COMPLETED')
        self.assertEqual(summary['selfReportedPhysicalMeasurementsAuthored'], '2022-06-01T18:23:57')
        self.assertEqual(summary["enrollmentStatus"], 'FULL_PARTICIPANT')

        response = self.send_get("Participant/{0}/PhysicalMeasurements".format(participant_id))
        self.assertEqual(1, len(response["entry"]))
        self.assertEqual(response["entry"][0]["resource"]["collectType"], 'SELF_REPORTED')
        self.assertEqual(response["entry"][0]["resource"]["originMeasurementUnit"], 'IMPERIAL')
        self.assertEqual(response["entry"][0]["resource"]["origin"], 'vibrent')
        self.assertEqual(len(response["entry"][0]["resource"]["entry"]), 2)
        self.assertEqual(response["entry"][0]["resource"]["entry"][0]['resource']['status'], 'final')
        self.assertEqual(response["entry"][0]["resource"]["entry"][0]['resource']['valueQuantity'],
                         {
                             "code": "cm",
                             "unit": "cm",
                             "value": 172.7,
                             "system": "http://unitsofmeasure.org"
                         }
                         )
        self.assertEqual(response["entry"][0]["resource"]["entry"][0]['resource']['effectiveDateTime'],
                         '2022-06-01T18:23:57')
        self.assertEqual(response["entry"][0]["resource"]["entry"][1]['resource']['status'], 'final')
        self.assertEqual(response["entry"][0]["resource"]["entry"][1]['resource']['valueQuantity'],
                         {
                             "code": "kg",
                             "unit": "kg",
                             "value": 72.8,
                             "system": "http://unitsofmeasure.org"
                         }
                         )
        self.assertEqual(response["entry"][0]["resource"]["entry"][1]['resource']['effectiveDateTime'],
                         '2022-06-01T18:23:57')

    def test_remote_pm_metric_response(self):
        participant_id = self.create_participant()
        authored_1 = datetime.datetime(2019, 3, 16, 1, 39, 33, tzinfo=pytz.utc)
        created = datetime.datetime(2019, 3, 16, 1, 51, 22)
        with FakeClock(created):
            self.send_consent(participant_id, authored=authored_1)
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary["selfReportedPhysicalMeasurementsStatus"], 'UNSET')

        remote_pm_questionnaire_id = self.create_questionnaire("remote_pm_questionnaire.json")

        resource = self._load_response_json("remote_pm_response_metric.json", remote_pm_questionnaire_id,
                                            participant_id)
        with FakeClock(TIME_2):
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary['selfReportedPhysicalMeasurementsStatus'], 'COMPLETED')
        self.assertEqual(summary['selfReportedPhysicalMeasurementsAuthored'], '2022-06-01T18:26:08')

        response = self.send_get("Participant/{0}/PhysicalMeasurements".format(participant_id))
        self.assertEqual(1, len(response["entry"]))
        self.assertEqual(response["entry"][0]["resource"]["collectType"], 'SELF_REPORTED')
        self.assertEqual(response["entry"][0]["resource"]["originMeasurementUnit"], 'METRIC')
        self.assertEqual(response["entry"][0]["resource"]["origin"], 'vibrent')
        self.assertEqual(len(response["entry"][0]["resource"]["entry"]), 2)
        self.assertEqual(response["entry"][0]["resource"]["entry"][0]['resource']['status'], 'final')
        self.assertEqual(response["entry"][0]["resource"]["entry"][0]['resource']['valueQuantity'],
                         {
                             "code": "cm",
                             "unit": "cm",
                             "value": 170,
                             "system": "http://unitsofmeasure.org"
                         }
                         )
        self.assertEqual(response["entry"][0]["resource"]["entry"][0]['resource']['effectiveDateTime'],
                         '2022-06-01T18:26:08')
        self.assertEqual(response["entry"][0]["resource"]["entry"][1]['resource']['status'], 'final')
        self.assertEqual(response["entry"][0]["resource"]["entry"][1]['resource']['valueQuantity'],
                         {
                             "code": "kg",
                             "unit": "kg",
                             "value": 60.6,
                             "system": "http://unitsofmeasure.org"
                         }
                         )
        self.assertEqual(response["entry"][0]["resource"]["entry"][1]['resource']['effectiveDateTime'],
                         '2022-06-01T18:26:08')

    def test_remote_pm_can_skip_questions(self):
        self.data_generator.create_database_code(value='pmi_skip')
        participant_id = to_client_participant_id(
            self.data_generator.create_database_participant_summary().participantId
        )
        remote_pm_questionnaire_id = self.create_questionnaire("remote_pm_questionnaire.json")

        resource = self._load_response_json("remote_pm_response_metric.json", remote_pm_questionnaire_id,
                                            participant_id)
        resource['group']['question'][1]['answer'] = [{
            "valueCoding": {
                "code": "PMI_Skip",
                "system": "http://terminology.pmi-ops.org/CodeSystem/ppi"
            }
        }]
        resource['group']['question'][2]['answer'] = [{
            "valueCoding": {
                "code": "PMI_Skip",
                "system": "http://terminology.pmi-ops.org/CodeSystem/ppi"
            }
        }]
        with FakeClock(TIME_2):
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary['selfReportedPhysicalMeasurementsStatus'], 'COMPLETED')

    def test_ehr_consent_expired(self):
        participant_id = self.create_participant()
        authored_1 = datetime.datetime(2019, 3, 16, 1, 39, 33, tzinfo=pytz.utc)
        created = datetime.datetime(2019, 3, 16, 1, 51, 22)
        with FakeClock(created):
            self.send_consent(participant_id, authored=authored_1)
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(parse(summary["consentForStudyEnrollmentTime"]), created.replace(tzinfo=None))
        self.assertEqual(parse(summary["consentForStudyEnrollmentAuthored"]), authored_1.replace(tzinfo=None))

        self.assertEqual(summary.get('consentForElectronicHealthRecordsAuthored'), None)
        self.assertEqual(summary.get('enrollmentStatusMemberTime'), None)
        self.assertEqual(summary.get('enrollmentStatus'), 'INTERESTED')

        self._ehr_questionnaire_id = self.create_questionnaire("ehr_consent_questionnaire.json")

        # send ConsentPermission_Yes questionnaire response
        with FakeClock(datetime.datetime(2020, 3, 12)):
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_YES_CODE, None,
                                          datetime.datetime(2020, 2, 12))
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('consentForElectronicHealthRecordsAuthored'), '2020-02-12T00:00:00')
        self.assertEqual(summary.get('enrollmentStatusMemberTime'), '2020-02-12T00:00:00')
        self.assertEqual(summary.get('enrollmentStatus'), 'MEMBER')
        self.assertEqual(summary.get('ehrConsentExpireStatus'), 'UNSET')

        summary2 = self.send_get("ParticipantSummary?_count=25&_offset=0&_sort%3Adesc=consentForStudyEnrollmentAuthored"
                                 "&consentForElectronicHealthRecords=SUBMITTED&ehrConsentExpireStatus=UNSET"
                                 "&_includeTotal=true")
        self.assertEqual(len(summary2.get('entry')), 1)

        # send EHRConsentPII_ConsentExpired_Yes questionnaire response
        # response payload sample
        # {
        #     "resourceType": "QuestionnaireResponse",
        #     "extension": [
        #         {
        #             "url": "http://hl7.org/fhir/StructureDefinition/iso21090-ST-language",
        #             "valueCode": "en"
        #         }
        #     ],
        #     "identifier": {
        #         "value": "1592553285370"
        #     },
        #     "questionnaire": {
        #         "reference": "Questionnaire/475180/_history/V2020.04.20"
        #     },
        #     "status": "completed",
        #     "subject": {
        #         "reference": "Patient/P443846736"
        #     },
        #     "authored": "2020-06-19T07:54:45+00:00",
        #     "group": {
        #         "linkId": "root_group",
        #         "title": "Consent to Share Electronic Health Records",
        #         "text": "Consent to Share Electronic Health Records",
        #         "question": [
        #             {
        #                 "linkId": "7068",
        #                 "text": "EHRConsentPII_ConsentPermission",
        #                 "answer": [
        #                     {
        #                         "valueCoding": {
        #                             "system": "http://terminology.pmi-ops.org/CodeSystem/ppi",
        #                             "code": "ConsentPermission_No",
        #                             "display": "No, I do not wish to give All of Us access to my EHR"
        #                         }
        #                     }
        #                 ]
        #             },
        #             {
        #                 "linkId": "47771",
        #                 "text": "EHRConsentPII_ConsentExpired",
        #                 "answer": [
        #                     {
        #                         "valueString": "EHRConsentPII_ConsentExpired_Yes"
        #                     }
        #                 ]
        #             }
        #         ]
        #     },
        #     "id": "431459662"
        # }
        with FakeClock(datetime.datetime(2020, 4, 12)):
            string_answer = ['ehrConsentExpired', 'EHRConsentPII_ConsentExpired_Yes']
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_NO_CODE, [string_answer],
                                          datetime.datetime(2020, 3, 20))
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('consentForElectronicHealthRecordsAuthored'), '2020-03-20T00:00:00')
        self.assertEqual(summary.get('consentForElectronicHealthRecords'), 'SUBMITTED_NO_CONSENT')
        self.assertEqual(summary.get('enrollmentStatusMemberTime'), '2020-02-12T00:00:00')
        self.assertEqual(summary.get('enrollmentStatus'), 'MEMBER')
        self.assertEqual(summary.get('ehrConsentExpireStatus'), 'EXPIRED')
        self.assertEqual(summary.get('ehrConsentExpireTime'), '2020-04-12T00:00:00')
        self.assertEqual(summary.get('ehrConsentExpireAuthored'), '2020-03-20T00:00:00')

        # test participant summary api ehr consent expire status returns right info
        summary2 = self.send_get("ParticipantSummary?_count=25&_offset=0&_sort%3Adesc=consentForStudyEnrollmentAuthored"
                                 "&consentForElectronicHealthRecords=SUBMITTED_NO_CONSENT"
                                 "&ehrConsentExpireStatus=EXPIRED&_includeTotal=true")
        self.assertEqual(len(summary2.get('entry')), 1)

        # test re-sign ehr consent
        with FakeClock(datetime.datetime(2020, 4, 12)):
            self.submit_ehr_questionnaire(participant_id, CONSENT_PERMISSION_YES_CODE, None,
                                          datetime.datetime(2020, 4, 11))
        summary = self.send_get("Participant/{0}/Summary".format(participant_id))
        self.assertEqual(summary.get('consentForElectronicHealthRecordsAuthored'), '2020-04-11T00:00:00')
        self.assertEqual(summary.get('enrollmentStatusMemberTime'), '2020-02-12T00:00:00')
        self.assertEqual(summary.get('enrollmentStatus'), 'MEMBER')
        self.assertEqual(summary.get('ehrConsentExpireStatus'), 'UNSET')
        self.assertEqual(summary.get('ehrConsentExpireTime'), None)
        self.assertEqual(summary.get('ehrConsentExpireAuthored'), None)

        summary2 = self.send_get("ParticipantSummary?_count=25&_offset=0&_sort%3Adesc=consentForStudyEnrollmentAuthored"
                                 "&consentForElectronicHealthRecords=SUBMITTED&ehrConsentExpireStatus=UNSET"
                                 "&_includeTotal=true")
        self.assertEqual(len(summary2.get('entry')), 1)

    def test_ehr_conflicting_responses_received_out_of_order(self):
        """
        Multiple EHR with conflicting consent responses received with non-consecutive authored dates
        """
        participant_id = self.data_generator.create_database_participant_summary(
            email='test@ehr.com'
        ).participantId
        participant_id_str = f'P{participant_id}'

        self._ehr_questionnaire_id = self.create_questionnaire("ehr_consent_questionnaire.json")

        # send ConsentPermission_No questionnaire response first
        with FakeClock(datetime.datetime(2022, 3, 12)):
            self.submit_ehr_questionnaire(
                participant_id=participant_id_str,
                ehr_response_code=CONSENT_PERMISSION_NO_CODE,
                string_answers=None,
                authored=datetime.datetime(2022, 2, 12)
            )

        # now send ConsentPermission_Yes questionnaire response
        # (but with an earlier authored than previous No payload)
        with FakeClock(datetime.datetime(2022, 3, 15)):
            self.submit_ehr_questionnaire(
                participant_id=participant_id_str,
                ehr_response_code=CONSENT_PERMISSION_YES_CODE,
                string_answers=None,
                authored=datetime.datetime(2022, 2, 7)
            )

        # Expect the later authored "No" payload to be reflected in participant summary current EHR status fields
        # (but have a "first yes" matching the earlier authored "yes" payload)
        summary = self.send_get(f'Participant/{participant_id_str}/Summary')
        self.assertEqual('SUBMITTED_NO_CONSENT', summary.get('consentForElectronicHealthRecords'))

    def submit_ehr_questionnaire(self, participant_id, ehr_response_code, string_answers, authored):
        if not self._ehr_questionnaire_id:
            self._ehr_questionnaire_id = self.create_questionnaire("ehr_consent_questionnaire.json")
        code_answers = []
        if ehr_response_code:
            _add_code_answer(code_answers, 'ehrConsent', ehr_response_code)
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            self._ehr_questionnaire_id,
            string_answers=string_answers,
            code_answers=code_answers,
            authored=authored
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)

    def test_insert_raises_400_for_excessively_long_valueString(self):
        participant_id = self.create_participant()
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        url = _questionnaire_response_url(participant_id)

        # Remember we need to send the consent first
        self.send_consent(participant_id)

        # Check that a string of exactly the max length will post
        # This one should be exactly long enough to pass
        string = "a" * QuestionnaireResponseAnswer.VALUE_STRING_MAXLEN
        string_answers = [["nameOfChild", string]]
        resource = self.make_questionnaire_response_json(participant_id, questionnaire_id, string_answers=string_answers)
        response = self.send_post(url, resource)
        self.assertEqual(response["group"]["question"][0]["answer"][0]["valueString"], string)

        # Check that a string longer than the max will not
        # This one should evaluate to a string that is one char too long; i.e. exactly 64KiB
        string = "a" * (QuestionnaireResponseAnswer.VALUE_STRING_MAXLEN + 1)
        string_answers = [["nameOfChild", string]]
        resource = self.make_questionnaire_response_json(participant_id, questionnaire_id, string_answers=string_answers)
        self.send_post(url, resource, expected_status=http.client.BAD_REQUEST)

    def test_invalid_authored_date_logs_error(self):
        participant_id = self.create_participant()
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        url = _questionnaire_response_url(participant_id)
        self.send_consent(participant_id)

        # Send a response that has an invalid authored date and check that the issue is logged
        resource = self.make_questionnaire_response_json(participant_id, questionnaire_id)
        resource['authored'] = '2021-13-05T15:00'
        with mock.patch('rdr_service.dao.questionnaire_response_dao.logging') as logging_mock:
            self.send_post(url, resource)
            logging_mock.error.assert_any_call(
                f'Response by {participant_id} to questionnaire {questionnaire_id} has missing or invalid authored date'
            )

    def test_insert(self):
        participant_id = self.create_participant()
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        with open(data_path("questionnaire_response3.json")) as fd:
            resource = json.load(fd)

        # Sending response with the dummy participant id in the file is an error
        self.send_post(
            _questionnaire_response_url("{participant_id}"), resource, expected_status=http.client.NOT_FOUND
        )

        # Fixing participant id but not the questionnaire id is also an error
        resource["subject"]["reference"] = resource["subject"]["reference"].format(participant_id=participant_id)
        self.send_post(_questionnaire_response_url(participant_id), resource, expected_status=http.client.BAD_REQUEST)

        # Fix the reference
        resource["questionnaire"]["reference"] = resource["questionnaire"]["reference"].format(
            questionnaire_id=questionnaire_id
        )

        # Sending the response before the consent is an error.
        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource, expected_status=http.client.BAD_REQUEST)

        # After consent, the post succeeds
        self.send_consent(participant_id)
        self._save_codes(resource)
        response = self.send_post(_questionnaire_response_url(participant_id), resource)
        resource["id"] = response["id"]
        # The resource gets rewritten to include the version
        resource['questionnaire']['reference'] = 'Questionnaire/%s/_history/aaa' % questionnaire_id
        self.assertJsonResponseMatches(resource, response)

        # Check that the extensions were saved
        questionnaire_response_id = response['id']
        test_extension: QuestionnaireResponseExtension = self.session.query(QuestionnaireResponseExtension).filter(
            QuestionnaireResponseExtension.url == 'extension-url',
            QuestionnaireResponseExtension.questionnaireResponseId == questionnaire_response_id
        ).one()
        self.assertEqual('test string', test_extension.valueString)
        code_extension: QuestionnaireResponseExtension = self.session.query(QuestionnaireResponseExtension).filter(
            QuestionnaireResponseExtension.url == 'code-url',
            QuestionnaireResponseExtension.questionnaireResponseId == questionnaire_response_id
        ).one()
        self.assertEqual('code_value', code_extension.valueCode)

        #  sending an update response with history reference
        with open(data_path('questionnaire_response4.json')) as fd:
            update_resource = json.load(fd)
        update_resource['subject']['reference'] = \
            update_resource['subject']['reference'].format(participant_id=participant_id)
        update_resource['questionnaire']['reference'] = \
            update_resource['questionnaire']['reference'].format(questionnaire_id=questionnaire_id,
                                                                 semantic_version='aaa')
        self._save_codes(resource)
        response = self.send_post(_questionnaire_response_url(participant_id), update_resource)
        update_resource['id'] = response['id']
        self.assertJsonResponseMatches(update_resource, response)

        # Do a get to fetch the questionnaire
        get_response = self.send_get(_questionnaire_response_url(participant_id) + "/" + response["id"])
        self.assertJsonResponseMatches(update_resource, get_response)

        code_dao = CodeDao()

        name_of_child = code_dao.get_code("sys", "nameOfChild")
        birth_weight = code_dao.get_code("sys", "birthWeight")
        birth_length = code_dao.get_code("sys", "birthLength")
        vitamin_k_dose_1 = code_dao.get_code("sys", "vitaminKDose1")
        vitamin_k_dose_2 = code_dao.get_code("sys", "vitaminKDose2")
        hep_b_given = code_dao.get_code("sys", "hepBgiven")
        abnormalities_at_birth = code_dao.get_code("sys", "abnormalitiesAtBirth")
        answer_dao = QuestionnaireResponseAnswerDao()
        with answer_dao.session() as session:
            code_ids = [
                code.codeId
                for code in [
                    name_of_child,
                    birth_weight,
                    birth_length,
                    vitamin_k_dose_1,
                    vitamin_k_dose_2,
                    hep_b_given,
                    abnormalities_at_birth,
                ]
            ]
            current_answers = answer_dao.get_current_answers_for_concepts(
                session, from_client_participant_id(participant_id), code_ids
            )
        self.assertEqual(7, len(current_answers))
        questionnaire = QuestionnaireDao().get_with_children(questionnaire_id)
        question_id_to_answer = {answer.questionId: answer for answer in current_answers}
        code_id_to_answer = {
            question.codeId: question_id_to_answer.get(question.questionnaireQuestionId)
            for question in questionnaire.questions
        }
        self.assertEqual("Cathy Jones", code_id_to_answer[name_of_child.codeId].valueString)
        self.assertEqual(3.25, code_id_to_answer[birth_weight.codeId].valueDecimal)
        self.assertEqual(44.3, code_id_to_answer[birth_length.codeId].valueDecimal)
        self.assertEqual(44, code_id_to_answer[birth_length.codeId].valueInteger)
        self.assertEqual(True, code_id_to_answer[hep_b_given.codeId].valueBoolean)
        self.assertEqual(0, code_id_to_answer[abnormalities_at_birth.codeId].valueInteger)
        self.assertEqual(datetime.date(1972, 11, 30), code_id_to_answer[vitamin_k_dose_1.codeId].valueDate)
        self.assertEqual(
            datetime.datetime(1972, 11, 30, 12, 34, 42), code_id_to_answer[vitamin_k_dose_2.codeId].valueDateTime
        )

    def test_cati_questionnaire_responses(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")

        resource = self._load_response_json("questionnaire_the_basics_resp.json", questionnaire_id, participant_id)
        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            response = self.send_post(_questionnaire_response_url(participant_id), resource)

        questionnaire_response_dao = QuestionnaireResponseDao()
        qr = questionnaire_response_dao.get(response['id'])
        self.assertEqual(qr.nonParticipantAuthor, 'CATI')

    def test_emoji_questionnaire_responses(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            string_answers = [
                ("firstName", 'test1'),
                ("lastName", 'test2'),
                ("email", 'test@example.com'),
                ("streetAddress", 'test address'),
                ("streetAddress2", 'Colombia 🇨🇴'),
            ]
            self.send_consent(participant_id, string_answers=string_answers)
        response = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(response['streetAddress2'], 'Colombia 🇨🇴')

    def test_demographic_questionnaire_responses(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")

        resource = self._load_response_json("questionnaire_the_basics_resp.json", questionnaire_id, participant_id)
        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        expected = dict(participant_summary_default_values)
        expected.update({
            "genderIdentity": "GenderIdentity_NonBinary",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "questionnaireOnTheBasicsTime": TIME_2.isoformat(),
            "questionnaireOnTheBasicsAuthored": TIME_2.isoformat(),
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

    def test_digital_health_consent(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")
            apple_kit_questionnaire_id = self.create_questionnaire("apple_health_kit_start_questionnaire.json")
            qr_json = self.make_questionnaire_response_json(
                participant_id,
                apple_kit_questionnaire_id,
                authored=TIME_1
            )
            self.send_post(self.questionnaire_response_url(participant_id), qr_json)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['digitalHealthSharingStatus'],
                         {'appleHealthKit':
                             {
                                 'status': 'YES',
                                 'history': [{'status': 'YES', 'authoredTime': '2016-01-01T00:00:00Z'}],
                                 'authoredTime': '2016-01-01T00:00:00Z'
                             }
                         })
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)
        # test duplication consent
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['digitalHealthSharingStatus'],
                         {'appleHealthKit':
                             {
                                 'status': 'YES',
                                 'history': [{'status': 'YES', 'authoredTime': '2016-01-01T00:00:00Z'}],
                                 'authoredTime': '2016-01-01T00:00:00Z'
                             }
                         })
        # test new consent
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            apple_kit_questionnaire_id,
            authored=TIME_2
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['digitalHealthSharingStatus'],
                         {'appleHealthKit':
                             {
                                 'status': 'YES',
                                 'history': [{'status': 'YES', 'authoredTime': '2016-01-02T00:00:00Z'},
                                             {'status': 'YES', 'authoredTime': '2016-01-01T00:00:00Z'}],
                                 'authoredTime': '2016-01-02T00:00:00Z'
                             }
                         })
        # test multiple different consents
        fitbit_questionnaire_id = self.create_questionnaire("fitbit_start_questionnaire.json")
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            fitbit_questionnaire_id,
            authored=TIME_1
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)
        fitbit_stop_questionnaire_id = self.create_questionnaire("fitbit_stop_questionnaire.json")
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            fitbit_stop_questionnaire_id,
            authored=TIME_3
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['digitalHealthSharingStatus'],
                         {
                             'fitbit':
                                 {
                                     'status': 'NO',
                                     'history': [{'status': 'NO', 'authoredTime': '2016-01-03T00:00:00Z'},
                                                 {'status': 'YES', 'authoredTime': '2016-01-01T00:00:00Z'}],
                                     'authoredTime': '2016-01-03T00:00:00Z'
                                 },
                             'appleHealthKit':
                                 {
                                     'status': 'YES',
                                     'history': [{'status': 'YES', 'authoredTime': '2016-01-02T00:00:00Z'},
                                                 {'status': 'YES', 'authoredTime': '2016-01-01T00:00:00Z'}],
                                     'authoredTime': '2016-01-02T00:00:00Z'
                                 }
                         })

    def test_get_digital_health_consent_out_of_order(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")
        fitbit_stop_questionnaire_id = self.create_questionnaire("fitbit_stop_questionnaire.json")
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            fitbit_stop_questionnaire_id,
            authored=TIME_3
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)

        fitbit_start_questionnaire_id = self.create_questionnaire("fitbit_start_questionnaire.json")
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            fitbit_start_questionnaire_id,
            authored=TIME_1
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['digitalHealthSharingStatus'],
                         {
                             'fitbit':
                                 {
                                     'status': 'NO',
                                     'history': [{'status': 'NO', 'authoredTime': '2016-01-03T00:00:00Z'},
                                                 {'status': 'YES', 'authoredTime': '2016-01-01T00:00:00Z'}],
                                     'authoredTime': '2016-01-03T00:00:00Z'
                                 }
                         })

    def test_gror_consent(self):
        """WIP: The json files associated with this test may need to change.
        Requirements are still being worked out on PTSC side and this was made
        before finalization."""
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)

        expected = dict(participant_summary_default_values_no_basics)
        expected.update({
            "genderIdentity": "UNSET",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "numCompletedPPIModules": 0,
            "numCompletedBaselinePPIModules": 0,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "primaryLanguage": "es",
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

        # verify if the response is not consent, the primary language will not change
        questionnaire_id = self.create_questionnaire("consent_for_genomic_ror_question.json")

        resource = self._load_response_json("consent_for_genomic_ror_resp.json", questionnaire_id, participant_id)
        resource['authored'] = TIME_1.isoformat()

        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['consentForGenomicsROR'], 'SUBMITTED')

        dont_know_resp = self._load_response_json(
            "consent_for_genomic_ror_dont_know.json",
            questionnaire_id,
            participant_id
        )
        dont_know_resp['authored'] = TIME_2.isoformat()

        with FakeClock(TIME_2):
            self._save_codes(dont_know_resp)
            self.send_post(_questionnaire_response_url(participant_id), dont_know_resp)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['semanticVersionForPrimaryConsent'], 'v1')
        self.assertEqual(summary['consentForGenomicsRORTime'], TIME_2.isoformat())
        self.assertEqual(summary['consentForGenomicsRORAuthored'], TIME_2.isoformat())
        self.assertEqual(summary['consentForGenomicsROR'], 'SUBMITTED_NOT_SURE')

        resource = self._load_response_json("consent_for_genomic_ror_no.json", questionnaire_id, participant_id)
        resource['authored'] = TIME_3.isoformat()

        with FakeClock(TIME_3):
            self._save_codes(resource)
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['semanticVersionForPrimaryConsent'], 'v1')
        self.assertEqual(summary['consentForGenomicsROR'], 'SUBMITTED_NO_CONSENT')
        self.assertEqual(summary['consentForGenomicsRORTime'], TIME_3.isoformat())
        self.assertEqual(summary['consentForGenomicsRORAuthored'], TIME_3.isoformat())

        # Test Bad Code Value Sent returns 400
        resource = self._load_response_json("consent_for_genomic_ror_bad_request.json",
                                            questionnaire_id, participant_id)
        resource['authored'] = TIME_4.isoformat()

        with FakeClock(TIME_4):
            self._save_codes(resource)
            self.send_post(_questionnaire_response_url(participant_id),
                           resource,
                           expected_status=http.client.BAD_REQUEST)

    def test_gror_consent_with_duplicate_answers(self):
        """ Simulate RDR questionnaire_response payloads that contain duplicate entries in question JSON array"""

        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)

        expected = dict(participant_summary_default_values_no_basics)
        expected.update({
            "genderIdentity": "UNSET",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "numCompletedPPIModules": 0,
            "numCompletedBaselinePPIModules": 0,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "primaryLanguage": "es",
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

        # verify if the response is not consent, the primary language will not change
        questionnaire_id = self.create_questionnaire("consent_for_genomic_ror_question.json")

        resource = self._load_response_json("consent_for_genomic_ror_resp.json",
                                            questionnaire_id, participant_id)

        # Repeat the question array element in the response JSON to simulate the duplication seen in RDR
        # See: questionnaire_response_id 680418686
        resource["group"]["question"].append(resource["group"]["question"][0])

        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['consentForGenomicsROR'], 'SUBMITTED')

        ps_json = self.make_bq_participant_summary(participant_id)
        gror = self.get_generated_items(ps_json['modules'], item_key='mod_module', item_value='GROR',
                                        sort_key='mod_authored')
        self.assertEqual(len(gror), 1)
        self.assertEqual(gror[0].get('mod_status', None), 'SUBMITTED')

    def test_consent_with_extension_language(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)

        expected = dict(participant_summary_default_values_no_basics)
        expected.update({
            "genderIdentity": "UNSET",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "numCompletedPPIModules": 0,
            "numCompletedBaselinePPIModules": 0,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "primaryLanguage": "es",
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

        # verify if the response is not consent, the primary language will not change
        questionnaire_id = self.create_questionnaire("questionnaire_family_history.json")

        resource = self._load_response_json("questionnaire_family_history_resp.json", questionnaire_id, participant_id)
        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(expected["primaryLanguage"], summary["primaryLanguage"])

    def test_social_determinants_of_health_questionnaire(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")

        questionnaire_id = self.create_questionnaire("questionnaire_social_determinants_of_health.json")
        resource = self._load_response_json("questionnaire_social_determinants_of_health_resp.json", questionnaire_id,
                                            participant_id)
        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['questionnaireOnSocialDeterminantsOfHealth'], 'SUBMITTED')
        self.assertEqual(summary['numCompletedPPIModules'], 1)
        self.assertEqual(summary['questionnaireOnSocialDeterminantsOfHealthAuthored'], '2018-07-23T21:21:12')

    def test_family_and_personal_health_history_questionnaire(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id, language="es")

        questionnaire_id = self.create_questionnaire("questionnaire_family_and_personal_health_history.json")
        resource = self._load_response_json("questionnaire_family_and_personal_health_history_resp.json",
                                            questionnaire_id, participant_id)
        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary['questionnaireOnPersonalAndFamilyHealthHistory'], 'SUBMITTED')
        self.assertEqual(summary['numCompletedPPIModules'], 1)
        self.assertEqual(summary['questionnaireOnPersonalAndFamilyHealthHistoryAuthored'], '2018-07-23T21:21:12')

    def test_invalid_questionnaire(self):
        participant_id = self.create_participant()
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        q = QuestionnaireDao()
        questionnaire = q.get(questionnaire_id)
        make_transient(questionnaire)
        questionnaire.status = QuestionnaireDefinitionStatus.INVALID
        with q.session() as session:
            existing_obj = q.get_for_update(session, q.get_id(questionnaire))
            q._do_update(session, questionnaire, existing_obj)
        q.get(questionnaire_id)

        with open(data_path("questionnaire_response3.json")) as fd:
            resource = json.load(fd)

        self.send_consent(participant_id)

        resource["subject"]["reference"] = resource["subject"]["reference"].format(participant_id=participant_id)
        # The resource gets rewritten to include the version
        resource["questionnaire"]["reference"] = "Questionnaire/%s" % questionnaire_id
        self.send_post(_questionnaire_response_url(participant_id), resource, expected_status=http.client.BAD_REQUEST)
        resource["questionnaire"]["reference"] = "Questionnaire/%s/_history/2" % questionnaire_id
        self.send_post(_questionnaire_response_url(participant_id), resource, expected_status=http.client.BAD_REQUEST)

    def test_response_allows_for_missing_group(self):
        participant_id = self.create_participant()
        self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        resource = self._load_response_json("questionnaire_response_empty.json", questionnaire_id, participant_id)

        self.send_post(_questionnaire_response_url(participant_id), resource)  # will fail if status 200 isn't returned

    def test_invalid_questionnaire_linkid(self):
        """
        DA-623 - Make sure that an invalid link id in response triggers a BadRequest status.
        Per a PTSC group request, only log a message for invalid link ids.
        In the future if questionnaires with bad link ids trigger a BadRequest, the code below
        can be uncommented.
        """
        participant_id = self.create_participant()
        self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_family_history.json")
        resource = self._load_response_json("questionnaire_family_history_resp.json", questionnaire_id, participant_id)

        self._save_codes(resource)
        self.send_post(_questionnaire_response_url(participant_id), resource, expected_status=http.client.OK)

        # Alter response to set a bad link id value
        # resource['group']['question'][0]['linkId'] = 'bad-link-id'

        # # DA-623 - Per the PTSC groups request, invalid link ids only log a message for
        # invalid link ids.
        # self.send_post(_questionnaire_response_url(participant_id), resource,
        #                           expected_status=httplib.BAD_REQUEST)

    def test_multiple_genders(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json(
            "questionnaire_the_basics_resp_multiple_gender.json",
            questionnaire_id,
            participant_id
        )

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        expected = dict(participant_summary_default_values)
        expected.update({
            "genderIdentity": "GenderIdentity_MoreThanOne",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "questionnaireOnTheBasicsTime": TIME_2.isoformat(),
            "questionnaireOnTheBasicsAuthored": TIME_2.isoformat(),
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

    def _get_expected_gender_code_ids(self, gender_code_values):
        self.session.commit()  # Commit the session to ensure we're pulling the latest data from the database
        gender_identity_code_ids = self.session.query(Code.codeId).filter(
            or_(*[Code.value == code_value for code_value in gender_code_values])
        ).all()
        # Need to unpack since sqlalchemy returns named tuples
        return [value for value, in gender_identity_code_ids]

    def test_participant_gender_answers(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json(
            "questionnaire_the_basics_resp_multiple_gender.json",
            questionnaire_id,
            participant_id
        )

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        participant_gender_answers_dao = ParticipantGenderAnswersDao()
        answers = participant_gender_answers_dao.get_all()
        expected_gender_code_ids = self._get_expected_gender_code_ids([GENDER_WOMAN_CODE, GENDER_MAN_CODE])
        self.assertEqual(len(answers), 2)
        for answer in answers:
            self.assertIn(answer.codeId, expected_gender_code_ids)

        # resubmit the answers, old value should be removed
        resource = self._load_response_json(
            "questionnaire_the_basics_resp_multiple_gender_2.json",
            questionnaire_id,
            participant_id
        )

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        answers = participant_gender_answers_dao.get_all()
        expected_gender_code_ids = self._get_expected_gender_code_ids([GENDER_TRANSGENDER_CODE, GENDER_MAN_CODE])
        self.assertEqual(len(answers), 2)
        for answer in answers:
            self.assertIn(answer.codeId, expected_gender_code_ids)

    def test_participant_race_answers(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json(
            "questionnaire_the_basics_resp_multiple_race.json",
            questionnaire_id,
            participant_id
        )

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        code_dao = CodeDao()
        code1 = code_dao.get_code("http://terminology.pmi-ops.org/CodeSystem/ppi", "WhatRaceEthnicity_White")
        code2 = code_dao.get_code("http://terminology.pmi-ops.org/CodeSystem/ppi", "WhatRaceEthnicity_Hispanic")

        participant_race_answers_dao = ParticipantRaceAnswersDao()
        answers = participant_race_answers_dao.get_all()
        self.assertEqual(len(answers), 2)
        for answer in answers:
            self.assertIn(answer.codeId, [code1.codeId, code2.codeId])

        # Confirm the PDR bigquery_sync data generator builds the correct races item, e.g.
        # bqs_data['races'] = [
        #   { 'race':  'WhatRaceEthnicity_White', 'race_id': <code_id integer> },
        #   { 'race': 'WhatRaceEthnicity_Hispanic', 'race_id': <code_id integer> }
        # ]
        bqs_data = self.make_bq_participant_summary(participant_id)
        self.assertEqual(len(bqs_data['races']), 2)
        for answer in bqs_data['races']:
            self.assertIn(answer.get('race'), [code1.value, code2.value])
            self.assertIn(answer.get('race_id'), [code1.codeId, code2.codeId])

        # Repeat the PDR data test for the resource generator output
        ps_rsrc_data = self.make_participant_resource(participant_id, get_data=True)

        self.assertEqual(len(ps_rsrc_data['races']), 2)
        for answer in ps_rsrc_data['races']:
            self.assertIn(answer.get('race'), [code1.value, code2.value])
            self.assertIn(answer.get('race_id'), [code1.codeId, code2.codeId])

        # resubmit the answers, old value should be removed
        resource = self._load_response_json(
            "questionnaire_the_basics_resp_multiple_race_2.json",
            questionnaire_id,
            participant_id
        )

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        code_dao = CodeDao()
        code1 = code_dao.get_code("http://terminology.pmi-ops.org/CodeSystem/ppi", "WhatRaceEthnicity_NHPI")
        code2 = code_dao.get_code("http://terminology.pmi-ops.org/CodeSystem/ppi", "PMI_PreferNotToAnswer")

        answers = participant_race_answers_dao.get_all()
        self.assertEqual(len(answers), 2)
        for answer in answers:
            self.assertIn(answer.codeId, [code1.codeId, code2.codeId])

    def test_gender_prefer_not_answer(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")

        resource = self._load_response_json("questionnaire_the_basics_resp.json", questionnaire_id, participant_id)
        resource["group"]["question"][2]["answer"][0]["valueCoding"]["code"] = "PMI_PreferNotToAnswer"

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        expected = dict(participant_summary_default_values)
        expected.update({
            "genderIdentity": "PMI_PreferNotToAnswer",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "questionnaireOnTheBasicsTime": TIME_2.isoformat(),
            "questionnaireOnTheBasicsAuthored": TIME_2.isoformat(),
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

    def test_gender_plus_skip_equals_gender(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")

        resource = self._load_response_json(
            "questionnaire_the_basics_resp_multiple_gender.json",
            questionnaire_id,
            participant_id
        )
        resource["group"]["question"][2]["answer"][1]["valueCoding"]["code"] = "PMI_Skip"

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self._save_codes(resource)
            self.send_post(_questionnaire_response_url(participant_id), resource)

        participant = self.send_get("Participant/%s" % participant_id)
        summary = self.send_get("Participant/%s/Summary" % participant_id)
        expected = dict(participant_summary_default_values)
        expected.update({
            "genderIdentity": "GenderIdentity_Man",
            "firstName": self.first_name,
            "lastName": self.last_name,
            "email": self.email,
            "streetAddress": self.streetAddress,
            "streetAddress2": self.streetAddress2,
            "biobankId": participant["biobankId"],
            "participantId": participant_id,
            "consentForStudyEnrollmentTime": TIME_1.isoformat(),
            "consentForStudyEnrollmentAuthored": TIME_1.isoformat(),
            "consentForStudyEnrollmentFirstYesAuthored": TIME_1.isoformat(),
            "questionnaireOnTheBasicsTime": TIME_2.isoformat(),
            "questionnaireOnTheBasicsAuthored": TIME_2.isoformat(),
            "signUpTime": TIME_1.isoformat(),
            "consentCohort": str(ParticipantCohort.COHORT_1),
            "cohort2PilotFlag": str(ParticipantCohortPilotFlag.UNSET),
            "enrollmentStatusParticipantV3_0Time": "2016-01-01T00:00:00",
            "enrollmentStatusParticipantV3_1Time": "2016-01-01T00:00:00"
        })
        self.assertJsonResponseMatches(expected, summary)

    def test_different_origin_cannot_submit(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)

        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json("questionnaire_the_basics_resp.json", questionnaire_id, participant_id)

        with FakeClock(TIME_2):
            resource["authored"] = TIME_2.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(summary["participantOrigin"], "example")

        with FakeClock(TIME_3):
            BaseTestCase.switch_auth_user("example@sabrina.com", "vibrent")
            resource["authored"] = TIME_3.isoformat()
            self.send_post(_questionnaire_response_url(participant_id), resource, expected_status=http.client.BAD_REQUEST)

            BaseTestCase.switch_auth_user("example@example.com", "example")
            summary = self.send_get("Participant/%s/Summary" % participant_id)
            # Posting a QR should not change origin.
            self.assertEqual(summary["participantOrigin"], "example")

    def test_cohort_group_from_payload(self):
        """Test that we use the cohort group if it is provided through the consent questionnaire response"""
        participant_id = self.create_participant()
        self.send_consent(participant_id, extra_string_values=[('cohort_group', '2')])

        summary = self.send_get("Participant/%s/Summary" % participant_id)
        self.assertEqual(str(ParticipantCohort.COHORT_2), summary['consentCohort'])

    @mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
    def test_cohort_use_date_fallback_on_invalid_number(self, mock_logging):
        """Test gracefully handling an invalid number sent for cohort group"""
        participant_id = self.create_participant()

        self.send_consent(
            participant_id,
            authored=datetime.datetime(2020, 4, 4),
            extra_string_values=[('cohort_group', '72')]
        )

        participant_summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == from_client_participant_id(participant_id)
        ).one()
        self.assertEqual(ParticipantCohort.COHORT_2, participant_summary.consentCohort)
        mock_logging.error.assert_called_with('Invalid value given for cohort group: received "72"')

    @mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
    def test_cohort_use_date_fallback_on_string(self, mock_logging):
        """Test gracefully handling a string sent for cohort group"""
        participant_id = self.create_participant()

        self.send_consent(
            participant_id,
            authored=datetime.datetime(2020, 4, 4),
            extra_string_values=[('cohort_group', 'A')]
        )

        participant_summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == from_client_participant_id(participant_id)
        ).one()
        self.assertEqual(ParticipantCohort.COHORT_2, participant_summary.consentCohort)
        mock_logging.error.assert_called_with('Invalid value given for cohort group: received "A"')

    @mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
    def test_warning_logged_for_missing_vibrent_cohort(self, mock_logging):
        """Test warning that vibrent hasn't sent cohort group information"""
        # Temporarily change the client that the test is seen as
        # so that we can submit a response for the participant under test
        user_info = config.getSettingJson(config.USER_INFO)
        original_user_client_id = user_info['example@example.com']['clientId']
        user_info['example@example.com']['clientId'] = 'vibrent'
        config.override_setting(config.USER_INFO, user_info)

        participant = self.data_generator.create_database_participant(participantOrigin='vibrent')
        self.send_consent(participant.participantId)

        mock_logging.warning.assert_called_with(f'Missing expected consent cohort information for participant '
                                             f'{participant.participantId}')

        user_info['example@example.com']['clientId'] = original_user_client_id
        config.override_setting(config.USER_INFO, user_info)

    def test_response_payload_cannot_create_new_codes(self):
        q_id = self.create_questionnaire("questionnaire1.json")
        p_id = self.create_participant()
        self.send_consent(p_id)

        resource = self.make_questionnaire_response_json(
            p_id,
            q_id,
            code_answers=[('2.3.2', Concept(PPI_SYSTEM, 'new_answer_code'))],
            create_codes=False
        )
        response = self.send_post(self.questionnaire_response_url(p_id), resource, expected_status=400)

        self.assertEqual(
            'The following code values were unrecognized: '
            'new_answer_code (system: http://terminology.pmi-ops.org/CodeSystem/ppi)',
            response.json['message']
        )

    def test_recording_response_payload_status(self):
        q_id = self.create_questionnaire("questionnaire1.json")
        p_id = self.create_participant()
        self.send_consent(p_id)

        resource = self.make_questionnaire_response_json(
            p_id,
            q_id,
            code_answers=[('2.3.2', Concept(PPI_SYSTEM, 'new_answer_code'))],
            status='entered-in-error'
        )
        self.send_post(self.questionnaire_response_url(p_id), resource)

        recorded_response = self.session.query(QuestionnaireResponse).filter(
            QuestionnaireResponse.participantId == from_client_participant_id(p_id),
            QuestionnaireResponse.questionnaireId == q_id
        ).one()
        self.assertEqual(QuestionnaireResponseStatus.ENTERED_IN_ERROR, recorded_response.status)

    def test_partial_survey_response(self):
        """
        We should be able to accept partial questionnaire responses without having them change the participant summary
        """
        participant_id = self.create_participant()
        self.send_consent(participant_id)

        # Set up a questionnaire that usually changes participant summary
        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        resource = self._load_response_json("questionnaire_the_basics_resp.json", questionnaire_id, participant_id)

        # Submit response as in-progress
        resource['status'] = 'in-progress'
        self.send_post(_questionnaire_response_url(participant_id), resource)

        # Make sure response doesn't affect participant summary
        participant_summary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == from_client_participant_id(participant_id)
        ).one()
        self.assertIsNone(participant_summary.questionnaireOnTheBasics)

        # PDR- 235 Add checks of the PDR generator data for module
        # response status of the "in progress" TheBasics test response
        ps_rsrc_data = self.make_participant_resource(participant_summary.participantId)

        # Check data from the resource generator
        basics_mod = self.get_generated_items(ps_rsrc_data['modules'], item_key='module', item_value='TheBasics')
        self.assertEqual(basics_mod[0].get('response_status', None), 'IN_PROGRESS')
        self.assertEqual(basics_mod[0].get('response_status_id', None), 0)

        # Check data from the bigquery_sync participant summary DAO / generator
        basics_mod = self.get_generated_items(ps_rsrc_data['modules'], item_key='module',
                                                   item_value='TheBasics')
        self.assertEqual(basics_mod[0].get('response_status', None), 'IN_PROGRESS')
        self.assertEqual(basics_mod[0].get('response_status_id', None), 0)

        # Check the bigquery_sync questionnaire response DAO / generator
        # TODO:  Validate Resource generator data for questionnaire response when implemented
        bqrs = self.make_bq_questionnaire_response(participant_summary.participantId, 'TheBasics', latest=True)

        # bqrs is a list of BQRecord types;  validating the field name values
        self.assertEqual(len(bqrs), 1)
        self.assertEqual(bqrs[0].status, 'IN_PROGRESS')
        self.assertEqual(bqrs[0].status_id, 0)

    @mock.patch('rdr_service.dao.questionnaire_response_dao.logging')
    def test_link_id_does_not_exist(self, mock_logging):
        # Get a participant set up for the test
        participant_id = self.create_participant()
        self.send_consent(participant_id)

        # Set up questionnaire, inserting through DAO to get history to generate as well
        questionnaire = self.data_generator._questionnaire()
        question = self.data_generator._questionnaire_question(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            linkId='not_answered'
        )
        questionnaire.questions.append(question)
        questionnaire_dao = QuestionnaireDao()
        questionnaire_dao.insert(questionnaire)

        # Create and send response
        questionnaire_response_json = self.make_questionnaire_response_json(
            participant_id,
            questionnaire.questionnaireId,
            string_answers=[
                ('invalid_link', 'This is an answer to a question that is not in the questionnaire')
            ]
        )
        self.send_post(f'Participant/{participant_id}/QuestionnaireResponse', questionnaire_response_json)

        # Make sure logs have been called for each issue
        mock_logging.error.assert_any_call('Questionnaire response contains invalid link ID "invalid_link"')

    def test_unexpected_extension_field_succeeds(self):
        """
        Not all extension fields were implemented in the RDR's Extension model, valueUri is one of them.
        This tests to be sure that a QuestionnaireResponse won't be rejected (or crash) if it contains an extension
        field we're not prepared to handle.
        """
        # Set up questionnaire and participant
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        participant_id = self.create_participant()
        self.send_consent(participant_id)

        # Check that POST doesn't fail on unknown extension fields
        resource = self._load_response_json("questionnaire_response3.json", questionnaire_id, participant_id)
        resource['extension'] = [{
            'url': 'test-unknown',
            'valueUri': 'testing'
        }]
        self._save_codes(resource)
        response = self.send_post(_questionnaire_response_url(participant_id), resource)

        # Double check that the extension wasn't made (if it was then this test may need to be updated)
        response_id = response['id']
        extension_query = self.session.query(QuestionnaireResponseExtension).filter(
            QuestionnaireResponseExtension.questionnaireResponseId == response_id
        )
        self.assertEqual(0, extension_query.count(),
                         'The extension was created, but the valueUri field is expected to be unrecognized')

    def test_response_for_withdrawn_participant(self):
        participant = self.data_generator.create_database_participant(withdrawalStatus=WithdrawalStatus.NO_USE)
        participant_id_str = to_client_participant_id(participant.participantId)

        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        with open(data_path("questionnaire_response3.json")) as fd:
            resource = json.load(fd)
        resource["subject"]["reference"] = resource["subject"]["reference"].format(participant_id=participant_id_str)
        resource["questionnaire"]["reference"] = resource["questionnaire"]["reference"].format(
            questionnaire_id=questionnaire_id
        )
        self._save_codes(resource)

        # Send questionnaire response expecting a 403 response status
        self.send_post(_questionnaire_response_url(participant_id_str), resource, expected_status=403)

    def test_storing_response_identifier(self):
        """Verifying saving the identifier sent with the response, and make sure unexpected values don't crash"""
        # Set up questionnaire and participant
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        participant = self.data_generator.create_database_participant_summary().participant
        participant_id_str = to_client_participant_id(participant.participantId)

        # Load a response with an identifier
        resource = self._load_response_json("questionnaire_response3.json", questionnaire_id, participant_id_str)
        self._save_codes(resource)
        response = self.send_post(_questionnaire_response_url(participant_id_str), resource)

        # Verify that the external identifier was stored
        response_obj = self.session.query(QuestionnaireResponse).filter(
            QuestionnaireResponse.questionnaireResponseId == response['id']
        ).one()
        self.assertEqual(resource['identifier']['value'], response_obj.externalId)

    def test_new_primary_consent_response(self):
        """Test that new consents for enrollment save a ConsentResponse"""
        participant = self.data_generator.create_database_participant()
        self.send_consent(participant.participantId, authored=datetime.datetime.now())

        consent_response = self.session.query(ConsentResponse).one()
        self.assertEqual(ConsentType.PRIMARY, consent_response.type)
        self.assertEqual(participant.participantId, consent_response.response.participantId)

    def test_primary_reconsent_response(self):
        """Test that a re-signing of the consent for enrollment saves a ConsentResponse"""
        consent_authored = datetime.datetime(2022, 1, 17, 13, 4)
        previous_response = self.data_generator.create_database_questionnaire_response(
            authored=consent_authored
        )
        self.session.add(ConsentResponse(response=previous_response, type=ConsentType.PRIMARY))
        self.session.commit()

        self.send_consent(
            previous_response.participantId,
            authored=consent_authored + datetime.timedelta(days=500)
        )

        consent_responses = self.session.query(ConsentResponse).all()
        self.assertEqual(2, len(consent_responses))
        self.assertEqual(consent_authored, consent_responses[0].response.authored)
        self.assertEqual(consent_authored + datetime.timedelta(days=500), consent_responses[1].response.authored)

    def test_primary_consent_replay(self):
        """Test that another payload for the same consent for enrollment does not save a ConsentResponse"""
        consent_authored = datetime.datetime(2022, 1, 17, 13, 4)
        previous_response = self.data_generator.create_database_questionnaire_response(
            authored=consent_authored
        )
        self.session.add(ConsentResponse(response=previous_response, type=ConsentType.PRIMARY))
        self.session.commit()

        self.send_consent(
            previous_response.participantId,
            authored=consent_authored + datetime.timedelta(seconds=20)
        )

        self.session.query(ConsentResponse).one()  # Raises an error if another object was created

    def test_receiving_long_identifier_fails_gracefully(self):
        """If the identifier is unexpectedly long, we should be able to skip it and still store the response"""
        # Set up questionnaire and participant
        questionnaire_id = self.create_questionnaire("questionnaire1.json")
        participant = self.data_generator.create_database_participant_summary().participant
        participant_id_str = to_client_participant_id(participant.participantId)

        # Load a response with an identifier
        resource = self._load_response_json("questionnaire_response3.json", questionnaire_id, participant_id_str)
        resource['identifier']['value'] = '1234' * 15
        self._save_codes(resource)
        response = self.send_post(_questionnaire_response_url(participant_id_str), resource, expected_status=200)

        # Verify that the response was saved, and that the identifier was skipped
        response_obj = self.session.query(QuestionnaireResponse).filter(
            QuestionnaireResponse.questionnaireResponseId == response['id']
        ).one()
        self.assertIsNone(response_obj.externalId)

    def test_questionnaire_life_functioning_survey(self):
        with FakeClock(TIME_1):
            participant_id = self.create_participant()
            self.send_consent(participant_id)
        questionnaire_id = self.create_questionnaire("questionnaire_life_functioning.json")
        resource = self._load_response_json("questionnaire_life_functioning_resp.json", questionnaire_id, participant_id)
        self._save_codes(resource)
        with FakeClock(datetime.datetime(2022, 9, 7, 1, 2, 3)):
            self.send_post(_questionnaire_response_url(participant_id), resource)

        summary = self.send_get(f"Participant/{participant_id}/Summary")
        self.assertEqual(summary['questionnaireOnLifeFunctioning'], 'SUBMITTED')
        self.assertEqual(summary['questionnaireOnLifeFunctioningAuthored'], '2022-09-06T14:32:28')
        self.assertEqual(summary['questionnaireOnLifeFunctioningTime'], '2022-09-07T01:02:03')

    @classmethod
    def _load_response_json(cls, template_file_name, questionnaire_id, participant_id_str):
        with open(data_path(template_file_name)) as fd:
            resource = json.load(fd)

        resource["subject"]["reference"] = f'Patient/{participant_id_str}'
        resource["questionnaire"]["reference"] = f'Questionnaire/{questionnaire_id}'

        return resource

    def _submit_consent_questionnaire_response(
        self, participant_id, questionnaire_id, ehr_consent_answer, time=TIME_1
    ):
        code_answers = []
        _add_code_answer(code_answers, "ehrConsent", ehr_consent_answer)
        qr = self.make_questionnaire_response_json(participant_id, questionnaire_id, code_answers=code_answers)
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

    def submit_response(
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

    def _submit_empty_questionnaire_response(self, participant_id, questionnaire_id, time=TIME_1):
        qr = self.make_questionnaire_response_json(participant_id, questionnaire_id)
        with FakeClock(time):
            self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)


def _add_code_answer(code_answers, link_id, code):
    if code:
        code_answers.append((link_id, Concept(PPI_SYSTEM, code)))


def _get_pdr_module_dict(ps_data, module_id, key_name=None):
    """
      Returns the first occurrence of a module entry from PDR participant data for the specified module_id
      :param ps_data: A participant data dictionary (assumed to contain a 'modules' key/list of dict values)
      :param module_id: The name / string of the module to search for
      :param key_name:  The key name to extract the value from to match to the supplied module_id string
    """
    if isinstance(key_name, str):
        for mod in ps_data.get('modules'):
            if mod.get(key_name).lower() == module_id.lower():
                return mod

    return None

