from datetime import date, datetime, timedelta
import pytz

from rdr_service import config
from rdr_service.api_util import HEALTHPRO, PTC
from rdr_service.model.api_user import ApiUser
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportDenialReason, DeceasedReportStatus,\
    DeceasedStatus
from tests.helpers.unittest_base import BaseTestCase


class DeceasedReportTestBase(BaseTestCase):
    def setUp(self):
        super(DeceasedReportTestBase, self).setUp()
        self.original_user_roles = None

    def tearDown(self):
        if self.original_user_roles is not None:
            self.overwrite_test_user_roles(self.original_user_roles, save_current=False)

        super(DeceasedReportTestBase, self).tearDown()

    def overwrite_test_user_roles(self, roles, save_current=True):
        user_info = config.getSettingJson(config.USER_INFO)

        if save_current:
            # Save what was there so we can set it back in the tearDown
            self.original_user_roles = user_info['example@example.com']['roles']

        user_info['example@example.com']['roles'] = roles
        config.override_setting(config.USER_INFO, user_info)

    @staticmethod
    def get_deceased_report_id(response):
        return int(response['identifier'][0]['value'])


class DeceasedReportApiTest(DeceasedReportTestBase):
    def setUp(self):
        super(DeceasedReportApiTest, self).setUp()

        hpo = self.data_generator.create_database_hpo()
        self.paired_participant_without_summary = self.data_generator.create_database_participant(hpoId=hpo.hpoId)

        self.paired_participant_with_summary = self.data_generator.create_database_participant(hpoId=hpo.hpoId)
        self.data_generator.create_database_participant_summary(participant=self.paired_participant_with_summary)

        self.unpaired_participant_with_summary = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=self.unpaired_participant_with_summary)

    def post_report(self, report_json, participant_id=None, expected_status=200):
        if participant_id is None:
            participant_id = self.paired_participant_without_summary.participantId
        return self.send_post(f'Participant/P{participant_id}/Observation',
                              request_data=report_json,
                              expected_status=expected_status)

    def post_report_review(self, review_json, report_id, participant_id, expected_status=200):
        return self.send_post(f'Participant/P{participant_id}/Observation/{report_id}/Review',
                              request_data=review_json,
                              expected_status=expected_status)

    def get_report_from_db(self, report_id):
        # The report might already be in the session, resetting just in case to make sure we get the latest data
        self.session.commit()
        self.session.close()
        return self.session.query(DeceasedReport).filter(DeceasedReport.id == report_id).one()

    def get_participant_summary_from_db(self, participant_id):
        # The participant summary exists in the session, so we need to reset the session to query the database for
        # new values
        self.session.commit()
        self.session.close()
        return self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == participant_id
        ).one()

    @staticmethod
    def build_deceased_report_json(status='preliminary', date_of_death='2020-01-01',
                                   notification=DeceasedNotification.EHR, notification_other=None, user_system='system',
                                   user_name='name', authored='2020-01-01T00:00:00Z', reporter_name='Jane Doe',
                                   reporter_relation='SPOUSE', reporter_phone=None,
                                   reporter_email=None):
        report_json = {
            'code': {
                'text': 'DeceasedReport'
            },
            'status': status,
            'effectiveDateTime': date_of_death,
            'performer': [{
                'type': user_system,
                'reference': user_name
            }],
            'issued': authored
        }

        encounter_json = {
            'reference': str(notification)
        }
        if notification == DeceasedNotification.OTHER:
            encounter_json['display'] = notification_other
        report_json['encounter'] = encounter_json

        if not (notification == DeceasedNotification.EHR or notification == DeceasedNotification.OTHER):
            extensions = [{
                'url': 'http://hl7.org/fhir/ValueSet/relatedperson-relationshiptype',
                'valueCode': reporter_relation
            }]
            if reporter_email:
                extensions.append({
                    'url': 'https://www.pmi-ops.org/email-address',
                    'valueString': reporter_email
                })
            if reporter_phone:
                extensions.append({
                    'url': 'https://www.pmi-ops.org/phone-number',
                    'valueString': reporter_phone
                })

            report_json['extension'] = [{
                'url': 'https://www.pmi-ops.org/deceased-reporter',
                'valueHumanName': {
                    'text': reporter_name,
                    'extension': extensions
                }
            }]

        return report_json

    @staticmethod
    def build_report_review_json(user_system='system', user_name='name', authored='2020-01-01T00:00:00Z',
                                 status='final', denial_reason=DeceasedReportDenialReason.MARKED_IN_ERROR,
                                 denial_reason_other='Another reason', date_of_death='2020-01-01'):
        report_json = {
            'code': {
                'text': 'DeceasedReport'
            },
            'status': status,
            'effectiveDateTime': date_of_death,
            'performer': [{
                'type': user_system,
                'reference': user_name
            }],
            'issued': authored
        }

        if status == 'cancelled':
            denial_reference = {
                'reference': str(denial_reason)
            }
            if denial_reason == DeceasedReportDenialReason.OTHER:
                denial_reference['display'] = denial_reason_other

            report_json['extension'] = [{
                'url': 'https://www.pmi-ops.org/observation-denial-reason',
                'valueReference': denial_reference
            }]

        return report_json

    def assertReportResponseMatches(self, expected, actual):
        del actual['identifier']
        del actual['subject']
        del actual['resourceType']
        self.assertJsonResponseMatches(expected, actual, strip_tz=False)

    def test_creating_minimal_deceased_report(self):
        report_json = self.build_deceased_report_json(
            status='preliminary',
            date_of_death='2020-01-02',
            notification=DeceasedNotification.EHR,
            user_system='https://example.com',
            user_name='me@test.com',
            authored='2020-01-05T13:43:21Z'
        )
        response = self.post_report(report_json, participant_id=self.paired_participant_with_summary.participantId)

        participant_summary = self.get_participant_summary_from_db(
            participant_id=self.paired_participant_with_summary.participantId
        )

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.PENDING, created_report.status)
        self.assertEqual(date(2020, 1, 2), created_report.dateOfDeath)
        self.assertEqual(DeceasedNotification.EHR, created_report.notification)
        self.assertEqual('https://example.com', created_report.author.system)
        self.assertEqual('me@test.com', created_report.author.username)
        self.assertEqual(datetime(2020, 1, 5, 13, 43, 21), created_report.authored)

        self.assertEqual(DeceasedStatus.PENDING, participant_summary.deceasedStatus)
        self.assertEqual(datetime(2020, 1, 5, 13, 43, 21), participant_summary.deceasedAuthored)
        self.assertEqual(date(2020, 1, 2), participant_summary.dateOfDeath)

        self.assertReportResponseMatches(report_json, response)

    def test_other_notification_method(self):
        report_json = self.build_deceased_report_json(
            notification=DeceasedNotification.OTHER,
            notification_other='Another reason'
        )
        response = self.post_report(report_json)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedNotification.OTHER, created_report.notification)
        self.assertEqual('Another reason', created_report.notificationOther)

        self.assertReportResponseMatches(report_json, response)

    def test_reporter_info(self):
        report_json = self.build_deceased_report_json(
            notification=DeceasedNotification.NEXT_KIN_SUPPORT,
            reporter_name='Jane Doe',
            reporter_relation='SPOUSE',
            reporter_phone='123-456-7890',
            reporter_email='jdoe@me.com'
        )
        response = self.post_report(report_json)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedNotification.NEXT_KIN_SUPPORT, created_report.notification)
        self.assertEqual('Jane Doe', created_report.reporterName)
        self.assertEqual('SPOUSE', created_report.reporterRelationship)
        self.assertEqual('123-456-7890', created_report.reporterPhone)
        self.assertEqual('jdoe@me.com', created_report.reporterEmail)

        self.assertReportResponseMatches(report_json, response)

    def test_naive_issued_timestamp(self):
        report_json = self.build_deceased_report_json(
            authored='2020-01-05T13:43:21'
        )
        response = self.post_report(report_json)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(datetime(2020, 1, 5, 13, 43, 21), created_report.authored)

        self.assertEqual('2020-01-05T13:43:21Z', response['issued'])

    def test_cst_issued_timestamp(self):
        report_json = self.build_deceased_report_json(
            authored='2020-01-05T13:43:21-06:00'
        )
        response = self.post_report(report_json)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(datetime(2020, 1, 5, 19, 43, 21), created_report.authored)

        self.assertEqual('2020-01-05T19:43:21Z', response['issued'])

    def test_post_with_invalid_fields(self):
        # Check missing status response
        report_json = self.build_deceased_report_json()
        del report_json['status']
        self.post_report(report_json, expected_status=400)

        # Check unauthorized status when creating
        report_json = self.build_deceased_report_json(status='final')
        self.post_report(report_json, expected_status=400)

        # Check missing code response
        report_json = self.build_deceased_report_json()
        del report_json['code']
        self.post_report(report_json, expected_status=400)

        # Check missing notification data response
        report_json = self.build_deceased_report_json()
        del report_json['encounter']
        self.post_report(report_json, expected_status=400)

        # Check missing 'other text' when notification is OTHER
        report_json = self.build_deceased_report_json(notification=DeceasedNotification.OTHER)
        del report_json['encounter']['display']
        self.post_report(report_json, expected_status=400)

        # Check for different states of missing author information
        report_json = self.build_deceased_report_json()
        del report_json['performer']
        self.post_report(report_json, expected_status=400)
        report_json = self.build_deceased_report_json()
        del report_json['performer'][0]['type']
        self.post_report(report_json, expected_status=400)
        report_json = self.build_deceased_report_json()
        del report_json['performer'][0]['reference']
        self.post_report(report_json, expected_status=400)

        # Check for missing authored date (referred to as 'issued' for FHIR compliance)
        report_json = self.build_deceased_report_json()
        del report_json['issued']
        self.post_report(report_json, expected_status=400)

        # Check for response when missing pieces of reporter information
        report_json = self.build_deceased_report_json(notification=DeceasedNotification.NEXT_KIN_SUPPORT)
        del report_json['extension']
        self.post_report(report_json, expected_status=400)
        report_json = self.build_deceased_report_json(notification=DeceasedNotification.NEXT_KIN_SUPPORT)
        del report_json['extension'][0]['valueHumanName']['text']
        self.post_report(report_json, expected_status=400)
        report_json = self.build_deceased_report_json(notification=DeceasedNotification.NEXT_KIN_SUPPORT)
        del report_json['extension'][0]['valueHumanName']['extension'][0]  # deleting association (only required one)
        self.post_report(report_json, expected_status=400)

        # Try invalid status
        report_json = self.build_deceased_report_json(status='unknown')
        self.post_report(report_json, expected_status=400)

        self.create_pending_deceased_report()

    def test_post_with_only_required_fields(self):
        report_json = self.build_deceased_report_json()
        del report_json['effectiveDateTime']

        response = self.post_report(report_json, participant_id=self.paired_participant_with_summary.participantId)
        del response['effectiveDateTime']
        self.assertReportResponseMatches(report_json, response)

        participant_summary = self.get_participant_summary_from_db(
            participant_id=self.paired_participant_with_summary.participantId
        )
        self.assertIsNone(participant_summary.dateOfDeath)

    def test_other_roles_not_allowed_to_create(self):
        report_json = self.build_deceased_report_json()
        self.overwrite_test_user_roles(['testing'])
        self.post_report(report_json, expected_status=403)

    def test_health_pro_can_create(self):
        report_json = self.build_deceased_report_json()
        self.overwrite_test_user_roles([HEALTHPRO])
        self.post_report(report_json)

    def test_ptsc_can_create(self):
        report_json = self.build_deceased_report_json()
        self.overwrite_test_user_roles([PTC])
        self.post_report(report_json)

    def test_report_auto_approve(self):
        # Deceased reports made for unpaired participants don't need second approval.
        # So these reports should be approved upon creation.
        unpaired_participant_id = self.unpaired_participant_with_summary.participantId

        report_json = self.build_deceased_report_json()
        response = self.post_report(report_json, participant_id=unpaired_participant_id)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status)

        self.assertEqual('final', response['status'])

        participant_summary = self.get_participant_summary_from_db(participant_id=unpaired_participant_id)
        self.assertEqual(DeceasedStatus.APPROVED, participant_summary.deceasedStatus)
        self.assertEqual(datetime(2020, 1, 1), participant_summary.deceasedAuthored)

    def create_pending_deceased_report(self, participant_id=None, **kwargs):
        if participant_id is None:
            participant_id = self.paired_participant_without_summary.participantId

        return self.data_generator.create_database_deceased_report(participantId=participant_id, **kwargs)

    def test_multiple_pending_reports_not_allowed(self):
        report = self.create_pending_deceased_report()

        # Try creating another deceased report and check for Conflict status code
        report_json = self.build_deceased_report_json()
        self.post_report(report_json, participant_id=report.participantId, expected_status=409)

    def test_approving_report(self):
        report = self.create_pending_deceased_report(
            participant_id=self.paired_participant_with_summary.participantId
        )

        review_json = self.build_report_review_json(
            status='final',
            authored='2020-07-01T00:00:00Z',
            user_system='https://example.com',
            user_name='reviewer@test.com'
        )
        review_response = self.post_report_review(review_json, report.id, report.participantId)

        created_report = self.get_report_from_db(report.id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status)
        self.assertEqual(datetime(2020, 7, 1), created_report.reviewed)
        self.assertEqual('https://example.com', created_report.reviewer.system)
        self.assertEqual('reviewer@test.com', created_report.reviewer.username)

        self.assertEqual('final', review_response['status'])

        participant_summary = self.get_participant_summary_from_db(participant_id=report.participantId)
        self.assertEqual(DeceasedStatus.APPROVED, participant_summary.deceasedStatus)
        self.assertEqual(datetime(2020, 7, 1), participant_summary.deceasedAuthored)

    def test_approving_can_overwrite_date_of_death(self):
        participant_id = self.paired_participant_with_summary.participantId
        report_json = self.build_deceased_report_json(date_of_death='2020-01-01')
        response = self.post_report(report_json, participant_id=participant_id)
        report_id = self.get_deceased_report_id(response)

        participant_summary = self.get_participant_summary_from_db(participant_id=participant_id)
        self.assertEqual(date(2020, 1, 1), participant_summary.dateOfDeath)

        review_json = self.build_report_review_json(
            date_of_death='2022-06-01'
        )
        self.post_report_review(review_json, report_id, participant_id)

        created_report = self.get_report_from_db(report_id)
        self.assertEqual(date(2022, 6, 1), created_report.dateOfDeath)

        participant_summary = self.get_participant_summary_from_db(participant_id=participant_id)
        self.assertEqual(date(2022, 6, 1), participant_summary.dateOfDeath)

    def test_only_healthpro_can_review(self):
        report = self.create_pending_deceased_report()
        review_json = self.build_report_review_json()

        self.overwrite_test_user_roles(['testing'])
        self.post_report_review(review_json, report.id, report.participantId, expected_status=403)

        self.overwrite_test_user_roles([PTC], save_current=False)
        self.post_report_review(review_json, report.id, report.participantId, expected_status=403)

        self.overwrite_test_user_roles([HEALTHPRO], save_current=False)
        self.post_report_review(review_json, report.id, report.participantId, expected_status=200)

    def test_report_denial(self):
        report = self.create_pending_deceased_report(
            participant_id=self.paired_participant_with_summary.participantId
        )

        review_json = self.build_report_review_json(
            status='cancelled',
            denial_reason=DeceasedReportDenialReason.OTHER,
            denial_reason_other='Another reason'
        )
        review_response = self.post_report_review(review_json, report.id, report.participantId)

        created_report = self.get_report_from_db(report.id)
        self.assertEqual(DeceasedReportStatus.DENIED, created_report.status)
        self.assertEqual(DeceasedReportDenialReason.OTHER, created_report.denialReason)
        self.assertEqual('Another reason', created_report.denialReasonOther)

        participant_summary = self.get_participant_summary_from_db(participant_id=report.participantId)
        self.assertEqual(DeceasedStatus.UNSET, participant_summary.deceasedStatus)
        self.assertIsNone(participant_summary.deceasedAuthored)
        self.assertIsNone(participant_summary.dateOfDeath)

        # Check that the denial reason comes through on the response
        self.assertEqual('cancelled', review_response['status'])

        denial_extension = review_response['extension'][0]['valueReference']
        self.assertEqual('OTHER', denial_extension['reference'])
        self.assertEqual('Another reason', denial_extension['display'])

    def test_pending_report_not_allowed_when_approved_report_exists(self):
        report = self.create_pending_deceased_report()
        review_json = self.build_report_review_json()
        self.post_report_review(review_json, report.id, report.participantId)

        created_report = self.get_report_from_db(report.id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status,
                         "Test is built assuming an APPROVED report would be created")

        # Try creating another deceased report and check for Conflict status code
        report_json = self.build_deceased_report_json()
        self.post_report(report_json, participant_id=report.participantId, expected_status=409)

    def test_multiple_denied_reports(self):
        report = self.create_pending_deceased_report()
        review_json = self.build_report_review_json(status='cancelled')
        self.post_report_review(review_json, report.id, report.participantId)

        # Build another report and deny it too
        report = self.create_pending_deceased_report(participant_id=report.participantId)
        self.post_report_review(review_json, report.id, report.participantId)

        # Try creating another deceased report, expecting it to work
        report = self.create_pending_deceased_report(participant_id=report.participantId)

        created_report = self.get_report_from_db(report.id)
        self.assertEqual(DeceasedReportStatus.PENDING, created_report.status)

    def test_approving_denied_report_not_allowed(self):
        report = self.create_pending_deceased_report()
        review_json = self.build_report_review_json(status='cancelled')
        self.post_report_review(review_json, report.id, report.participantId)

        # Try approving the denied report
        review_json = self.build_report_review_json(status='final')
        self.post_report_review(review_json, report.id, report.participantId, expected_status=400)

    def test_denying_approved_report_not_allowed(self):
        report = self.create_pending_deceased_report()
        review_json = self.build_report_review_json(status='final')
        self.post_report_review(review_json, report.id, report.participantId)

        # Try approving the denied report
        review_json = self.build_report_review_json(status='cancelled')
        self.post_report_review(review_json, report.id, report.participantId, expected_status=400)

    def test_api_users_not_duplicated(self):
        report = self.create_pending_deceased_report()

        created_report = self.get_report_from_db(report.id)

        review_json = self.build_report_review_json(
            user_system=created_report.author.system,
            user_name=created_report.author.username
        )
        self.post_report_review(review_json, report.id, report.participantId)

        self.assertEqual(1, self.session.query(ApiUser).count())

    def test_participant_summary_fields_redacted(self):
        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(
            participant=participant,
            phoneNumber='123-456-7890',
            loginPhoneNumber='1-800-555-5555',
            email='test@me.com',
            streetAddress='123 Elm',
            streetAddress2='Unit A',
            city='Eureka',
            zipCode='12345'
        )

        participant_id = participant.participantId
        report_json = self.build_deceased_report_json(authored="2020-01-01T00:00:00Z")
        response = self.post_report(report_json, participant_id=participant_id)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status,
                         "Test is built assuming an APPROVED report would be created")

        summary_response = self.send_get(f'Participant/P{participant_id}/Summary')
        for field in ['phoneNumber', 'loginPhoneNumber', 'email', 'streetAddress', 'streetAddress2', 'city', 'zipCode']:
            self.assertEqual('UNSET', summary_response[field])
        self.assertEqual('NO_CONTACT', summary_response['recontactMethod'])

    def test_participant_summary_redact_time_window(self):
        # Fields should still be available for a short time window
        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(
            participant=participant,
            phoneNumber='123-456-7890'
        )

        participant_id = participant.participantId
        yesterday = datetime.now() - timedelta(days=1)
        report_json = self.build_deceased_report_json(authored=yesterday.isoformat())
        response = self.post_report(report_json, participant_id=participant_id)

        report_id = self.get_deceased_report_id(response)
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status,
                         "Test is built assuming an APPROVED report would be created")

        summary_response = self.send_get(f'Participant/P{participant_id}/Summary')
        self.assertEqual('123-456-7890', summary_response['phoneNumber'])
        self.assertEqual('NO_CONTACT', summary_response['recontactMethod'])


class ParticipantDeceasedReportApiTest(DeceasedReportTestBase):
    def test_report_list_for_participant(self):
        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 3, 18, tzinfo=pytz.utc)
        )
        self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 2, 27, tzinfo=pytz.utc)
        )
        self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 4, 1, tzinfo=pytz.utc)
        )

        report_list_response = self.send_get(f'Participant/P{participant.participantId}/DeceasedReport')

        first_report = report_list_response[0]  # Most recent report
        self.assertEqual('cancelled', first_report['status'])
        self.assertEqual('2020-04-01T00:00:00Z', first_report['issued'])

        second_report = report_list_response[1]
        self.assertEqual('cancelled', second_report['status'])
        self.assertEqual('2020-03-18T00:00:00Z', second_report['issued'])

        third_report = report_list_response[2]
        self.assertEqual('cancelled', third_report['status'])
        self.assertEqual('2020-02-27T00:00:00Z', third_report['issued'])


class SearchDeceasedReportApiTest(DeceasedReportTestBase):
    def setUp(self):
        super(SearchDeceasedReportApiTest, self).setUp()

        # Shortening the following lines
        create_participant_func = self.data_generator.create_database_participant
        create_deceased_report_func = self.data_generator.create_database_deceased_report

        unpaired_participant_id_1 = create_participant_func().participantId
        self.unpaired_1_report_id = create_deceased_report_func(
            participantId=unpaired_participant_id_1,
            status=DeceasedReportStatus.PENDING,
            authored=datetime(2020, 4, 1)
        ).id
        unpaired_participant_id_2 = create_participant_func().participantId
        self.unpaired_2_report_id = create_deceased_report_func(
            participantId=unpaired_participant_id_2,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 1, 5)
        ).id
        unpaired_participant_id_3 = create_participant_func().participantId
        self.unpaired_3_report_id = create_deceased_report_func(
            participantId=unpaired_participant_id_3,
            status=DeceasedReportStatus.PENDING,
            authored=datetime(2020, 2, 18)
        ).id

        test_org = self.data_generator.create_database_organization(externalId='TEST')
        test_participant_1_id = create_participant_func(organizationId=test_org.organizationId).participantId
        self.test_1_report_id = create_deceased_report_func(
            participantId=test_participant_1_id,
            status=DeceasedReportStatus.PENDING,
            authored=datetime(2020, 12, 5)
        ).id
        test_participant_2_id = create_participant_func(organizationId=test_org.organizationId).participantId
        self.test_2_report_id = create_deceased_report_func(
            participantId=test_participant_2_id,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 8, 9)
        ).id
        test_participant_3_id = create_participant_func(organizationId=test_org.organizationId).participantId
        self.test_3_report_id = create_deceased_report_func(
            participantId=test_participant_3_id,
            status=DeceasedReportStatus.APPROVED,
            authored=datetime(2020, 2, 3)
        ).id

        other_org = self.data_generator.create_database_organization(externalId='')
        other_participant_1_id = create_participant_func(organizationId=other_org.organizationId).participantId
        self.other_1_report_id = create_deceased_report_func(
            participantId=other_participant_1_id,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 5, 19)
        ).id
        other_participant_2_id = create_participant_func(organizationId=other_org.organizationId).participantId
        self.other_2_report_id = create_deceased_report_func(
            participantId=other_participant_2_id,
            status=DeceasedReportStatus.DENIED,
            authored=datetime(2020, 9, 5)
        ).id
        other_participant_3_id = create_participant_func(organizationId=other_org.organizationId).participantId
        self.other_3_report_id = create_deceased_report_func(
            participantId=other_participant_3_id,
            status=DeceasedReportStatus.APPROVED,
            authored=datetime(2020, 9, 7)
        ).id

    def assertListResponseMatches(self, expected_report_ids, actual_json):
        self.assertEqual(len(expected_report_ids), len(actual_json), "Unexpected number of reports returned")

        for index in range(len(expected_report_ids)):
            expected_id = expected_report_ids[index]
            report_json = actual_json[index]

            self.assertEqual(int(expected_id), self.get_deceased_report_id(report_json), 'Report id mismatch')

    def test_searching_api_by_status(self):
        self.assertListResponseMatches([
            self.other_2_report_id,     # Authored 09/05
            self.test_2_report_id,      # Authored 08/09
            self.other_1_report_id,     # Authored 05/19
            self.unpaired_2_report_id   # Authored 01/05
        ], self.send_get(f'DeceasedReports?status=cancelled'))

        self.assertListResponseMatches([
            self.test_1_report_id,      # Authored 12/05
            self.unpaired_1_report_id,  # Authored 04/01
            self.unpaired_3_report_id   # Authored 02/18
        ], self.send_get(f'DeceasedReports?status=preliminary'))

    def test_searching_api_by_organization(self):
        self.assertListResponseMatches([
            self.test_1_report_id,      # Authored 12/05
            self.test_2_report_id,      # Authored 08/09
            self.test_3_report_id       # Authored 02/03
        ], self.send_get(f'DeceasedReports?org_id=TEST'))

        self.assertListResponseMatches([
            self.unpaired_1_report_id,  # Authored 04/01
            self.unpaired_3_report_id,  # Authored 02/18
            self.unpaired_2_report_id   # Authored 01/05
        ], self.send_get(f'DeceasedReports?org_id=UNSET'))

    def test_searching_api_by_org_and_status(self):
        self.assertListResponseMatches(
            [],
            self.send_get(f'DeceasedReports?org_id=OTHER&status=preliminary'))

        self.assertListResponseMatches([
            self.unpaired_1_report_id,  # Authored 04/01
            self.unpaired_3_report_id  # Authored 02/18
        ], self.send_get(f'DeceasedReports?org_id=UNSET&status=preliminary'))

    def test_searching_api_by_org_and_status(self):
        self.overwrite_test_user_roles(['TEST'])
        self.send_get(f'DeceasedReports', expected_status=403)

        self.overwrite_test_user_roles([PTC], save_current=False)
        self.send_get(f'DeceasedReports', expected_status=403)

        self.overwrite_test_user_roles([HEALTHPRO], save_current=False)
        self.send_get(f'DeceasedReports', expected_status=200)
