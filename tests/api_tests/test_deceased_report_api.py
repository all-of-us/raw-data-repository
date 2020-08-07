from datetime import date, datetime

from rdr_service import config
from rdr_service.api_util import HEALTHPRO, PTC
from rdr_service.model.api_user import ApiUser
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportDenialReason, DeceasedReportStatus
from tests.helpers.unittest_base import BaseTestCase


class DeceasedReportApiTest(BaseTestCase):
    def setUp(self):
        super(DeceasedReportApiTest, self).setUp()
        self.original_user_roles = None

    def tearDown(self):
        if self.original_user_roles is not None:
            print("RESTORING ROLES", self.original_user_roles)
            self.overwrite_test_user_roles(self.original_user_roles, save_current=False)
        else:
            print('SKIPPING RESTORE')

        super(DeceasedReportApiTest, self).tearDown()

    def post_report(self, report_json, participant_id=None, expected_status=200):
        if participant_id is None:
            hpo = self.data_generator.create_database_hpo()
            participant_id = self.data_generator.create_database_participant(hpoId=hpo.hpoId).participantId
        return self.send_post(f'Participant/P{participant_id}/Observation',
                              request_data=report_json,
                              expected_status=expected_status)

    def post_report_review(self, review_json, report_id, participant_id, expected_status=200):
        return self.send_post(f'Participant/P{participant_id}/Observation/{report_id}/Review',
                              request_data=review_json,
                              expected_status=expected_status)

    def get_report_from_db(self, report_id):
        return self.session.query(DeceasedReport).filter(DeceasedReport.id == report_id).one()

    def overwrite_test_user_roles(self, roles, save_current=True):
        user_info = config.getSettingJson(config.USER_INFO)

        if save_current:
            # Save what was there so we can set it back in the tearDown
            self.original_user_roles = user_info['example@example.com']['roles']
            print("SAVING ROLES", self.original_user_roles)

        print("SETTING ROLES", roles)
        user_info['example@example.com']['roles'] = roles
        config.override_setting(config.USER_INFO, user_info)

    @staticmethod
    def build_deceased_report_json(status='preliminary', date_of_death='2020-01-01',
                                   notification=DeceasedNotification.EHR, notification_other=None, user_system='system',
                                   user_name='name', authored='2020-01-01T00:00:00+00:00', reporter_name='Jane Doe',
                                   reporter_relation='SPOUSE', reporter_phone=None,
                                   reporter_email=None):
        report_json = {
            'code': {
                'text': 'DeceasedReport'
            },
            'status': status,
            'effectiveDateTime': date_of_death,
            'performer': {
                'type': user_system,
                'reference': user_name
            },
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
                'url': 'https://www.pmi-ops.org/association',
                'valueCode': reporter_relation
            }]
            if reporter_phone:
                extensions.append({
                    'url': 'https://www.pmi-ops.org/phone-number',
                    'valueString': reporter_phone
                })
            if reporter_email:
                extensions.append({
                    'url': 'https://www.pmi-ops.org/email-address',
                    'valueString': reporter_email
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
    def build_report_review_json(user_system='system', user_name='name', authored='2020-01-01T00:00:00+00:00',
                            status='final', denial_reason=DeceasedReportDenialReason.MARKED_IN_ERROR,
                            denial_reason_other='Another reason', date_of_death='2020-01-01'):
        report_json = {
            'code': {
                'text': 'DeceasedReport'
            },
            'status': status,
            'effectiveDateTime': date_of_death,
            'performer': {
                'type': user_system,
                'reference': user_name
            },
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
        self.assertJsonResponseMatches(expected, actual, strip_tz=False)

    def test_creating_minimal_deceased_report(self):
        report_json = self.build_deceased_report_json(
            status='preliminary',
            date_of_death='2020-01-02',
            notification=DeceasedNotification.EHR,
            user_system='https://example.com',
            user_name='me@test.com',
            authored='2020-01-05T13:43:21+00:00'
        )
        response = self.post_report(report_json)

        report_id = response['identifier']['value']
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.PENDING, created_report.status)
        self.assertEqual(date(2020, 1, 2), created_report.dateOfDeath)
        self.assertEqual(DeceasedNotification.EHR, created_report.notification)
        self.assertEqual('https://example.com', created_report.author.system)
        self.assertEqual('me@test.com', created_report.author.username)
        self.assertEqual(datetime(2020, 1, 5, 13, 43, 21), created_report.authored)

        self.assertReportResponseMatches(report_json, response)

    def test_other_notification_method(self):
        report_json = self.build_deceased_report_json(
            notification=DeceasedNotification.OTHER,
            notification_other='Another reason'
        )
        response = self.post_report(report_json)

        report_id = response['identifier']['value']
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

        report_id = response['identifier']['value']
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

        report_id = response['identifier']['value']
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(datetime(2020, 1, 5, 13, 43, 21), created_report.authored)

        self.assertEqual('2020-01-05T13:43:21+00:00', response['issued'])

    def test_cst_issued_timestamp(self):
        report_json = self.build_deceased_report_json(
            authored='2020-01-05T13:43:21-06:00'
        )
        response = self.post_report(report_json)

        report_id = response['identifier']['value']
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(datetime(2020, 1, 5, 19, 43, 21), created_report.authored)

        self.assertEqual('2020-01-05T19:43:21+00:00', response['issued'])

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
        del report_json['performer']['type']
        self.post_report(report_json, expected_status=400)
        report_json = self.build_deceased_report_json()
        del report_json['performer']['reference']
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

        response = self.post_report(report_json)
        self.assertReportResponseMatches(report_json, response)

    def test_report_creation_authorization(self):
        # Check that other roles can't create deceased reports
        report_json = self.build_deceased_report_json()
        self.overwrite_test_user_roles(['testing'])
        self.post_report(report_json, expected_status=403)

        # Check that HPRO is allowed
        report_json = self.build_deceased_report_json()
        self.overwrite_test_user_roles([HEALTHPRO], save_current=False)
        self.post_report(report_json)

        # Check that PTSC is allowed
        report_json = self.build_deceased_report_json()
        self.overwrite_test_user_roles([PTC], save_current=False)
        self.post_report(report_json)

    def test_report_auto_approve(self):
        # Deceased reports made for unpaired participants don't need second approval.
        # So these reports should be approved upon creation.
        unpaired_participant = self.data_generator.create_database_participant()

        report_json = self.build_deceased_report_json()
        response = self.post_report(report_json, participant_id=unpaired_participant.participantId)

        report_id = response['identifier']['value']
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status)

        self.assertEqual('final', response['status'])

    def test_multiple_pending_reports_not_allowed(self):
        hpo = self.data_generator.create_database_hpo()
        participant = self.data_generator.create_database_participant(hpoId=hpo.hpoId)
        #todo: refactor out a paired_participant

        report_json = self.build_deceased_report_json()
        response = self.post_report(report_json, participant_id=participant.participantId)

        report_id = response['identifier']['value']
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.PENDING, created_report.status,
                         "Test is built assuming a PENDING report would be created")

        # Try creating another deceased report and check for Conflict status code
        self.post_report(report_json, participant_id=participant.participantId, expected_status=409)

    def create_pending_deceased_report(self, set_date_of_death=True):
        hpo = self.data_generator.create_database_hpo()
        participant = self.data_generator.create_database_participant(hpoId=hpo.hpoId)
        report_json = self.build_deceased_report_json()
        if not set_date_of_death:
            del report_json['effectiveDateTime']

        response = self.post_report(report_json, participant_id=participant.participantId)
        return response['identifier']['value'], participant.participantId

    def test_approving_report(self):
        report_id, participant_id = self.create_pending_deceased_report()

        review_json = self.build_report_review_json(
            status='final',
            authored='2020-07-01T00:00:00+00:00',
            user_system='https://example.com',
            user_name='reviewer@test.com'
        )
        review_response = self.post_report_review(review_json, report_id, participant_id)

        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status)
        self.assertEqual(datetime(2020, 7, 1), created_report.reviewed)
        self.assertEqual('https://example.com', created_report.reviewer.system)
        self.assertEqual('reviewer@test.com', created_report.reviewer.username)

        self.assertEqual('final', review_response['status'])

    def test_approving_can_overwrite_date_of_death(self):
        report_id, participant_id = self.create_pending_deceased_report()

        review_json = self.build_report_review_json(
            date_of_death='2022-06-01'
        )
        self.post_report_review(review_json, report_id, participant_id)

        created_report = self.get_report_from_db(report_id)
        self.assertEqual(date(2022, 6, 1), created_report.dateOfDeath)

    def test_only_healthpro_can_review(self):
        report_id, participant_id = self.create_pending_deceased_report()
        review_json = self.build_report_review_json()

        self.overwrite_test_user_roles(['testing'])
        self.post_report_review(review_json, report_id, participant_id, expected_status=403)

        self.overwrite_test_user_roles([PTC], save_current=False)
        self.post_report_review(review_json, report_id, participant_id, expected_status=403)

    def test_report_denial(self):
        report_id, participant_id = self.create_pending_deceased_report()

        review_json = self.build_report_review_json(
            status='cancelled',
            denial_reason=DeceasedReportDenialReason.OTHER,
            denial_reason_other='Another reason'
        )
        review_response = self.post_report_review(review_json, report_id, participant_id)

        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.DENIED, created_report.status)
        self.assertEqual(DeceasedReportDenialReason.OTHER, created_report.denialReason)
        self.assertEqual('Another reason', created_report.denialReasonOther)

        self.assertEqual('cancelled', review_response['status'])

    def test_pending_report_with_approved_not_allowed(self):
        report_id, participant_id = self.create_pending_deceased_report()
        review_json = self.build_report_review_json()
        self.post_report_review(review_json, report_id, participant_id)

        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.APPROVED, created_report.status,
                         "Test is built assuming an APPROVED report would be created")

        # Try creating another deceased report and check for Conflict status code
        report_json = self.build_deceased_report_json()
        self.post_report(report_json, participant_id=participant_id, expected_status=409)

    def test_multiple_denied_reports(self):
        report_id, participant_id = self.create_pending_deceased_report()
        review_json = self.build_report_review_json(status='cancelled')
        self.post_report_review(review_json, report_id, participant_id)

        # Build another report and deny it too
        report_json = self.build_deceased_report_json()
        response = self.post_report(report_json, participant_id=participant_id)
        report_id = response['identifier']['value']
        self.post_report_review(review_json, report_id, participant_id)

        # Try creating another deceased report, expecting it to work
        response = self.post_report(report_json, participant_id=participant_id)

        report_id = response['identifier']['value']
        created_report = self.get_report_from_db(report_id)
        self.assertEqual(DeceasedReportStatus.PENDING, created_report.status)

    def test_approving_denied_report_not_allowed(self):
        report_id, participant_id = self.create_pending_deceased_report()
        review_json = self.build_report_review_json(status='cancelled')
        self.post_report_review(review_json, report_id, participant_id)

        # Try approving the denied report
        review_json = self.build_report_review_json(status='final')
        self.post_report_review(review_json, report_id, participant_id, expected_status=400)

    def test_denying_approved_report_not_allowed(self):
        report_id, participant_id = self.create_pending_deceased_report()
        review_json = self.build_report_review_json(status='final')
        self.post_report_review(review_json, report_id, participant_id)

        # Try approving the denied report
        review_json = self.build_report_review_json(status='cancelled')
        self.post_report_review(review_json, report_id, participant_id, expected_status=400)

    def test_api_users_not_duplicated(self):
        report_id, participant_id = self.create_pending_deceased_report()

        created_report = self.get_report_from_db(report_id)

        review_json = self.build_report_review_json(
            user_system=created_report.author.system,
            user_name=created_report.author.username
        )
        self.post_report_review(review_json, report_id, participant_id)

        self.assertEqual(1, self.session.query(ApiUser).count())

    # todo: check for participant summary changes

    # todo: refactor everything
