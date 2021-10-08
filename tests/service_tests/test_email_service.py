import mock

from rdr_service import config
from rdr_service.services.email import Email, EmailService
from tests.helpers.unittest_base import BaseTestCase


class EmailServiceTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(EmailServiceTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(EmailServiceTest, self).setUp(*args, **kwargs)
        self.temporarily_override_config_setting(config.SENDGRID_KEY, ['test_key'])

    def test_from_email_default(self):
        """Test that an email without a from-email provided will default to the no-reply email"""
        email = Email()
        self.assertEqual(config.SENDGRID_FROM_EMAIL, email.from_email)

    @mock.patch('rdr_service.services.email.sendgrid')
    def test_sendgrid_email_data_structure(self, sendgrid_mock):
        """Make sure the email data structure is formed correctly when sending an email to SendGrid"""
        email = Email(
            from_email='unit@test.me',
            recipients=[
                'first@test.com',
                'another@foo.baz'
            ],
            subject='Testing email data structure for SendGrid',
            plain_text_content='This is the content\n\tof the test email'
        )
        EmailService.send_email(email)

        sendgrid_post_mock = sendgrid_mock.SendGridAPIClient.return_value.client.mail.send.post
        sent_email_dict = sendgrid_post_mock.call_args.kwargs['request_body']
        self.assertEqual({
            'personalizations': [
                {
                    'to': [
                        {'email': 'first@test.com'},
                        {'email': 'another@foo.baz'}
                    ],
                    'subject': 'Testing email data structure for SendGrid',
                }
            ],
            'from': {
                'email': 'unit@test.me'
            },
            'content': [
                {
                    'type': 'text/plain',
                    'value': 'This is the content\n\tof the test email'
                }
            ]
        }, sent_email_dict)

