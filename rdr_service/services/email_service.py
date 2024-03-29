from typing import List

from sendgrid import sendgrid

from rdr_service import config


class Email:
    """Object for encapsulating the data for an email"""
    def __init__(
        self,
        subject: str = '',
        recipients: List[str] = None,
        cc_recipients: List[str] = None,
        from_email: str = None,
        plain_text_content: str = ''
    ):

        self.subject = subject
        self.plain_text_content = plain_text_content

        if from_email is None:
            from_email = config.SENDGRID_FROM_EMAIL
        self.from_email = from_email

        if recipients is None:
            recipients = []
        self.recipients = recipients

        if cc_recipients is None:
            cc_recipients = []
        self.cc_recipients = cc_recipients


class EmailService:
    """
    Class for acting as an interface for sending an email.
    Will build the data structure as required by an external API (such as SendGrid).
    """
    @classmethod
    def send_email(cls, email: Email):
        sendgrid_client = sendgrid.SendGridAPIClient(api_key=config.getSetting(config.SENDGRID_KEY))

        sendgrid_email_dict = cls._sendgrid_dict_from_email(email)
        return sendgrid_client.client.mail.send.post(request_body=sendgrid_email_dict)

    @classmethod
    def _sendgrid_dict_from_email(cls, email: Email) -> dict:
        email_payload = {
            'personalizations': [
                {
                    'to': [{'email': recipient} for recipient in email.recipients],
                    'subject': email.subject
                }
            ],
            'from': {
                'email': email.from_email
            },
            'content': [
                {
                    'type': 'text/plain',
                    'value': email.plain_text_content
                }
            ]
        }

        if email.cc_recipients:
            email_payload['personalizations'][0]['cc'] = [{'email': recipient} for recipient in email.cc_recipients]

        return email_payload
