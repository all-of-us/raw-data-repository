import base64
from dataclasses import dataclass, field
from io import StringIO
from typing import List

from sendgrid import sendgrid

from rdr_service import config


@dataclass
class Attachment:
    mime_type: str  # TODO: default?
    filename: str
    data: StringIO = field(default_factory=StringIO)
    disposition: str = 'attachment'


class Email:
    """Object for encapsulating the data for an email"""
    def __init__(self, subject: str = '', recipients: List[str] = None, from_email: str = None,
                 plain_text_content: str = '', attachments: List[Attachment] = None):
        self.subject = subject
        self.plain_text_content = plain_text_content
        self.attachments = attachments

        if from_email is None:
            from_email = config.SENDGRID_FROM_EMAIL
        self.from_email = from_email

        if recipients is None:
            recipients = []
        self.recipients = recipients


class EmailService:
    """
    Class for acting as an interface for sending an email.
    Will build the data structure as required by an external API (such as SendGrid).
    """

    @classmethod
    def send_email(cls, email: Email, server_config: dict = None):
        if server_config:
            sendgrid_api_key = server_config[config.SENDGRID_KEY][0]
        else:
            sendgrid_api_key = config.getSetting(config.SENDGRID_KEY)

        sendgrid_client = sendgrid.SendGridAPIClient(api_key=sendgrid_api_key)

        sendgrid_email_dict = cls._sendgrid_dict_from_email(email)
        return sendgrid_client.client.mail.send.post(request_body=sendgrid_email_dict)

    @classmethod
    def _sendgrid_dict_from_email(cls, email: Email) -> dict:
        sendgrid_data_dict = {
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

        if email.attachments:
            attachment_data_list = []
            for attachment in email.attachments:
                attachment.data.seek(0)

                data_bytes = attachment.data.read().encode('utf-8')
                base64_data_str = base64.b64encode(data_bytes).decode()

                attachment_data_list.append({
                    'content': base64_data_str,
                    'type': attachment.mime_type,
                    'filename': attachment.filename,
                    'disposition': attachment.disposition
                })
            sendgrid_data_dict['attachments'] = attachment_data_list

        return sendgrid_data_dict
