
import json
import logging
import requests

from rdr_service.config import GAE_PROJECT


class SlackMessageHandler:
    """
    Create Slack Messages
    """
    def __init__(self, *, webhook_url):
        self.webhook_url = webhook_url
        self.headers = {'Content-Type': 'application/json'}

    @staticmethod
    def valid_message_data(message):
        fields = ['text']
        return all(key in message for key in fields) and \
            all(val for val in message.values() if val is not None)

    def send_message_to_webhook(self, *, message_data):

        if not self.webhook_url:
            raise ValueError('Slack webhook is not set from config')

        if not self.valid_message_data(message_data):
            raise ValueError('Message payload is not valid')

        if self.webhook_url and GAE_PROJECT == 'all-of-us-rdr-prod':
            data = json.dumps(message_data)
            slack_response = requests.post(
                self.webhook_url,
                data,
                self.headers,
            )
            return slack_response
        else:
            logging.info('Suppressing slack message for non-prod environment')
            return


