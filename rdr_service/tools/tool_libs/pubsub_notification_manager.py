import json
import logging
import os
import sys

from pprint import pprint
from google.cloud import storage
from google.api_core.exceptions import NotFound

from rdr_service.config import PUBSUB_NOTIFICATION_BUCKETS_PROD, PUBSUB_NOTIFICATION_BUCKETS_SANDBOX, \
    PUBSUB_NOTIFICATION_BUCKETS_STABLE
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'pubsub-manager'
tool_desc = 'Manage GCloud Pub/Sub Notifications'

_logger = logging.getLogger("rdr_logger")

CONFIG_ROOT = os.path.join(os.path.dirname(__file__), '../../config')
SUPPORTED_PROJECT_CONFIGS = {
    'all-of-us-rdr-prod': 'pub_sub_config_prod.json',
    'all-of-us-rdr-stable': 'pub_sub_config_stable.json',
    'all-of-us-rdr-sandbox': 'pub_sub_config_sandbox.json'
}


class PubSubNotificationManager(ToolBase):
    def run(self):
        if self.args.command == "list":
            return self.command_list()

        if self.args.command == "create":
            return self.command_create()

        if self.args.command == "delete":
            return self.command_delete()

    def command_list(self):
        """
        Lists all Pub/Sub notifications for all registered buckets.
        If --bucket is supplied, only lists Pub/Sub notifications for
        given bucket.
        """
        # Get buckets
        project_bucket_mappings = {
            'all-of-us-rdr-prod': PUBSUB_NOTIFICATION_BUCKETS_PROD,
            'all-of-us-rdr-stable': PUBSUB_NOTIFICATION_BUCKETS_STABLE,
            'all-of-us-rdr-sandbox': PUBSUB_NOTIFICATION_BUCKETS_SANDBOX,
        }

        bucket_list = [self.args.bucket] if self.args.bucket else project_bucket_mappings[self.gcp_env.project]

        notifications_dict = {
            "notifications": []
        }

        for bucket_name in bucket_list:
            # call storage api
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            notifications = bucket.list_notifications(client)

            for notification in notifications:
                # Skip the default topic notification (which won't have an integer ID"
                try:
                    id_int = int(notification.notification_id)
                except ValueError:
                    continue

                if self.args.id and self.args.id != id_int:
                    continue

                output_dict = dict()

                try:
                    output_dict['bucket'] = bucket_name
                    output_dict['id'] = notification.notification_id
                    output_dict['topic_name'] = notification.topic_name
                    output_dict['topic_project'] = notification.topic_project
                    output_dict['payload_format'] = notification.payload_format
                    output_dict['object_name_prefix'] = notification._properties['object_name_prefix']
                    output_dict['event_types'] = notification.event_types
                except KeyError:
                    pass

                notifications_dict['notifications'].append(output_dict)

        pprint(notifications_dict)

        return 0

    def command_create(self):
        """
        Create a new Pub/Sub notification based on the JSON
        in the supplied --config-file for configurations
        where 'id' key has a value of null
        """
        config_path = self.get_config_path()

        if not os.path.exists(config_path):
            _logger.error(f'File {config_path} was not found.')
            return 1

        with open(config_path) as f:
            config_data = json.load(f)

        new_notifications_list = filter(
            lambda x: x['id'] is None,
            config_data['notifications']
        )
        for new_notification in new_notifications_list:

            notification = self.create_notification(new_notification)

            new_notification['id'] = notification.notification_id

            # Output created notification config data
            _logger.info('Notification created:')
            pprint(new_notification)

        # replace existing json file with updated notification config
        self.update_notification_config(config_data, config_path)

        return 0

    def command_delete(self):
        """
        Delete the Pub/Sub notification with --id and --bucket
        """
        if self.gcp_env.project not in SUPPORTED_PROJECT_CONFIGS.keys():
            _logger.error(f'Project config not supported {self.gcp_env.project}')
            return 1

        if not self.args.bucket and not self.args.id:
            _logger.error("--bucket and --id required for delete.")
            return 1

        # Get notification
        client = storage.Client()
        bucket = client.get_bucket(self.args.bucket)
        target = bucket.get_notification(self.args.id, client)

        if self.gcp_env.project != target.topic_project:
            _logger.error("Notification project and specified project do not match.")
            return 1

        # Delete the notification
        try:
            target.delete(client=client)

        except NotFound:
            _logger.error(f"Notification ID {self.args.id} not found.")
            return 1

        _logger.info(f"Notification id {self.args.id} has been deleted.")

        _logger.info("Removing notification from config...")
        self.delete_notification_from_config()

        return 0

    def create_notification(self, new_notification: dict):
        """
        Calls google api to create new pub/sub notification
        :param new_notification: dict from config file
        :return:
        """
        bucket_name = new_notification['bucket']

        if self.gcp_env.project != new_notification['topic_project']:
            _logger.error(f'Notification project mismatch.')
            sys.exit(1)

        # create notification
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        notification = bucket.notification(
            topic_name=new_notification['topic_name'],
            topic_project=new_notification['topic_project'],
            custom_attributes=None,
            event_types=new_notification['event_types'],
            blob_name_prefix=new_notification['object_name_prefix'],
            payload_format=new_notification['payload_format'],
            notification_id=None,
        )

        notification.create(client=client)
        return notification

    def get_config_path(self):
        if self.args.config_file:
            config_name = self.args.config_file
        else:
            config_name = SUPPORTED_PROJECT_CONFIGS[self.gcp_env.project]

        return os.path.join(CONFIG_ROOT, config_name)

    @staticmethod
    def update_notification_config(data, config_path):
        with open(config_path, "w") as f:
            f.write(json.dumps(data, indent=2))

    def delete_notification_from_config(self):
        config_path = self.get_config_path()

        with open(config_path, "r") as f:
            config_data = json.load(f)

        notification_iter = filter(
            lambda x: x['id'] == str(self.args.id),
            config_data['notifications']
        )

        for notification in notification_iter:
            config_data['notifications'].remove(notification)

        with open(config_path, "w") as f:
            f.write(json.dumps(config_data, indent=2))

        _logger.info("Notification removed from config.")


def add_additional_arguments(parser):
    parser.add_argument("--command", default=None, required=True, choices=['list', 'create', 'delete'], type=str)
    parser.add_argument("--bucket", default=None, required=False, help="GCS bucket to target", type=str)
    parser.add_argument("--config-file", default=None, required=False,
                        help="path to json notification config file", type=str)
    parser.add_argument("--id", default=None, required=False, help="notification ID to target", type=int)


def run():
    return cli_run(tool_cmd, tool_desc, PubSubNotificationManager, add_additional_arguments)
