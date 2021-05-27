import json

from google.cloud import storage

from rdr_service.config import PUBSUB_NOTIFICATION_BUCKETS
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'pubsub-manager'
tool_desc = 'Manage GCloud Pub/Sub Notifications'


class PubSubNotificationManager(ToolBase):
    def run(self):

        super(PubSubNotificationManager, self).run()

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
        bucket_list = [self.args.bucket] if self.args.bucket else PUBSUB_NOTIFICATION_BUCKETS

        # bucket_list = ["prod-genomics-baylor"]
        notifications_dict = {"notifications": []}

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

                output_dict['bucket'] = bucket_name
                output_dict['id'] = notification.notification_id
                output_dict['topic_name'] = notification.topic_name
                output_dict['topic_project'] = notification.topic_project
                output_dict['payload_format'] = notification.payload_format
                output_dict['object_name_prefix'] = notification._properties['object_name_prefix']
                output_dict['event_types'] = notification.event_types

                notifications_dict['notifications'].append(output_dict)

        print(json.dumps(notifications_dict))

        return 0

    def command_create(self):
        """
        Create a new Pub/Sub notification based on the JSON
        in the supplied --config-file
        """
        return 0

    def command_delete(self):
        """
        Delete the Pub/Sub notification based on the JSON
        in the supplied --config-file
        """
        return 0


def add_additional_arguments(parser):
    parser.add_argument("--command", default=None, required=True, choices=['list', 'create', 'delete'], type=str)
    parser.add_argument("--bucket", default=None, required=False, help="GCS bucket to target", type=str)
    parser.add_argument("--config-file", default=None, required=False,
                        help="path to json notification config file", type=str)
    parser.add_argument("--id", default=None, required=False, help="notification ID to target", type=int)


def run():
    return cli_run(tool_cmd, tool_desc, PubSubNotificationManager, add_additional_arguments)
