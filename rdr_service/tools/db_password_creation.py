#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Update database passwords for key operational user ids
#
import argparse
import json
import logging
import os
import random
import secrets
import string
import time
from datetime import datetime, timezone
from typing import List

import sys

from python_easy_json import JSONObject

from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask, Queue
from aou_cloud.tools.config_editor import DS_DB_CONFIG_KEY, ConfigDeployClass, ConfigEditClass
from services.mixins.rdr_mysql_connection_mixin import RDRMySQLConnectionMixin
from tools import GCPProcessContext, GCPEnvConfigObject


_logger = logging.getLogger("pdr")


tool_cmd = "update-db-passwords"
tool_desc = "update database passwords for key operational user ids"
tool_cat = "Data Tools"


class UpdateDatabasePasswordsTool(RDRMySQLConnectionMixin):
    """
    Automation to reset database passwords for key user accounts
    """
    db_config: JSONObject = None
    service_account: str = None
    task_service: GCPCloudTask = None
    queues: List[Queue] = None

    config_edit_service: ConfigEditClass = None
    config_deploy_service: ConfigDeployClass = None

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def _pause_queues_and_wait(self, queues: List[Queue]):
        """
        waits for queues to finish and then pauses
        :return:
        """
        _logger.info('Pausing cloud task queues and waiting for running tasks to complete...')

        for queue in queues:
            self.task_service.pause_queue(queue)

        start_ts = datetime.now(timezone.utc)
        success = False
        while (datetime.now(timezone.utc) - start_ts).seconds < 180:
            is_empty = True
            for queue in queues:
                queue_stats = self.task_service.fetch_queue_stats(queue)
                if queue_stats.concurrentDispatchesCount != 0:
                    is_empty = False
            if is_empty is True:
                success = True
                break
            time.sleep(3.0)
            sys.stdout.write('.')

        if success is True:
            sys.stdout.write(' Done.')
        else:
            _logger.warning('Not all running tasks completed before timeout')

        return success

    def pause_task_queues(self):
        """ Pause all running cloud task queues, ignore any that are paused. """
        self.task_service = GCPCloudTask(self.gcp_env.project)
        queues = self.task_service.fetch_all_queues()
        # Only grab the queues that are currently processing tasks
        self.queues = [q for q in queues if q.state == 'RUNNING']

        self._pause_queues_and_wait(queues)

    def resume_task_queues(self):
        """ Resume the cloud tasks queues we paused """
        for queue in self.queues:
            self.task_service.resume_queue(queue)

    @staticmethod
    def generate_password(length=20, use_uppercase=True, use_digits=True, use_punctuation=True):
        """Generates a random password with specified criteria."""

        # Try to only use special characters that are allowed on every database platform.
        special = r"~!#^*<>()-+="

        char_sets = [string.ascii_lowercase]
        char_pool = string.ascii_lowercase
        if use_uppercase:
            char_pool += string.ascii_uppercase
            char_sets.append(string.ascii_uppercase)
        if use_digits:
            char_pool += string.digits
            char_sets.append(string.digits)
        if use_punctuation:
            char_pool += special
            char_sets.append(special)

        if not any([use_uppercase, use_digits, use_punctuation]):
            raise ValueError("At least one character set must be selected.")

        # Retry until we get a password with at least one character from every set
        while True:
            password = ''.join(secrets.choice(char_pool) for _ in range(length-1))
            # Verify there is at least one character in the password from each character set
            bad_password = False
            for char_set in char_sets:
                found = False
                for char in char_set:
                    if char in password:
                        found = True
                        break
                if found is False:
                    bad_password = True

            if bad_password is False:
                break

        # PostgreSQL requires passwords to begin only with a letter, no numbers or special characters.
        char = random.choice(string.ascii_uppercase + string.ascii_uppercase)
        password = char + password

        return password


    def change_mysql_password(self, user_cfg, new_password: str):
        """
        Change a user password on a mysql instance
        :param user_cfg: User config from db_config
        :param new_password: New password for the user
        """
        # Capaxcu5MMRdn8sS
        mysql_conn = self.connect_mysql_instance(self.gcp_env.project, 'rdr', replica=False)
        sql = f"ALTER USER '{user_cfg.user}'@'%' IDENTIFIED BY '{new_password}';"
        cursor = mysql_conn.cursor()
        cursor.execute(sql)
        cursor.close()
        return True

    def run(self):

        self.gcp_env.override_project('aou-pdr-data-dev')

        # See if we should change a password for a specific user
        if self.args.target == 'user':
            self.change_postgres_user_password(self.args.username)
            return 0

        # Change passwords for all users listed in DB config
        config_service_args = JSONObject({
            'base-config': False,
            'key': 'db_config',
            'bucket': os.environ.get('APP_CONFIG_BUCKET', None),
            'from_file': ''
        })
        self.config_edit_service = ConfigEditClass(config_service_args, self.gcp_env)
        self.config_deploy_service = ConfigDeployClass(config_service_args, self.gcp_env)

        # Read the most recent config from the bucket
        self.db_config = JSONObject(self.config_deploy_service.get_bucket_config())

        self.pause_task_queues()

        _logger.info(f'Updating all db config passwords')

        all_instances = self.db_config.instances
        for user_cfg in self.db_config.users:

            new_password = self.generate_password()
            # Find only primary database instances to change the user password on.
            instances = list(filter(lambda i: i.pool in user_cfg.instance_pools and not i.is_readonly, all_instances))
            for inst_cfg in instances:

                _logger.info(f"Updating user '{user_cfg.user}' on {inst_cfg.connection_name} ({inst_cfg.platform})")
                _logger.warning(f'   user: {user_cfg.user}, passwords: prev: {user_cfg.password}, new: {new_password}')

                if inst_cfg.platform == 'postgresql':
                    if self.change_postgres_password(user_cfg, inst_cfg, new_password) is True:
                        user_cfg.password = new_password
                    else:
                        break
                elif inst_cfg.platform == 'mysql':
                    if self.change_mysql_password(user_cfg, new_password) is True:
                        user_cfg.password = new_password
                    else:
                        break
                else:
                    _logger.warning(f'Unknown database instance platform ({inst_cfg.platform}) ')

        # Update config, save it to the config bucket and then push config to firestore.
        updated_config = self.db_config.to_dict()
        config_json = json.dumps(updated_config, indent=2)
        self.config_edit_service.save_config_to_bucket(self.gcp_env.project, DS_DB_CONFIG_KEY, config_json)
        self.config_deploy_service.write_firestore_config()

        self.resume_task_queues()

        return 0


def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)

    # Create a subparser for multiple tool sub commands
    subparser = parser.add_subparsers(help='change operational or specific user password', dest='target')

    username_parser = argparse.ArgumentParser(add_help=False)
    username_parser.add_argument('-u', "--username", help="database username on pdr postgres instance",
                                 type=str, default=None, required=True)

    subparser.add_parser('ops')
    subparser.add_parser('user', parents=[username_parser])
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args) as gcp_env:
        process = UpdateDatabasePasswordsTool(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
