#! /bin/env python
#
# Copy Consent EHR files to HPO buckets.
#
# Replaces older ehr_upload_for_organization.sh script.
#

import argparse
import logging
import os
import random
import shutil
import sys
from datetime import datetime
from zipfile import ZipFile

import MySQLdb
import pytz

from rdr_service.dao import database_factory
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.services.gcp_utils import gcp_cp, gcp_format_sql_instance, gcp_make_auth_header
from rdr_service.services.system_utils import make_api_request, print_progress_bar, setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

from rdr_service.offline.sync_consent_files import get_org_data_map, build_participant_query

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "sync-consents"
tool_desc = "manually sync consent files to sites"

SOURCE_BUCKET = {
    "vibrent": "gs://ptc-uploads-all-of-us-rdr-prod/Participant/P{p_id}/*{file_ext}",
    "careevolution": "gs://ce-uploads-all-of-us-rdr-prod/Participant/P{p_id}/*{file_ext}"
}

HPO_REPORT_CONFIG_GCS_PATH = "gs://all-of-us-rdr-sequestered-config-test/hpo-report-config-mixin.json"
DEST_BUCKET = "gs://{bucket_name}/Participant/{org_external_id}/{site_name}/P{p_id}/"

TEMP_CONSENTS_PATH = "./temp_consents"


class SyncConsentClass(object):
    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env

        self.file_filter = "pdf"

    @staticmethod
    def _get_count_sql(participant_sql):
        return "select count(1) {0}".format(participant_sql[participant_sql.find("from"):])

    def _get_files_updated_in_range(self, source_bucket, date_limit, p_id):
        """
        Uses the date limit to filter cloud storage files by the date
        :param source_bucket:
        :param date_limit:
        :param p_id:
        :return:
        """
        directory = source_bucket.split('/')[2]
        timezone = pytz.timezone('Etc/Greenwich')
        date_limit_obj = timezone.localize(datetime.strptime(date_limit, '%Y-%m-%d'))
        prefix = f'Participant/P{p_id}'
        try:
            provider = GoogleCloudStorageProvider()
            files = list(provider.list(directory, prefix=prefix))
            file_list = [
                f.name.replace(f'{prefix}', f'gs://{directory}/{prefix}')
                for f in files if f.updated > date_limit_obj
                and f.name.endswith(self.file_filter)
            ]
            return file_list
        except FileNotFoundError:
            return False

    def _format_debug_out(self, p_id, src, dest):
        _logger.debug(" Participant: {0}".format(p_id))
        _logger.debug("    src: {0}".format(src))
        _logger.debug("   dest: {0}".format(dest))

    @staticmethod
    def _add_path_to_zip(zip_file, directory_path):
        for current_path, _, files in os.walk(directory_path):
            # os.walk will recurse into sub_directories, so we only need to handle the files in the current directory
            for file in files:
                file_path = os.path.join(current_path, file)
                archive_name = file_path[len(directory_path):]
                zip_file.write(file_path, arcname=archive_name)

    @staticmethod
    def _directories_in(directory_path):
        with os.scandir(directory_path) as objects:
            return [directory_object for directory_object in objects if directory_object.is_dir()]

    def _copy(self, source, destination, participant_id):
        print('\ncoping from', source, 'to', destination)
        if not self.args.dry_run:
            if self.args.zip_files:
                # gcp_cp doesn't create local directories when they don't exist
                os.makedirs(destination, exist_ok=True)
            gcp_cp(source, destination, flags="-m")

        self._format_debug_out(participant_id, source, destination)

    def run(self):
        """
    Main program process
    :return: Exit code value
    """
        sites = get_org_data_map()

        _logger.info("retrieving db configuration...")
        headers = gcp_make_auth_header()
        resp_code, resp_data = make_api_request(
            "{0}.appspot.com".format(self.gcp_env.project), "/rdr/v1/Config/db_config", headers=headers
        )
        if resp_code != 200:
            _logger.error(resp_data)
            _logger.error("failed to retrieve config, aborting.")
            return 1

        passwd = resp_data["rdr_db_password"]
        if not passwd:
            _logger.error("failed to retrieve database user password from config.")
            return 1

        # connect a sql proxy to the current project
        _logger.info("starting google sql proxy...")
        port = random.randint(10000, 65535)
        instances = gcp_format_sql_instance(self.gcp_env.project, port=port)
        proxy_pid = self.gcp_env.activate_sql_proxy(instance=instances, port=port)
        if not proxy_pid:
            _logger.error("activating google sql proxy failed.")
            return 1

        try:
            # _logger.info("retrieving participant information...")
            # # get record count
            # query_args = {}
            # if self.args.date_limit:
            #     # TODO: Add execption handling for incorrect date format
            #     query_args['start_date'] = self.args.date_limit
            # if self.args.end_date:
            #     # TODO: Add execption handling for incorrect date format
            #     query_args['end_date'] = self.args.end_date
            # org_ids = None
            # if not self.args.all_va:
            #     if self.args.org_id:
            #         org_ids = [self.args.org_id]
            #     else:
            #         raise Exception("Org id required for consent sync")
            # else:
            #     query_args['all_va'] = True
            #
            # participant_sql, params = build_participant_query(org_ids, **query_args)
            # count_sql = self._get_count_sql(participant_sql)
            # with database_factory.make_server_cursor_database().session() as session:
            #     total_participants = session.execute(count_sql, params).scalar()
            #
            #     _logger.info("transferring files to destinations...")
            #     count = 0
            #     for rec in session.execute(participant_sql, params):
            #         if not self.args.debug:
            #             print_progress_bar(
            #                 count, total_participants, prefix="{0}/{1}:".format(count, total_participants),
            #                 suffix="complete"
            #             )
            #
            #         p_id = rec[0]
            #         origin_id = rec[1]
            #         site = rec[2]
            #         org_id = rec[3]
            #         if self.args.destination_bucket is not None:
            #             # override destination bucket lookup (the lookup table is incomplete)
            #             bucket = self.args.destination_bucket
            #         else:
            #             site_info = sites.get(rec[3])
            #             if not site_info:
            #                 _logger.warning("\nsite info not found for [{0}].".format(rec[2]))
            #                 count += 1
            #                 continue
            #             bucket = site_info.get("bucket_name")
            #         if not bucket:
            #             _logger.warning("\nno bucket name found for [{0}].".format(rec[2]))
            #             count += 1
            #             continue
            #
            #         # Copy all files, not just PDFs
            #         if self.args.all_files:
            #             self.file_filter = ""
            #
            #         src_bucket = SOURCE_BUCKET.get(origin_id, SOURCE_BUCKET[
            #             next(iter(SOURCE_BUCKET))
            #         ]).format(p_id=p_id, file_ext=self.file_filter)
            #
            #         if self.args.zip_files:
            #             destination_pattern =\
            #                 TEMP_CONSENTS_PATH + '/{bucket_name}/{org_external_id}/{site_name}/P{p_id}/'
            #         else:
            #             destination_pattern = DEST_BUCKET
            #         destination = destination_pattern.format(
            #             bucket_name=bucket,
            #             org_external_id=org_id,
            #             site_name=site if site else "no-site-assigned",
            #             p_id=p_id,
            #         )
            #         if self.args.date_limit:
            #             # only copy files newer than date limit
            #             files_in_range = self._get_files_updated_in_range(
            #                 date_limit=self.args.date_limit,
            #                 source_bucket=src_bucket, p_id=p_id)
            #             if not files_in_range or len(files_in_range) == 0:
            #                 _logger.info(f'No files in bucket updated after {self.args.date_limit}')
            #             for f in files_in_range:
            #                 self._copy(f, destination, p_id)
            #         else:
            #             self._copy(src_bucket, destination, p_id)
            #
            #         count += 1
            #
            # # print progressbar one more time to show completed.
            # if total_participants > 0 and not self.args.debug:
            #     print_progress_bar(
            #         count, total_participants, prefix="{0}/{1}:".format(count, total_participants), suffix="complete"
            #     )

            count = 1
            if self.args.zip_files and count > 0:
                _logger.info("zipping and uploading consent files...")
                for bucket_dir in self._directories_in(TEMP_CONSENTS_PATH):
                    for org_dir in self._directories_in(bucket_dir):
                        for site_dir in self._directories_in(org_dir):
                            zip_file_name = os.path.join(org_dir.path, site_dir.name + '.zip')
                            with ZipFile(zip_file_name, 'w') as zip_file:
                                self._add_path_to_zip(zip_file, site_dir.path)

                            destination = "gs://{bucket_name}/Participant/{org_external_id}/".format(
                                bucket_name='aou179',  # todo: should be orgs bucket name
                                org_external_id=org_dir.name
                            )
                            if not self.args.dry_run:
                                _logger.debug("Uploading file '{zip_file}' to '{destination}'".format(
                                    zip_file=zip_file_name,
                                    destination=destination
                                ))
                                gcp_cp(zip_file_name, destination, flags="-m")
                shutil.rmtree(TEMP_CONSENTS_PATH)

        except MySQLdb.OperationalError as e:
            _logger.error("failed to connect to {0} mysql instance. [{1}]".format(self.gcp_env.project, e))

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--org-id", help="organization id", default=None)  # noqa
    parser.add_argument(
        "--destination-bucket", default=None, help="Override the destination bucket lookup for the given organization."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not copy files, only print the list of files that would be copied"
    )
    parser.add_argument(
        "--zip-files", action="store_true", help="Zip the consent files by site rather than uploading them individually"
    )
    parser.add_argument(
        "--all-va", action="store_true", help="Zip consents for all VA organizations"
    )

    parser.add_argument(
        "--date-limit", help="Limit consents to sync to those created after the date", default=None)  # noqa

    parser.add_argument(
        "--end-date", help="Limit consents to sync to those created before the date", default=None)  # noqa

    parser.add_argument(
        "--all-files", help="Transfer all file types, default is only PDF.",
        default=False, action="store_true")  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = SyncConsentClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
