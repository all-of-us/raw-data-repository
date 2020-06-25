#! /bin/env python
#
# Copy Consent EHR files to HPO buckets.
#
# Replaces older ehr_upload_for_organization.sh script.
#
import argparse
import logging
import os
import sys
from datetime import datetime

import MySQLdb
import pytz

from rdr_service.dao import database_factory
from rdr_service.offline.sync_consent_files import get_org_data_map, build_participant_query, \
    DEFAULT_GOOGLE_GROUP, get_consent_destination, archive_and_upload_consents, copy_file
from rdr_service.services.system_utils import print_progress_bar, setup_logging, setup_i18n
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "sync-consents"
tool_desc = "manually sync consent files to sites"

SOURCE_BUCKET = {
    "vibrent": "gs://ptc-uploads-all-of-us-rdr-prod/Participant/P{p_id}/*{file_ext}",
    "careevolution": "gs://ce-uploads-all-of-us-rdr-prod/Participant/P{p_id}/*{file_ext}"
}


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

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        proxy_pid = self.gcp_env.activate_sql_proxy()
        if not proxy_pid:
            return 1

        filter_pids = None
        if self.args.pid_file:
            filter_pids = open(self.args.pid_file).read().strip().split('\n')
            filter_pids = [int(x) for x in filter_pids]

        org_buckets = get_org_data_map()

        try:
            _logger.info("retrieving participant information...")
            # get record count
            query_args = {}
            if self.args.date_limit:
                # TODO: Add execption handling for incorrect date format
                query_args['start_date'] = self.args.date_limit
            if self.args.end_date:
                # TODO: Add execption handling for incorrect date format
                query_args['end_date'] = self.args.end_date
            org_ids = None
            if not self.args.all_va:
                if self.args.org_id:
                    org_ids = [self.args.org_id]
                else:
                    raise Exception("Org id required for consent sync")
            else:
                query_args['all_va'] = True

            participant_sql, params = build_participant_query(org_ids, **query_args)
            count_sql = self._get_count_sql(participant_sql)
            with database_factory.make_server_cursor_database().session() as session:
                total_participants = session.execute(count_sql, params).scalar() if not filter_pids \
                                            else len(filter_pids)

                _logger.info("transferring files to destinations...")
                count = 0
                for rec in session.execute(participant_sql, params):
                    if rec[0] not in filter_pids:
                        continue
                    if not self.args.debug:
                        print_progress_bar(
                            count, total_participants, prefix="{0}/{1}:".format(count, total_participants),
                            suffix="complete"
                        )

                    p_id = rec[0]
                    origin_id = rec[1]
                    site = rec[2]
                    org_id = rec[3]
                    if self.args.destination_bucket is not None:
                        # override destination bucket lookup (the lookup table is incomplete)
                        bucket = self.args.destination_bucket
                    elif self.args.all_va:
                        bucket = 'aou179'
                    else:
                        bucket = org_buckets.get(org_id, None)
                    if not bucket:
                        _logger.warning("\nno bucket name found for [{0}].".format(site))
                        count += 1
                        continue

                    # Copy all files, not just PDFs
                    if self.args.all_files:
                        self.file_filter = ""

                    src_bucket = SOURCE_BUCKET.get(origin_id, SOURCE_BUCKET[
                        next(iter(SOURCE_BUCKET))
                    ]).format(p_id=p_id, file_ext=self.file_filter)

                    destination = get_consent_destination(
                        add_protocol=True,
                        zipping=self.args.zip_files,
                        bucket_name=bucket,
                        org_external_id=org_id,
                        site_name=site if site else DEFAULT_GOOGLE_GROUP,
                        p_id=p_id
                    )
                    if self.args.date_limit:
                        # only copy files newer than date limit
                        files_in_range = self._get_files_updated_in_range(
                            date_limit=self.args.date_limit,
                            source_bucket=src_bucket, p_id=p_id)
                        if not files_in_range or len(files_in_range) == 0:
                            _logger.info(f'No files in bucket updated after {self.args.date_limit}')
                        for f in files_in_range:
                            copy_file(f, destination, p_id, dry_run=self.args.dry_run, zip_files=self.args.zip_files)
                    else:
                        copy_file(src_bucket, destination, p_id,
                                  dry_run=self.args.dry_run, zip_files=self.args.zip_files)

                    count += 1

            # print progressbar one more time to show completed.
            if total_participants > 0 and not self.args.debug:
                print_progress_bar(
                    count, total_participants, prefix="{0}/{1}:".format(count, total_participants), suffix="complete"
                )

            if self.args.zip_files and count > 0:
                archive_and_upload_consents(dry_run=self.args.dry_run)

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
    parser.add_argument('--pid-file', help="File with list of pids to sync", default=None, type=str)  # noqa

    args = parser.parse_args()

    if args.pid_file and not os.path.exists(args.pid_file):
        _logger.error(f'File "{args.pid_file}" does not exist.')
        return 1

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = SyncConsentClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
