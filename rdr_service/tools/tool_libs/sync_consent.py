from datetime import datetime
import MySQLdb
import logging
import pytz

from rdr_service.config import CONSENT_SYNC_BUCKETS
from rdr_service.offline.sync_consent_files import build_participant_query, \
    DEFAULT_GOOGLE_GROUP, get_consent_destination, archive_and_upload_consents, copy_file
from rdr_service.services.system_utils import print_progress_bar
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "sync-consents"
tool_desc = "manually sync consent files to sites"

SOURCE_BUCKET = {
    "vibrent": "gs://ptc-uploads-all-of-us-rdr-prod/Participant/P{p_id}/*{file_ext}",
    "careevolution": "gs://ce-uploads-all-of-us-rdr-prod/Participant/P{p_id}/*{file_ext}"
}


class SyncConsentClass(ToolBase):
    def __init__(self, args, gcp_env):
        super(SyncConsentClass, self).__init__(args, gcp_env)
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
        super(SyncConsentClass, self).run()

        filter_pids = None
        if self.args.pid_file:
            filter_pids = open(self.args.pid_file).read().strip().split('\n')
            filter_pids = [int(x) for x in filter_pids]

        server_config = self.get_server_config()
        org_buckets = server_config[CONSENT_SYNC_BUCKETS]

        try:
            logger.info("retrieving participant information...")
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

            with self.get_session() as session:
                participant_query = build_participant_query(session, org_ids, **query_args)
                total_participants = participant_query.count() if not filter_pids else len(filter_pids)

                logger.info("transferring files to destinations...")
                count = 0
                for rec in participant_query:
                    if filter_pids and rec[0] not in filter_pids:
                        continue
                    if not self.args.debug:
                        print_progress_bar(
                            count, total_participants, prefix="{0}/{1}:".format(count, total_participants),
                            suffix="complete"
                        )

                    site = rec.googleGroup
                    org_id = rec.externalId
                    if self.args.destination_bucket is not None:
                        # override destination bucket lookup (the lookup table is incomplete)
                        bucket = self.args.destination_bucket
                    elif self.args.all_va:
                        bucket = 'aou179'
                    else:
                        bucket = org_buckets.get(org_id, None)
                    if not bucket:
                        logger.warning("\nno bucket name found for [{0}].".format(site))
                        count += 1
                        continue

                    # Copy all files, not just PDFs
                    if self.args.all_files:
                        self.file_filter = ""

                    src_bucket = SOURCE_BUCKET.get(rec.participantOrigin, SOURCE_BUCKET[
                        next(iter(SOURCE_BUCKET))
                    ]).format(p_id=rec.participantId, file_ext=self.file_filter)

                    destination = get_consent_destination(
                        add_protocol=True,
                        zipping=self.args.zip_files,
                        bucket_name=bucket,
                        org_external_id=org_id,
                        site_name=site if site else DEFAULT_GOOGLE_GROUP,
                        p_id=rec.participantId
                    )
                    if self.args.date_limit:
                        # only copy files newer than date limit
                        files_in_range = self._get_files_updated_in_range(
                            date_limit=self.args.date_limit,
                            source_bucket=src_bucket, p_id=rec.participantId)
                        if not files_in_range or len(files_in_range) == 0:
                            logger.info(f'No files in bucket updated after {self.args.date_limit}')
                        for f in files_in_range:
                            copy_file(
                                f,
                                destination,
                                rec.participantId,
                                dry_run=self.args.dry_run,
                                zip_files=self.args.zip_files
                            )
                    else:
                        copy_file(src_bucket, destination, rec.participantId,
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
            logger.error("failed to connect to {0} mysql instance. [{1}]".format(self.gcp_env.project, e))

        return 0


def add_additional_arguments(parser):
    parser.add_argument("--org-id", help="organization id", default=None)
    parser.add_argument("--destination-bucket", default=None,
                        help="Override the destination bucket lookup for the given organization.")
    parser.add_argument("--zip-files", action="store_true",
                        help="Zip the consent files by site rather than uploading them individually")
    parser.add_argument("--all-va", action="store_true", help="Zip consents for all VA organizations")
    parser.add_argument("--date-limit", help="Limit consents to sync to those created after the date", default=None)
    parser.add_argument("--end-date", help="Limit consents to sync to those created before the date", default=None)
    parser.add_argument("--all-files", help="Transfer all file types, default is only PDF.",
                        default=False, action="store_true")
    parser.add_argument('--pid-file', help="File with list of pids to sync", default=None, type=str)

    # todo: add functionality for specific PIDs


def run():
    cli_run(tool_cmd, tool_desc, SyncConsentClass, add_additional_arguments)
