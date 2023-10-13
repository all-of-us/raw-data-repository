#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import datetime
import logging
import sys

from rdr_service.dao.retention_eligible_metrics_dao import RetentionEligibleMetricsDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline.retention_eligible_import import _create_retention_eligible_metrics_obj_from_row, \
    _supplement_with_rdr_calculations
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.storage import GoogleCloudStorageCSVReader

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "ptsc-retention"
tool_desc = "load ptsc retention eligible metrics into rdr"

class RetentionBaseClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.gcp_env.activate_sql_proxy()

    def get_db_entry(self, session, pid):
        return session.query(
            RetentionEligibleMetrics
        ).filter(RetentionEligibleMetrics.participantId == pid).first()

class RetentionRecalcClass(RetentionBaseClass):
    def run(self):

        if self.args.id:
            participant_id_list = [int(i) for i in self.args.id.split(',')]
        elif self.args.from_file:
            participant_id_list = self.get_int_ids_from_file(self.args.from_file)

        dao = RetentionEligibleMetricsDao()
        with dao.session() as session:
            for pid in participant_id_list:
                rem_rec = self.get_db_entry(session, pid)
                _supplement_with_rdr_calculations(metrics_data=rem_rec, session=session)
                if rem_rec.retentionEligible != rem_rec.rdr_retention_eligible:
                    pass
class RetentionQCClass(RetentionBaseClass):

    def check_all_mismatches(self, pid, file_obj, db_obj):
        mismatches = []
        if (file_obj.retentionEligible != db_obj.retentionEligible or
            file_obj.retentionEligibleStatus != db_obj.retentionEligibleStatus or
            file_obj.activelyRetained != db_obj.activelyRetained or
            file_obj.passivelyRetained != db_obj.passivelyRetained or
            file_obj.lastActiveRetentionActivityTime != db_obj.lastActiveRetentionActivityTime or
            file_obj.retentionEligibleTime != db_obj.retentionEligibleTime):
            _logger.error(f'P{pid} file and database PTSC fields do not match')
            return
        if file_obj.retentionEligible != db_obj.rdr_retention_eligible:
            mismatches.append('retentionEligible')
        if file_obj.activelyRetained != db_obj.rdr_is_actively_retained:
            mismatches.append('activelyRetained')
        if file_obj.passivelyRetained != db_obj.rdr_is_passively_retained:
            mismatches.append('passivelyRetained')
        if ((file_obj.lastActiveRetentionActivityTime and not db_obj.rdr_last_retention_activity_time) or
            (db_obj.rdr_last_retention_activity_time and not file_obj.lastActiveRetentionActivityTime)):
            mismatches.append('lastActiveRetentionActivityTime or RDR timestamp missing')
        elif (file_obj.lastActiveRetentionActivityTime and db_obj.rdr_last_retention_activity_time and
              file_obj.lastActiveRetentionActivityTime.date() !=
              db_obj.rdr_last_retention_activity_time.date()):
            mismatches.append('lastActiveRetentionActivityTime date mismatch')
        if ((file_obj.retentionEligibleTime and not db_obj.rdr_retention_eligible_time) or
            (db_obj.rdr_retention_eligible_time and not file_obj.retentionEligibleTime)):
            mismatches.append('retentionEligibleTime or RDR timestamp missing')
        elif (file_obj.retentionEligibleTime and db_obj.rdr_retention_eligible_time and
              file_obj.retentionEligibleTime.date() != db_obj.rdr_retention_eligible_time.date()):
            mismatches.append('retentionEligibleTime date mismatch')

    def run(self):

        # Copy bucket file to local temp file.
        _logger.info(f"Reading gs://{self.args.bucket_file}.")
        file_date = self.args.bucket_file.split('/')[1][:10]
        year, month, day = file_date.split('-')
        csv_reader = GoogleCloudStorageCSVReader(self.args.bucket_file)
        dao = RetentionEligibleMetricsDao()
        recalculated = []
        active_retention_mismatches = []
        passive_retention_mismatches = []
        with dao.session() as session:
            count = 0
            for row in csv_reader:
                upload_date = datetime.datetime(int(year), int(month), int(day))
                file_obj = _create_retention_eligible_metrics_obj_from_row(row, upload_date)
                if file_obj and file_obj.participantId:
                    pid = int(file_obj.participantId)
                    has_participant_summary = session.query(
                        ParticipantSummary
                    ).filter(ParticipantSummary.participantId == pid).first()
                    if not has_participant_summary:
                        # Ignore, known mismatch condition w/PTSC file data
                        continue
                    db_obj = self.get_db_entry(session, pid)
                    if not db_obj:
                        _logger.warning(f'No DB entry for P{pid}')
                        continue

                    if (file_obj.activelyRetained != db_obj.rdr_is_actively_retained or
                        file_obj.passivelyRetained != db_obj.rdr_is_passively_retained):
                        recalculated.append(pid)
                        _supplement_with_rdr_calculations(metrics_data=file_obj, session=session)
                        if file_obj.activelyRetained != file_obj.rdr_is_actively_retained:
                            active_retention_mismatches.append(pid)
                        if file_obj.passivelyRetained != file_obj.rdr_is_passively_retained:
                            passive_retention_mismatches.append(pid)

                count += 1
                if count % 500 == 0:
                    _logger.info(f'Processed {count} pids, active mismatches: {len(active_retention_mismatches)}, ' +
                                 f'passive mismatches: {len(passive_retention_mismatches)}')

class PTSCRetentionMetricsClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        self.gcp_env.activate_sql_proxy()

        # TODO: Glob the bucket and get the latest file or get file date from command line arg.
        """
        task_payload = {
            'bucket': '...',  # removed hard coded bucket.
            'file_path': '...',  # removed hard coded path.
            'upload_date': '2021-07-26 11:24:21'
        }
        """

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    subparser = parser.add_subparsers(title='action', dest='action',
                                      help='action to perform, such as qc')

    qc_parser = subparser.add_parser("qc")
    qc_parser.add_argument("--bucket-file", help="bucket_file_path", type=str)

    recalc_parser = subparser.add_parser("recalc")
    recalc_parser.add_argument('--from-file', help='file of ids to recalculate', default='', type=str)  # noqa
    recalc_parser.add_argument("--id", help="comma-separated list of ids to recalculate", type=str, default=None)
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        if args.action == 'qc':
            process = RetentionQCClass(args, gcp_env)
        elif args.action == 'recalc':
            process = RetentionRecalcClass(args, gcp_env)
        else:
            process = PTSCRetentionMetricsClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
