#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import datetime
import logging
import os
import sys

from typing import List

from sqlalchemy import or_
from sqlalchemy.orm import Session

# from rdr_service.offline.retention_eligible_import import import_retention_eligible_metrics_file
from rdr_service.dao.retention_eligible_metrics_dao import RetentionEligibleMetricsDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline.retention_eligible_import import _create_retention_eligible_metrics_obj_from_row, \
    _supplement_with_rdr_calculations
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.storage import GoogleCloudStorageCSVReader
from rdr_service.services.system_utils import list_chunks


_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "retention-metrics"
tool_desc = "perform retention metrics data import/remediations"

def has_flag_mismatch(val1, val2):
    """
    Compare flag field values for presence,  and matching boolean status
    """
    # Check for presence mismatches
    if val1 is None and val2 is None:
        return False
    if (val1 is None and val2 is not None) or (val1 is not None and val2 is None):
        return True
    # Check for
    if bool(val1) == bool(val2):
        return False
    else:
        return True


def has_date_mismatch(ts1, ts2):
    """
    Compare date component of two datetime timestamps for mismatches
    """
    if ts1 is None and ts2 is None:
        return False
    if (ts1 is None and ts2 is not None) or (ts1 is not None and ts2 is None):
        return True
    if ts1.date() == ts2.date():
        return False
    else:
        return True

class RetentionBaseClass(object):

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.gcp_env.activate_sql_proxy()

    def get_retention_db_record(self, session, pid):
        """ Retrieves the retention_eligible_metrics record from the database for the specified participant """
        return session.query(
            RetentionEligibleMetrics
        ).filter(RetentionEligibleMetrics.participantId == pid).first()

    def has_participant_summary(self, session, pid):
        """
        Check if RDR has a participant_summary for the specified participant
        (since PTSC includes REGISTERED/unconsented pids in their retention metrics file drop)
        """
        ps_rec = session.query(
            ParticipantSummary
        ).filter(ParticipantSummary.participantId == pid).first()
        return True if ps_rec else False

    @staticmethod
    def get_int_ids_from_file(file_path):
        """ Ingest a file of integer ids, such as participant_id or table record ids """
        with open(os.path.expanduser(file_path)) as id_list:
            ids = id_list.readlines()
            # convert ids from a list of strings to a list of integers.
            return [int(i) for i in ids if i.strip()]

class RetentionRecalcClass(RetentionBaseClass):
    """
    A tool class to force an update to a retention_eligible_metrics record and the RDR calculated fields
    """

    @classmethod
    def check_for_rdr_mismatches(cls, rem_rec: RetentionEligibleMetrics):
        mismatches = []

        if has_flag_mismatch(rem_rec.retentionEligible, rem_rec.rdr_retention_eligible):
            _logger.error(f'P{rem_rec.participantId}\tPTSC/RDR retentionEligible mismatch, ' +
                          f'{rem_rec.retentionEligible}/{rem_rec.rdr_retention_eligible}')
            mismatches.append('retentionEligible')
        if has_date_mismatch(rem_rec.retentionEligibleTime, rem_rec.rdr_retention_eligible_time):
            _logger.error(f'P{rem_rec.participantId}\tPTSC/RDR retentionEligibleTime mismatch, ' +
                          f'{rem_rec.retentionEligibleTime}/{rem_rec.rdr_retention_eligible_time}')
            mismatches.append('retentionEligibleTime')
        if has_flag_mismatch(rem_rec.activelyRetained, rem_rec.rdr_is_actively_retained):
            _logger.error(f'P{rem_rec.participantId}\tPTSC/RDR ActivelyRetained mismatch, ' +
                          f'{rem_rec.activelyRetained}/{rem_rec.rdr_is_actively_retained}')
            mismatches.append('activelyRetained')
        if has_flag_mismatch(rem_rec.passivelyRetained, rem_rec.rdr_is_passively_retained):
            _logger.error(f'P{rem_rec.participantId}\tPTSC/RDR ActivelyRetained mismatch, ' +
                          f'{rem_rec.passivelyRetained}/{rem_rec.rdr_is_passively_retained}')
            mismatches.append('passivelyRetained')
        if has_date_mismatch(rem_rec.lastActiveRetentionActivityTime,
                                  rem_rec.rdr_last_retention_activity_time):
            _logger.error(f'P{rem_rec.participantId}\tPTSC/RDR lastActiveRetentionActivityTime mismatch, ' +
                          f'{rem_rec.lastActiveRetentionActivityTime}/{rem_rec.rdr_last_retention_activity_time}')
            mismatches.append('lastActiveRetentionActivityTime')

        return mismatches

    @classmethod
    def recalculate_rdr_retention(cls, session, rem_rec: RetentionEligibleMetrics):
        _supplement_with_rdr_calculations(metrics_data=rem_rec, session=session)
        return cls.check_for_rdr_mismatches(rem_rec)

    @staticmethod
    def fetch_mismatches_from_participant_summary(session: Session) -> list:
        """
        Fetches mismatches between the Participant Summary & Retention Eligible Metrics tables.
        """
        mismatches = (
            session.query(ParticipantSummary, RetentionEligibleMetrics)
            .filter(
                ParticipantSummary.participantId
                == RetentionEligibleMetrics.participantId,
                or_(
                    ParticipantSummary.retentionEligibleStatus
                    != RetentionEligibleMetrics.retentionEligibleStatus,
                    ParticipantSummary.retentionEligibleTime
                    != RetentionEligibleMetrics.retentionEligibleTime,
                    ParticipantSummary.retentionType
                    != RetentionEligibleMetrics.retentionType,
                    ParticipantSummary.lastActiveRetentionActivityTime
                    != RetentionEligibleMetrics.lastActiveRetentionActivityTime,
                ),
            )
            .order_by(ParticipantSummary.participantId)
            .all()
        )
        return mismatches

    def handle_mismatches(self, session: Session):
        """
        Updates the retention metrics data in the participant summary table to match the data that is sent to us by PTSC
        """
        mismatches = self.fetch_mismatches_from_participant_summary(session)
        for participant, retention_metric in mismatches:
            _logger.info(f"Updating retention metrics in participant summary table for P{participant.participantId}")
            participant.retentionEligibleStatus = retention_metric.retentionEligibleStatus
            participant.retentionEligibleTime = retention_metric.retentionEligibleTime
            participant.retentionType = retention_metric.retentionType
            participant.lastActiveRetentionActivityTime = (
                retention_metric.lastActiveRetentionActivityTime
            )
            session.add(participant)

        try:
            session.commit()
            _logger.info('Successfully updated retention metric for these participants')
        except CommitException as e:
            session.rollback()
            _logger.error(f'Failed to commit retention metric updates due to {e.response}')

    def run(self):

        if self.args.id:
            participant_id_list = [int(i) for i in self.args.id.split(',')]
        elif self.args.from_file:
            participant_id_list = self.get_int_ids_from_file(self.args.from_file)
        elif self.args.fix_mismatches:
            # Ayaz is confused on what session argument to add to this function
            self.handle_mismatches()
            return 0


        dao = RetentionEligibleMetricsDao()
        with dao.session() as session:
            count = 0
            for pid_list in list_chunks(participant_id_list, chunk_size=500):
                for pid in pid_list:
                    _logger.info(f'Recalculating P{pid}...')
                    # Error messages will be emitted if there are mismatches after recalculation
                    self.recalculate_rdr_retention(session, self.get_retention_db_record(session, pid))
                    count += 1

                session.commit()
                _logger.info(f'-----Processed {count} of {len(participant_id_list)} participants')
        return 0


class CommitException(Exception):
    """ an exception when making a commit to the DB using a session
    """
    def __init__(self, response):
        self.response = response


class RetentionQCClass(RetentionBaseClass):
    """
    A tool class to compare retention_eligible_metrics file entries with a PTSC CSV file, and to compare
    RDR calculated values to the PTSC values
    """

    @staticmethod
    def has_ptsc_mismatches(pid, file_obj, db_obj):
        # This identifies mismatches between a PTSC CSV file and the last values recorded from PTSC in the
        # retention_eligible_metrics table.  E.g., to help identify potential failed/incomplete ingestions
        if (has_flag_mismatch(file_obj.retentionEligible, db_obj.retentionEligible)
            or has_flag_mismatch(file_obj.activelyRetained, db_obj.activelyRetained)
            or has_flag_mismatch(file_obj.passivelyRetained, db_obj.passivelyRetained)
            or has_date_mismatch(file_obj.retentionEligibleTime, db_obj.retentionEligibleTime)
            or has_date_mismatch(file_obj.lastActiveRetentionActivityTime,
                                 db_obj.lastActiveRetentionActivityTime)):
            _logger.error(f'P{pid} file and database PTSC fields do not match')

    def check_for_all_mismatches(self, pid, file_obj, db_obj):
        """
        Verify consistency of PTSC values in the provided CSV file vs. the retention_eligible_metrics table,
        and the PTSC vs. RDR calculated retention data in the retention_eligible_metrics record for a participant
        """

        if self.has_ptsc_mismatches(pid, file_obj, db_obj):
            # Don't bother continuing with RDR vs. PTSC checks if PTSC data isn't consistent
            mismatches = ['ptsc_values_mismatch']
        else:
            mismatches = RetentionRecalcClass.check_for_rdr_mismatches(db_obj)

        return mismatches

    @staticmethod
    def output_mismatch_list(header_line: str, mismatch_list: List[int]):
        """
        At end of QC tool run, will output lists of pids that had mismatches specified by the header_line
        """
        if len(mismatch_list):
            print(header_line)
            for pid_list in list_chunks(mismatch_list, chunk_size=20):
                print('\t' + ','.join([str(pid) for pid in pid_list]))
            print('\n')

    def run(self):

        # Copy bucket file to local temp file.
        # TODO:  Use something different than GoogleCloudStorageCSVReader() when running as a local tool?
        # That class requires a GAE_ENV environment variable that starts with 'standard' only to use a bucket file for
        # doing data comparison.  Must include that in a run config.  Alternative is to download to local CSV
        if self.args.bucket_file:
            _logger.info(f"Reading gs://{self.args.bucket_file}.")
            csv_reader = GoogleCloudStorageCSVReader(self.args.bucket_file)
            file_date = self.args.bucket_file.split('/')[-1][:10]
        elif self.args.csv:
            # NOTE!! Still expected that a local CSV file will follow the naming convention used for bucket files,
            # so that the prefix of the base filename is a YYYY-MM-DD date string
            _logger.info(f"Reading local CSV file {self.args.csv}")
            csv_file = open(self.args.csv)
            csv_reader = csv.DictReader(csv_file)
            file_date = self.args.csv.split('/')[-1][:10]

        year, month, day = file_date.split('-')

        dao = RetentionEligibleMetricsDao()
        ptsc_mismatches = []
        recalculated = []
        retention_eligible_mismatches = []
        active_retention_mismatches = []
        passive_retention_mismatches = []
        eligibility_date_mismatches = []
        last_activity_date_mismatches = []
        with dao.session() as session:
            count = 0
            for row in csv_reader:
                # Make an upload date value from date string taken from the file name
                upload_date = datetime.datetime(int(year), int(month), int(day))
                file_obj = _create_retention_eligible_metrics_obj_from_row(row, upload_date)
                pid = int(file_obj.participantId) if file_obj.participantId else 0

                # Ignore expected diffs because PTSC includes REGISTERED/unconsented participants in the file drop
                if not self.has_participant_summary(session, pid):
                    continue

                db_obj = self.get_retention_db_record(session, pid)
                if not db_obj:
                    _logger.warning(f'No DB entry for P{pid}')
                    continue

                mismatches = self.check_for_all_mismatches(pid, file_obj, db_obj)
                if 'ptsc_values_mismatch' in mismatches:
                    ptsc_mismatches.append(pid)

                elif len(mismatches):
                    # Try recalculating the RDR values to resolve deltas
                    recalculated.append(pid)
                    mismatches = RetentionRecalcClass.recalculate_rdr_retention(session, db_obj)
                    if 'retentionEligible' in mismatches:
                        retention_eligible_mismatches.append(pid)
                    if 'activelyRetained' in mismatches:
                        active_retention_mismatches.append(pid)
                    if 'passivelyRetained' in mismatches:
                        passive_retention_mismatches.append(pid)
                    if 'retentionEligibleTime' in mismatches:
                        eligibility_date_mismatches.append(pid)
                    if 'lastActiveRetentionActivityTime' in mismatches:
                        last_activity_date_mismatches.append(pid)

                count += 1
                if count % 500 == 0:
                    _logger.info(f'Processed {count} pids, ', f'PTSC data mismatches: {len(ptsc_mismatches)}, ' +
                                 f'active mismatches: {len(active_retention_mismatches)}, ' +
                                 f'passive mismatches: {len(passive_retention_mismatches)}, ' +
                                 f'eligible_mismatches: {len(retention_eligible_mismatches)}, ' +
                                 f'eligibility date mismatches: {len(eligibility_date_mismatches)}, ' +
                                 f'last active retention date mismatches: {len(last_activity_date_mismatches)}, ')
        if csv_file:
            csv_file.close()

        if recalculated:
            print(f'Recalculated {len(recalculated)} pids...')
        for mismatches in (('PTSC data mismatches:', ptsc_mismatches),
                           ('Retention eligible mismatches:', retention_eligible_mismatches),
                           ('Active retention_mismatches: ', active_retention_mismatches),
                           ('Passive retention mismatches: ', passive_retention_mismatches),
                           ('Eligibility date mismatches: ', eligibility_date_mismatches),
                           ('Last Active retention activity date mismatches: ', last_activity_date_mismatches)):
            self.output_mismatch_list(mismatches[0], mismatches[1])

        return 0


class RetentionLoadClass(object):
    """
    Manual ingestion of PTSC retention metrics file - NEEDS UPDATING
    """
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

        """"
        # TODO: Glob the bucket and get the latest file or get file date from command line arg.
        task_payload = {
            'bucket': '...',  # removed hard coded bucket.
            'file_path': '...',  # removed hard coded path.
            'upload_date': self.args.file_upload_time
        }
        import_retention_eligible_metrics_file(task_payload)
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

    load_parser = subparser.add_parser("load")
    load_parser.add_argument(
        "--file-upload-time",
        help="bucket file upload time (as string) of metrics file",
        type=str,
    )

    qc_parser = subparser.add_parser("qc")
    qc_parser.add_argument("--bucket-file", help="bucket_file_path", type=str)
    qc_parser.add_argument("--csv", help="local CSV file path", type=str)

    recalc_parser = subparser.add_parser("recalc")
    recalc_parser.add_argument(
        "--from-file", help="file of ids to recalculate", default="", type=str
    )  # noqa
    recalc_parser.add_argument(
        "--id",
        help="comma-separated list of ids to recalculate",
        type=str,
        default=None,
    )
    recalc_parser.add_argument(
        "--fix-mismatches",
        help="fix mismatches in retention between participant summary and retention eligible metrics",
        action="store_true"
    )
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        if args.action == 'load':
            # process = RetentionLoadClass(args, gcp_env)
            _logger.error('Manual load is not operational')
        elif args.action == 'qc':
            process = RetentionQCClass(args, gcp_env)
        elif args.action == 'recalc':
            process = RetentionRecalcClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
