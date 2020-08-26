#! /bin/env python
#
# PDR data tools.
#

import argparse
# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import math
import os
import sys

from werkzeug.exceptions import NotFound

from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import rebuild_bq_participant
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.bq_questionnaires import BQPDRConsentPII, BQPDRTheBasics, BQPDRLifestyle, BQPDROverallHealth, \
    BQPDREHRConsentPII, BQPDRDVEHRSharing, BQPDRCOPEMay
from rdr_service.model.participant import Participant
from rdr_service.offline.bigquery_sync import batch_rebuild_participants_task
from rdr_service.resource.generators.participant import rebuild_participant_summary_resource
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "resource"
tool_desc = "Tools for updating resource records in RDR"


class ResourceClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env


    def update_single_pid(self, pid):
        """
        Update a single pid
        :param pid: participant id
        :return: 0 if successful otherwise 1
        """
        try:
            rebuild_bq_participant(pid, project_id=self.gcp_env.project)
            rebuild_participant_summary_resource(pid)

            mod_bqgen = BQPDRQuestionnaireResponseGenerator()

            # Generate participant questionnaire module response data
            modules = (
                BQPDRConsentPII,
                BQPDRTheBasics,
                BQPDRLifestyle,
                BQPDROverallHealth,
                BQPDREHRConsentPII,
                BQPDRDVEHRSharing,
                BQPDRCOPEMay
            )
            for module in modules:
                mod = module()
                table, mod_bqrs = mod_bqgen.make_bqrecord(pid, mod.get_schema().get_module_name())
                if not table:
                    continue

                w_dao = BigQuerySyncDao()
                with w_dao.session() as w_session:
                    for mod_bqr in mod_bqrs:
                        mod_bqgen.save_bqrecord(mod_bqr.questionnaire_response_id, mod_bqr, bqtable=table,
                                                w_dao=w_dao, w_session=w_session)
        except NotFound:
            return 1
        return 0

    def update_batch(self, pids):
        """
        Submit batches of pids to Cloud Tasks for rebuild.
        """
        import gc
        if self.gcp_env.project == 'all-of-us-rdr-prod':
            batch_size = 100
        else:
            batch_size = 25

        total_rows = len(pids)
        batch_total = int(math.ceil(float(total_rows) / float(batch_size)))
        if self.args.batch:
            batch_total = math.ceil(total_rows / batch_size)
        _logger.info('Calculated {0} tasks from {1} pids with a batch size of {2}.'.
                     format(batch_total, total_rows, batch_size))

        count = 0
        batch_count = 0
        batch = list()
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        # queue up a batch of participant ids and send them to be rebuilt.
        for pid in pids:

            batch.append({'pid': pid})
            count += 1

            if count == batch_size:
                payload = {'batch': batch}

                if self.gcp_env.project == 'localhost':
                    batch_rebuild_participants_task(payload)
                else:
                    task.execute('rebuild_participants_task', payload=payload, in_seconds=15,
                                        queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

                batch_count += 1
                # reset for next batch
                batch = list()
                count = 0
                if not self.args.debug:
                    print_progress_bar(
                        batch_count, batch_total, prefix="{0}/{1}:".format(batch_count, batch_total), suffix="complete"
                    )

                # Collect the garbage after so long to prevent hitting open file limit.
                if batch_count % 250 == 0:
                    gc.collect()

        # send last batch if needed.
        if count:
            payload = {'batch': batch}
            batch_count += 1
            if self.gcp_env.project == 'localhost':
                batch_rebuild_participants_task(payload)
            else:
                task.execute('rebuild_participants_task', payload=payload, in_seconds=15,
                                    queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            if not self.args.debug:
                print_progress_bar(
                    batch_count, batch_total, prefix="{0}/{1}:".format(batch_count, batch_total), suffix="complete"
                )

        logging.info(f'Submitted {batch_count} tasks.')

        return 0

    def update_many_pids(self, pids):
        """
        Update many pids from a file.
        :return:
        """
        if not pids:
            return 1

        if self.args.batch or self.args.all_pids:
            return self.update_batch(pids)

        total_pids = len(pids)
        count = 0
        errors = 0

        for pid in pids:
            count += 1

            if self.update_single_pid(pid) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'PID {pid} not found.')

            if not self.args.debug:
                print_progress_bar(
                    count, total_pids, prefix="{0}/{1}:".format(count, total_pids), suffix="complete"
                )

        if errors > 0:
            _logger.warning(f'\n\nThere were {errors} PIDs not found during processing.')

        return 0


    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        clr = self.gcp_env.terminal_colors
        pids = None

        if not self.args.from_file and not self.args.pid and not self.args.all_pids:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild Participant Summaries for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))

        if self.args.from_file:
            filename = os.path.expanduser(self.args.from_file)
            if not os.path.exists(filename):
                _logger.error(f"File '{self.args.from_file}' not found.")
                return 1

            # read pids from file.
            pids = open(os.path.expanduser(self.args.from_file)).readlines()
            # convert pids from a list of strings to a list of integers.
            pids = [int(i) for i in pids if i.strip()]
            _logger.info('  PIDs File             : {0}'.format(clr.fmt(self.args.from_file)))
            _logger.info('  Total PIDs            : {0}'.format(clr.fmt(len(pids))))
        elif self.args.all_pids:
            dao = ResourceDataDao()
            with dao.session() as session:
                results = session.query(Participant.participantId).all()
                pids = [p.participantId for p in results]
                _logger.info('  Rebuild All PIDs      : {0}'.format(clr.fmt('Yes')))
                _logger.info('  Total PIDs            : {0}'.format(clr.fmt(len(pids))))
        else:
            _logger.info('  PID                   : {0}'.format(clr.fmt(self.args.pid)))

        _logger.info('=' * 90)
        _logger.info('')

        if self.args.from_file or self.args.all_pids:
            return self.update_many_pids(pids)

        if self.args.pid:
            if self.update_single_pid(self.args.pid) == 0:
                _logger.info(f'Participant {self.args.pid} updated.')
            else:
                _logger.error(f'Participant ID {self.args.pid} not found.')

        return 1


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

    subparser = parser.add_subparsers(help='pdr tools')

    # Rebuild PDR participants
    rebuild_parser = subparser.add_parser("rebuild-pids")
    rebuild_parser.add_argument("--pid", help="rebuild single participant id", type=int, default=None)  # noqa
    rebuild_parser.add_argument("--all-pids", help="rebuild all participants", default=False,
                                action="store_true")  # noqa
    rebuild_parser.add_argument("--from-file", help="rebuild participant ids from a file with a list of pids",
                                default=None)  # noqa
    rebuild_parser.add_argument("--batch", help="Submit pids in batch to Cloud Tasks", default=False,
                                action="store_true")  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        if hasattr(args, 'pid') and hasattr(args, 'from_file'):
            process = ResourceClass(args, gcp_env)
            exit_code = process.run()
        else:
            _logger.info('Please select an option to run. For help use "pdr-tool --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
