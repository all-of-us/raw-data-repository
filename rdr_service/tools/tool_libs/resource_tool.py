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
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_update, bq_genomic_set_member_update, \
    bq_genomic_job_run_update, bq_genomic_gc_validation_metrics_update, bq_genomic_file_processed_update, \
    bq_genomic_manifest_file_update, bq_genomic_manifest_feedback_update
from rdr_service.dao.bq_workbench_dao import bq_workspace_update, bq_workspace_user_update, \
    bq_institutional_affiliations_update, bq_researcher_update
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.bq_questionnaires import BQPDRConsentPII, BQPDRTheBasics, BQPDRLifestyle, BQPDROverallHealth, \
    BQPDREHRConsentPII, BQPDRDVEHRSharing, BQPDRCOPEMay, BQPDRCOPENov, BQPDRCOPEDec, BQPDRCOPEFeb
from rdr_service.model.participant import Participant
from rdr_service.offline.bigquery_sync import batch_rebuild_participants_task
from rdr_service.resource.generators.participant import rebuild_participant_summary_resource
from rdr_service.resource.generators.genomics import genomic_set_update, genomic_set_member_update, \
    genomic_job_run_update, genomic_gc_validation_metrics_update, genomic_file_processed_update, \
    genomic_manifest_file_update, genomic_manifest_feedback_update
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "resource"
tool_desc = "Tools for updating resource records in RDR"


GENOMIC_DB_TABLES = ('genomic_set', 'genomic_set_member', 'genomic_job_run', 'genomic_gc_validation_metrics',
                     'genomic_file_processed', 'genomic_manifest_file', 'genomic_manifest_feedback')

RESEARCH_WORKBENCH_TABLES = ('workspace', 'workspace_user', 'researcher', 'institutional_affiliations')


class ParticipantResourceClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject, pid_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        :param pid_list: list of integer participant ids, if --from-file was specified
        """
        self.args = args
        self.gcp_env = gcp_env
        self.pid_list = pid_list


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
                BQPDRCOPEMay,
                BQPDRCOPENov,
                BQPDRCOPEDec,
                BQPDRCOPEFeb
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
                                                w_dao=w_dao, w_session=w_session, project_id=self.gcp_env.project)
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
        pids = self.pid_list

        if not pids and not self.args.pid and not self.args.all_pids:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild Participant Summaries for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))

        if pids:
            _logger.info('  PIDs File             : {0}'.format(clr.fmt(self.args.from_file)))
            _logger.info('  Total PIDs            : {0}'.format(clr.fmt(len(pids))))
        elif self.args.all_pids:
            dao = ResourceDataDao()
            with dao.session() as session:
                results = session.query(Participant.participantId).all()
                pids = [p.participantId for p in results]
                _logger.info('  Rebuild All PIDs      : {0}'.format(clr.fmt('Yes')))
                _logger.info('  Total PIDs            : {0}'.format(clr.fmt(len(pids))))
        elif self.args.pid:
            _logger.info('  PID                   : {0}'.format(clr.fmt(self.args.pid)))

        _logger.info('=' * 90)
        _logger.info('')

        if pids and len(pids):
            return self.update_many_pids(pids)

        if self.args.pid:
            if self.update_single_pid(self.args.pid) == 0:
                _logger.info(f'Participant {self.args.pid} updated.')
            else:
                _logger.error(f'Participant ID {self.args.pid} not found.')

        return 1


class GenomicResourceClass(object):

    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        :param id_list: list of integer ids from a genomic table, if --genomic-table and --from-file were specified
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list

    def update_single_id(self, table, _id):

        try:
            if table == 'genomic_set':
                bq_genomic_set_update(_id, project_id=self.gcp_env.project)
                genomic_set_update(_id)
            elif table == 'genomic_set_member':
                bq_genomic_set_member_update(_id, project_id=self.gcp_env.project)
                genomic_set_member_update(_id)
            elif table == 'genomic_job_run':
                bq_genomic_job_run_update(_id, project_id=self.gcp_env.project)
                genomic_job_run_update(_id)
            elif table == 'genomic_file_processed':
                bq_genomic_file_processed_update(_id, project_id=self.gcp_env.project)
                genomic_file_processed_update(_id)
            elif table == 'genomic_manifest_file':
                bq_genomic_manifest_file_update(_id, project_id=self.gcp_env.project)
                genomic_manifest_file_update(_id)
            elif table == 'genomic_manifest_feedback':
                bq_genomic_manifest_feedback_update(_id, project_id=self.gcp_env.project)
                genomic_manifest_feedback_update(_id)
            elif table == 'genomic_gc_validation_metrics':
                bq_genomic_gc_validation_metrics_update(_id, project_id=self.gcp_env.project)
                genomic_gc_validation_metrics_update(_id)
        except NotFound:
            return 1
        return 0

    def update_batch(self, table, _ids):

        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        count = 0
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        if not self.args.debug:
            print_progress_bar(
                count, len(_ids), prefix="{0}/{1}:".format(count, len(_ids)), suffix="complete"
            )

        for batch in chunks(_ids, 250):
            if self.gcp_env.project == 'localhost':
                for _id in batch:
                    self.update_single_id(table, _id)
            else:
                payload = {'table': table, 'ids': batch}
                task.execute('rebuild_genomic_table_records_task', payload=payload, in_seconds=15,
                             queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            count += len(batch)
            if not self.args.debug:
                print_progress_bar(
                    count, len(_ids), prefix="{0}/{1}:".format(count, len(_ids)), suffix="complete"
                )

    def update_many_ids(self, table, _ids):
        if not _ids:
            return 1

        _logger.info(f'Processing batch for table {table}...')
        if self.args.batch:
            self.update_batch(table, _ids)
            _logger.info(f'Processing {table} batch complete.')
            return 0

        total_ids = len(_ids)
        count = 0
        errors = 0

        for _id in _ids:
            count += 1

            if self.update_single_id(table, _id) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'{table} ID {_id} not found.')

            if not self.args.debug:
                print_progress_bar(
                    count, total_ids, prefix="{0}/{1}:".format(count, total_ids), suffix="complete"
                )

        if errors > 0:
            _logger.warning(f'\n\nThere were {errors} IDs not found during processing.')

        return 0



    def run(self):
        clr = self.gcp_env.terminal_colors

        if not self.args.id and not self.args.all_ids and not self.args.all_tables and not self.id_list:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild Genomic Records for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Genomic Table         : {0}'.format(clr.fmt(self.args.table)))

        if self.args.all_ids or self.args.all_tables:
            dao = ResourceDataDao()
            _logger.info('  Rebuild All Records   : {0}'.format(clr.fmt('Yes')))
            if self.args.all_tables:
                tables = [{'name': t, 'ids': list()} for t in GENOMIC_DB_TABLES]
            else:
                tables = [{'name': self.args.table, 'ids': list()}]
            _logger.info('  Rebuild Table(s)      : {0}'.format(
                clr.fmt(', '.join([t['name'] for t in tables]))))

            for table in tables:
                with dao.session() as session:
                    results = session.execute(f'select id from {table["name"]}')
                    table['ids'] = [r.id for r in results]
                    _logger.info('  Total Records         : {0} = {1}'.
                                 format(clr.fmt(table["name"]), clr.fmt(len(table['ids']))))

            for table in tables:
                self.update_many_ids(table['name'], table['ids'])

        elif self.args.id:
            _logger.info('  Record ID             : {0}'.format(clr.fmt(self.args.id)))
            self.update_single_id(self.args.table, self.args.id)
        elif self.id_list:
            _logger.info('  Total Records         : {0}'.format(clr.fmt(len(self.id_list))))
            if len(self.id_list):
                self.update_many_ids(self.args.table, self.id_list)

        return 1


class EHRReceiptClass(object):
    """  """

    def __init__(self, args, gcp_env: GCPEnvConfigObject, pid_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.pid_list = pid_list

    def update_batch(self, records):
        """
        Submit batches of pids to Cloud Tasks for rebuild.
        """
        import gc
        batch_size = 100

        total_rows = len(records)
        batch_total = int(math.ceil(float(total_rows) / float(batch_size)))
        _logger.info('Calculated {0} tasks from {1} ehr records with a batch size of {2}.'.
                     format(batch_total, total_rows, batch_size))

        count = 0
        batch_count = 0
        batch = list()
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        from rdr_service.participant_enums import EhrStatus

        # queue up a batch of participant ids and send them to be rebuilt.
        for row in records:

            ehr_status = EhrStatus(row.ehr_status)

            batch.append({
                'pid': row.participant_id,
                'patch': {
                    'ehr_status': str(ehr_status),
                    'ehr_status_id': int(ehr_status),
                    'ehr_receipt': row.ehr_receipt_time.isoformat() if row.ehr_receipt_time else None,
                    'ehr_update': row.ehr_update_time.isoformat() if row.ehr_update_time else None
                }
            })

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

    def run(self):

        clr = self.gcp_env.terminal_colors

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nUpdate Participant Summary Records with RDR EHR receipt data:',
                             clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))

        pids = self.pid_list if self.pid_list else []
        dao = ResourceDataDao()

        with dao.session() as session:

            sql = 'select participant_id, ehr_status, ehr_receipt_time, ehr_update_time from participant_summary'
            cursor = session.execute(sql)
            if len(pids):
                _logger.info('  PIDs File             : {0}'.format(clr.fmt(self.args.from_file)))
                _logger.info('  Total PIDs            : {0}'.format(clr.fmt(len(pids))))
                records = [row for row in cursor if row.participant_id in pids]
            else:
                records = [row for row in cursor]

            _logger.info('  Total Records         : {0}'.format(clr.fmt(len(records))))
            _logger.info('  Batch Size            : 100')
            if len(records):
                self.update_batch(records)

        return 0


class ResearchWorkbenchResourceClass(object):

    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        :param id_list: list of integer ids from a research workbench table, if --table and --from-file were specified.
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list

    def update_single_id(self, table, _id):

        try:
            if table == 'workspace':
                bq_workspace_update(_id, project_id=self.gcp_env.project)
            elif table == 'workspace_user':
                bq_workspace_user_update(_id, project_id=self.gcp_env.project)
            elif table == 'institutional_affiliations':
                bq_institutional_affiliations_update(_id, project_id=self.gcp_env.project)
            elif table == 'researcher':
                bq_researcher_update(_id, project_id=self.gcp_env.project)
        except NotFound:
            return 1
        return 0

    def update_batch(self, table, _ids):

        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        count = 0
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        if not self.args.debug:
            print_progress_bar(
                count, len(_ids), prefix="{0}/{1}:".format(count, len(_ids)), suffix="complete"
            )

        for batch in chunks(_ids, 250):
            if self.gcp_env.project == 'localhost':
                for _id in batch:
                    self.update_single_id(table, _id)
            else:
                payload = {'table': table, 'ids': batch}
                task.execute('rebuild_research_workbench_table_records_task', payload=payload, in_seconds=15,
                             queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            count += len(batch)
            if not self.args.debug:
                print_progress_bar(
                    count, len(_ids), prefix="{0}/{1}:".format(count, len(_ids)), suffix="complete"
                )

    def update_many_ids(self, table, _ids):
        if not _ids:
            _logger.warning(f'No records found in table {table}, skipping.')
            return 1

        _logger.info(f'Processing batch for table {table}...')
        if self.args.batch:
            self.update_batch(table, _ids)
            _logger.info(f'Processing {table} batch complete.')
            return 0

        total_ids = len(_ids)
        count = 0
        errors = 0

        for _id in _ids:
            count += 1

            if self.update_single_id(table, _id) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'{table} ID {_id} not found.')

            if not self.args.debug:
                print_progress_bar(
                    count, total_ids, prefix="{0}/{1}:".format(count, total_ids), suffix="complete"
                )

        if errors > 0:
            _logger.warning(f'\n\nThere were {errors} IDs not found during processing.')

        return 0



    def run(self):
        clr = self.gcp_env.terminal_colors

        if not self.args.id and not self.args.all_ids and not self.args.all_tables and not self.id_list:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild Research Workbench Records for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Database Table        : {0}'.format(clr.fmt(self.args.table)))

        table_map = {
            'workspace': 'workbench_workspace_snapshot',
            'workspace_user': 'workbench_workspace_user',
            'researcher': 'workbench_researcher',
            'institutional_affiliations': 'workbench_institutional_affiliations'
        }

        if self.args.all_ids or self.args.all_tables:
            dao = ResourceDataDao()
            _logger.info('  Rebuild All Records   : {0}'.format(clr.fmt('Yes')))
            if self.args.all_tables:
                tables = [{'name': t, 'ids': list()} for t in RESEARCH_WORKBENCH_TABLES]
            else:
                tables = [{'name': self.args.table, 'ids': list()}]
            _logger.info('  Rebuild Table(s)      : {0}'.format(
                clr.fmt(', '.join([t['name'] for t in tables]))))

            for table in tables:
                with dao.session() as session:
                    results = session.execute(f'select id from {table_map[table["name"]]}')
                    table['ids'] = [r.id for r in results]
                    _logger.info('  Total Records         : {0} = {1}'.
                                 format(clr.fmt(table["name"]), clr.fmt(len(table['ids']))))

            for table in tables:
                self.update_many_ids(table['name'], table['ids'])

        elif self.args.id:
            _logger.info('  Record ID             : {0}'.format(clr.fmt(self.args.id)))
            self.update_single_id(self.args.table, self.args.id)
        elif self.id_list:
            _logger.info('  Total Records         : {0}'.format(clr.fmt(len(self.id_list))))
            if len(self.id_list):
                self.update_many_ids(self.args.table, self.id_list)

        return 1


def get_id_list(fname):
    """
    Shared helper routine for tool classes that allow input from a file of integer ids (participant ids or
    id values from a specific genomic table).
    :param fname:  The filename passed with the --from-file argument
    :return: A list of integers, or None on missing/empty fname
    """
    filename = os.path.expanduser(fname)
    if not os.path.exists(filename):
        _logger.error(f"File '{fname}' not found.")
        return None

    # read ids from file.
    ids = open(os.path.expanduser(fname)).readlines()
    # convert ids from a list of strings to a list of integers.
    ids = [int(i) for i in ids if i.strip()]
    return ids if len(ids) else None


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

    # The "dest" add_subparsers() argument specifies the property name in the args object where the
    # sub-parser used in the command line will be stored. IE: if args.resource == 'participant'...
    subparser = parser.add_subparsers(title='resource types', dest='resource',
                                      help='specific resource type to work with')

    # Common individual arguments that may be used in multiple subparsers.  The Help text and Choices can
    # be overridden by calling update_argument() after the subparser has been created.
    id_parser = argparse.ArgumentParser(add_help=False)
    id_parser.add_argument("--id", help="rebuild single genomic table id", type=int, default=None)

    all_ids_parser = argparse.ArgumentParser(add_help=False)
    all_ids_parser.add_argument("--all-ids", help="rebuild all records", default=False, action="store_true")

    from_file_parser = argparse.ArgumentParser(add_help=False)
    from_file_parser.add_argument("--from-file", help="rebuild resource ids from a file with a list of ids",
                                metavar='FILE', type=str, default=None)

    table_parser = argparse.ArgumentParser(add_help=False)
    table_parser.add_argument("--table", help="research workbench db table name to rebuild from", type=str,
                              metavar='TABLE')

    all_tables_parser = argparse.ArgumentParser(add_help=False)
    all_tables_parser.add_argument("--all-tables", help="rebuild all records from all tables", default=False,
                            action="store_true")

    batch_parser = argparse.ArgumentParser(add_help=False)
    batch_parser.add_argument("--batch", help="submit resource ids in batches to Cloud Tasks", default=False,
                                action="store_true")
    # End common subparser arguments.

    def update_argument(p, dest, help=None):  # pylint: disable=redefined-builtin
        """
        Update sub-parser argument description and choices.
        :param dest: Destination property where argument value is stored.  IE: 'file_name' == args.file_name.
        """
        if not p or not dest:
            raise ValueError('Arguments must include a sub-parser and dest string.')
        for a in p._actions:
            if a.dest == dest:
                a.help = help

    def argument_conflict(args_, ids_, choices=()):
        """ Check if common arguments conflict """
        if args_.table and args_.all_tables:
            _logger.error("Arguments 'table' and 'all-tables' conflict.")
            return True
        elif args_.all_tables and args_.from_file:
            _logger.error("Argument 'from-file' cannot be used with 'all-tables', only with 'table'")
            return True
        elif args_.id and ids_:
            _logger.error("Argument 'from-file' cannot be used if a single 'id' was also specified")
            return True
        elif ids_ and not args_.table:
            _logger.error("Argument 'from-file' was provided without a specified 'table' ")
            return True
        if args_.table and args_.table not in choices:
            _logger.error(f"Argument 'table' value '{args_.table}' is invalid, possible values are:\n   {choices}.")
            return True
        return False

    # Rebuild participant resources
    rebuild_parser = subparser.add_parser(
        "participant",
        parents=[from_file_parser, batch_parser])
    rebuild_parser.add_argument("--pid", help="rebuild single participant id", type=int, default=None)  # noqa
    rebuild_parser.add_argument("--all-pids", help="rebuild all participants", default=False,
                                action="store_true")  # noqa
    update_argument(rebuild_parser, dest='from_file',
                    help="rebuild participant ids from a file with a list of pids")

    # Rebuild genomic resources.
    genomic_parser = subparser.add_parser(
        "genomic",
        parents=[id_parser, all_ids_parser, table_parser, all_tables_parser, from_file_parser, batch_parser])
    update_argument(genomic_parser, 'table', help="genomic db table name to rebuild from")
    genomic_parser.epilog = f'Possible TABLE Values: {{{",".join(GENOMIC_DB_TABLES)}}}.'

    # Rebuild EHR receipt resources.
    ehr_parser = subparser.add_parser('ehr-receipt', parents=[batch_parser, from_file_parser])
    ehr_parser.add_argument("--ehr", help="Submit batch to Cloud Tasks", default=False,
                                action="store_true")  # noqa
    update_argument(ehr_parser, dest='from_file',
                    help="rebuild EHR info for specific participant ids read from a file with a list of pids")

    # Rebuild Research Workbench resources.
    rw_parser = subparser.add_parser(
        "research-workbench",
        parents=[batch_parser, id_parser, all_ids_parser, table_parser, all_tables_parser, from_file_parser])
    update_argument(rw_parser, 'table', help="research workbench db table name to rebuild from")
    rw_parser.epilog = f'Possible TABLE Values: {{{",".join(RESEARCH_WORKBENCH_TABLES)}}}.'

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        ids = None
        if hasattr(args, 'from_file') and args.from_file:
            ids = get_id_list(args.from_file)

        # Rebuild participant resources
        if args.resource == 'participant':
            process = ParticipantResourceClass(args, gcp_env, ids)
            exit_code = process.run()

        # Rebuild genomic resources.
        elif args.resource == 'genomic':
            if argument_conflict(args, ids, choices=GENOMIC_DB_TABLES):
                sys.exit(1)

            process = GenomicResourceClass(args, gcp_env, ids)
            exit_code = process.run()

        # Rebuild EHR receipt resources.
        elif args.resource == 'ehr-receipt':
            process = EHRReceiptClass(args, gcp_env, ids)
            exit_code = process.run()

        # Rebuild Research Workbench resources.
        elif args.resource == 'research-workbench':
            if argument_conflict(args, ids, choices=RESEARCH_WORKBENCH_TABLES):
                sys.exit(1)

            process = ResearchWorkbenchResourceClass(args, gcp_env, ids)
            exit_code = process.run()

        else:
            _logger.info('Please select an option to run. For help use "[resource] --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
