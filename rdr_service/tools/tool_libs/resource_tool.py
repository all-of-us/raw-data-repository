#! /bin/env python
#
# PDR data tools.
#

import argparse
# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import datetime
import json
import logging
import math
import os
import sys

from time import sleep
from werkzeug.exceptions import NotFound

from rdr_service.model.code import Code
from rdr_service.model.genomics import UserEventMetrics
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import rebuild_bq_participant
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.dao.bq_code_dao import rebuild_bq_code_ids
from rdr_service.dao.bq_hpo_dao import bq_hpo_update_by_id
from rdr_service.dao.bq_organization_dao import bq_organization_update_by_id
from rdr_service.dao.bq_site_dao import bq_site_update_by_id
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.bq_questionnaires import PDR_MODULE_LIST
from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.onsite_id_verification import OnsiteIdVerification
from rdr_service.model.participant import Participant
from rdr_service.model.resource_data import ResourceData
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.offline.bigquery_sync import batch_rebuild_participants_task, sync_bigquery_handler
from rdr_service.resource import generators
from rdr_service.resource.generators.onsite_id_verification import onsite_id_verification_batch_rebuild
from rdr_service.resource.constants import SKIP_TEST_PIDS_FOR_PDR
from rdr_service.resource.tasks import batch_rebuild_consent_metrics_task
from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar, list_chunks
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "resource"
tool_desc = "Tools for updating and cleaning PDR resource records in RDR"

PDR_PROJECT_ID_MAP = {
    'all-of-us-rdr-sandbox': 'all-of-us-rdr-sandbox',
    'all-of-us-rdr-stable': 'aou-pdr-data-stable',
    'all-of-us-rdr-prod': 'aou-pdr-data-prod',
    'localhost': 'localhost'
}

MISC_TABLES = ('hpo', 'site', 'organization', 'code', 'onsite_verification_id')

class CleanPDRDataClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject, pk_id_list: None):
        self.args = args
        self.gcp_env = gcp_env
        if args.id:
            self.pk_id_list = [args.id, ]
        else:
            self.pk_id_list = pk_id_list

    def delete_pk_ids_from_bigquery_sync(self, table_id):
        """
        Delete the requested records from the bigquery_sync table.  Note, that to restore an unintentional delete,
        a rebuild can be performed of the associated pdr_mod_* data, but (for example) you may first need to find the
        participants who are associated with the deleted data and rebuild *all* of their module response data.
        :param table_id:  Filter value for the bigquery_sync.table_id column
        """
        # Target dataset for all the BigQuery RDR-to-PDR pipeline data
        dataset_id = 'rdr_ops_data_view'
        project_id = PDR_PROJECT_ID_MAP.get(self.gcp_env.project, None)
        if not project_id:
            raise ValueError(f'Unable to map {self.gcp_env.project} to an active BigQuery project')
        else:
            dao = BigQuerySyncDao()
            with dao.session() as session:

                # Verify the table_id matches at least some existing records in the bigquery_sync table
                query = session.query(BigQuerySync.id)\
                    .filter(BigQuerySync.projectId == project_id).filter(BigQuerySync.datasetId == dataset_id)\
                    .filter(BigQuerySync.tableId == table_id)
                if query.first() is None:
                    raise ValueError(f'No records found for bigquery_sync.table_id = {table_id}')

                batch_count = 500
                batch_total = len(self.pk_id_list)
                processed = 0
                for pk_ids in list_chunks(self.pk_id_list, batch_count):
                    session.query(BigQuerySync
                                  ).filter(BigQuerySync.projectId == project_id,
                                           BigQuerySync.datasetId == dataset_id,
                                           BigQuerySync.tableId == table_id
                                  ).filter(BigQuerySync.pk_id.in_(pk_ids)
                                  ).delete(synchronize_session=False)
                    # Inject a short delay between chunk-sized delete operations to avoid blocking other table updates
                    session.commit()
                    sleep(0.5)
                    processed += len(pk_ids)
                    if not self.args.debug:
                        print_progress_bar(
                            processed, batch_total, prefix="{0}/{1}:".format(processed, batch_total),
                            suffix="complete"
                        )

    def delete_resource_pk_ids_from_resource_data(self, resource_type_id):
        """ Perform deletions from the resource_data table based on resource_pk_id field matches """

        dao = ResourceDataDao()
        with dao.session() as session:
            batch_count = 500
            batch_total = len(self.pk_id_list)
            processed = 0
            for pk_ids in list_chunks(self.pk_id_list, batch_count):
                session.query(ResourceData
                              ).filter(ResourceData.resourceTypeID == resource_type_id
                              ).filter(ResourceData.resourcePKID.in_(pk_ids)
                              ).delete(synchronize_session=False)
                # Inject a short delay between chunk-sized delete operations to avoid blocking other table updates
                session.commit()
                sleep(0.5)
                processed += len(pk_ids)
                if not self.args.debug:
                    print_progress_bar(
                        processed, batch_total, prefix="{0}/{1}:".format(processed, batch_total),
                        suffix="complete"
                    )

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        clr = self.gcp_env.terminal_colors
        if not self.pk_id_list:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nClean PDR Data pipeline records:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
        if self.args.from_file:
            _logger.info('  PK_IDs File             : {0}'.format(clr.fmt(self.args.from_file)))

        _logger.info('  Total PK_IDs            : {0}'.format(clr.fmt(len(self.pk_id_list))))
        _logger.info('=' * 90)
        _logger.info('')

        if self.args.pdr_mod_responses:
            # This option/use case is primarily for orphaned responses due to retroactively flagged duplicate
            # questionnaire responses. Use the cleanup code already in the ResponseDuplicationDetector class
            dao = BigQuerySyncDao()
            with dao.session() as session:
                ResponseDuplicationDetector.clean_pdr_module_data(self.pk_id_list, session, self.gcp_env.project)
        else:
            if self.args.bq_table_id:
                self.delete_pk_ids_from_bigquery_sync(self.args.bq_table_id)

            # Can delete from both bigquery_sync and resource data tables on the same run as long as the
            # bigquery_sync.pk_id matches the resource_data.resource_pk_id for the resource_type_id specified.
            if self.args.resource_type_id:
                self.delete_resource_pk_ids_from_resource_data(self.args.resource_type_id)

class BigQuerySyncDryRunClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject, pk_id_list: None):
        self.args = args
        self.gcp_env = gcp_env
        self.pk_id_list = pk_id_list

    def run(self):
        clr = self.gcp_env.terminal_colors
        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nBigQuerySync dry run test (run in debug mode)', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('')
        self.gcp_env.activate_sql_proxy()
        sync_bigquery_handler(project_id=self.args.project, dryrun=True)


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
        self.qc_error_list = []

    def update_single_pid(self, pid):
        """
        Update a single pid
        :param pid: participant id
        :return: 0 if successful otherwise 1
        """
        try:
            if not self.args.modules_only:
                if not self.args.qc:
                    # Skip the BQ build in QC mode;  will just test the resource data return
                    rebuild_bq_participant(pid, project_id=self.gcp_env.project)
                res = generators.participant.rebuild_participant_summary_resource(pid, qc_mode=self.args.qc)

                if self.args.qc and self.args.project == 'all-of-us-rdr-prod':
                    # We didn't rebuild for BQ, but check the previously built participant record against the updated
                    # generator code output
                    w_dao = BigQuerySyncDao()
                    with w_dao.session() as session:
                        sql = f"""
                            select resource from resource_data
                            where resource_pk_id = {pid} and resource_type_id = 2
                        """
                        old_gen_data = session.execute(sql).first()
                        if old_gen_data:
                            old_dict = json.loads(old_gen_data.resource)
                            pid_dict = res.get_data()
                            for k, v in old_dict.items():
                                if isinstance(v, list) or isinstance(v, dict) or k.endswith('_time'):
                                    # Don't bother checking nested arrays, dicts, or obviously named timestamps
                                    continue
                                if pid_dict.get(k) != v:
                                    # Will still be some diffs in date/time strings that can be ignored
                                    # Typing differences and/or string formatting that can be verified visually
                                    print(f'P{pid}: {k} (old/new), {v}/{pid_dict.get(k)}')

            if not self.args.no_modules and not self.args.qc:
                mod_bqgen = BQPDRQuestionnaireResponseGenerator()

                # Generate participant questionnaire module response data
                for module in PDR_MODULE_LIST:
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
            if pid not in SKIP_TEST_PIDS_FOR_PDR:
                batch.append({'pid': pid})
                count += 1

            if count == batch_size:
                payload = {'batch': batch,
                           'build_modules': not self.args.no_modules,
                           'build_participant_summary': not self.args.modules_only
                           }

                if self.gcp_env.project == 'localhost':
                    batch_rebuild_participants_task(payload)
                else:
                    task.execute('rebuild_participants_task', payload=payload, in_seconds=30,
                                        queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

                batch_count += 1
                # reset for next batch
                batch = list()
                count = 0
                if not self.args.debug and not self.args.qc:
                    print_progress_bar(
                        batch_count, batch_total, prefix="{0}/{1}:".format(batch_count, batch_total), suffix="complete"
                    )

                # Collect the garbage after so long to prevent hitting open file limit.
                if batch_count % 250 == 0:
                    if self.args.qc:
                        print(f'({batch_count * 250} pids processed)')
                    gc.collect()

        # send last batch if needed.
        if count:
            payload = {'batch': batch,
                       'build_modules': not self.args.no_modules,
                       'build_participant_summary': not self.args.modules_only
                       }
            batch_count += 1
            if self.gcp_env.project == 'localhost':
                batch_rebuild_participants_task(payload)
            else:
                task.execute('rebuild_participants_task', payload=payload, in_seconds=30,
                                    queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            if not self.args.debug and not self.args.qc:
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

        if self.args.batch or (self.args.all_pids and not self.args.qc):
            return self.update_batch(pids)

        total_pids = len(pids)
        count = 0
        errors = 0

        for pid in pids:
            count += 1
            if pid in SKIP_TEST_PIDS_FOR_PDR:
                _logger.info(f'Skipping PDR data build for test pid {pid}')
                continue

            if self.update_single_pid(pid) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'PID {pid} not found.')

            if not self.args.debug:
                if self.args.qc:
                    if count % 250 == 0:
                        print(f'({count} pids processed)')
                else:
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
            if self.update_single_pid(self.args.pid) == 0 and not self.args.qc:
                _logger.info(f'Participant {self.args.pid} updated.')
            elif not self.args.qc:
                _logger.error(f'Participant ID {self.args.pid} not found.')

        return 1

# class GenomicResourceClass(object):
#
#     def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list: None):
#         """
#         :param args: command line arguments.
#         :param gcp_env: gcp environment information, see: gcp_initialize().
#         :param id_list: list of integer ids from a genomic table, if --genomic-table and --from-file were specified
#         """
#         self.args = args
#         self.gcp_env = gcp_env
#         self.id_list = id_list
#
#     def update_single_id(self, table, _id):
#
#         try:
#             if table == 'genomic_set':
#                 bq_genomic_set_update(_id, project_id=self.gcp_env.project)
#                 genomic_set_update(_id)
#             elif table == 'genomic_set_member':
#                 bq_genomic_set_member_update(_id, project_id=self.gcp_env.project)
#                 genomic_set_member_update(_id)
#             elif table == 'genomic_job_run':
#                 bq_genomic_job_run_update(_id, project_id=self.gcp_env.project)
#                 genomic_job_run_update(_id)
#             elif table == 'genomic_file_processed':
#                 bq_genomic_file_processed_update(_id, project_id=self.gcp_env.project)
#                 genomic_file_processed_update(_id)
#             elif table == 'genomic_manifest_file':
#                 bq_genomic_manifest_file_update(_id, project_id=self.gcp_env.project)
#                 genomic_manifest_file_update(_id)
#             elif table == 'genomic_manifest_feedback':
#                 bq_genomic_manifest_feedback_update(_id, project_id=self.gcp_env.project)
#                 genomic_manifest_feedback_update(_id)
#             elif table == 'genomic_gc_validation_metrics':
#                 bq_genomic_gc_validation_metrics_update(_id, project_id=self.gcp_env.project)
#                 genomic_gc_validation_metrics_update(_id)
#             elif table == 'genomic_informing_loop':
#                 genomic_informing_loop_update(_id)
#             elif table == 'genomic_cvl_result_past_due':
#                 genomic_cvl_result_past_due_update(_id)
#             elif table == 'genomic_member_report_state':
#                 genomic_member_report_state_update(_id)
#             elif table == 'genomic_result_viewed':
#                 genomic_result_viewed_update(_id)
#             elif table == 'genomic_appointment_event':
#                 genomic_appointment_event_update(_id)
#         except NotFound:
#             return 1
#         return 0
#
#     def update_batch(self, table, _ids):
#
#         count = 0
#         task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()
#
#         if not self.args.debug:
#             print_progress_bar(
#                 count, len(_ids), prefix="{0}/{1}:".format(count, len(_ids)), suffix="complete"
#             )
#
#         for batch in list_chunks(_ids, 250):
#             if self.gcp_env.project == 'localhost':
#                 for _id in batch:
#                     self.update_single_id(table, _id)
#             else:
#                 payload = {'table': table, 'ids': batch}
#                 task.execute('rebuild_genomic_table_records_task', payload=payload, in_seconds=30,
#                              queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)
#
#             count += len(batch)
#             if not self.args.debug:
#                 print_progress_bar(
#                     count, len(_ids), prefix="{0}/{1}:".format(count, len(_ids)), suffix="complete"
#                 )
#
#     def update_many_ids(self, table, _ids):
#         if not _ids:
#             return 1
#
#         _logger.info(f'Processing batch for table {table}...')
#         if self.args.batch:
#             self.update_batch(table, _ids)
#             _logger.info(f'Processing {table} batch complete.')
#             return 0
#
#         total_ids = len(_ids)
#         count = 0
#         errors = 0
#
#         for _id in _ids:
#             count += 1
#
#             if self.update_single_id(table, _id) != 0:
#                 errors += 1
#                 if self.args.debug:
#                     _logger.error(f'{table} ID {_id} not found.')
#
#             if not self.args.debug:
#                 print_progress_bar(
#                     count, total_ids, prefix="{0}/{1}:".format(count, total_ids), suffix="complete"
#                 )
#
#         if errors > 0:
#             _logger.warning(f'\n\nThere were {errors} IDs not found during processing.')
#
#         return 0
#
#     def run(self):
#
#         clr = self.gcp_env.terminal_colors
#
#         if not self.args.id and not self.args.all_ids and not self.args.all_tables and not self.id_list:
#             _logger.error('Nothing to do')
#             return 1
#
#         self.gcp_env.activate_sql_proxy()
#         _logger.info('')
#
#         _logger.info(clr.fmt('\nRebuild Genomic Records for PDR:', clr.custom_fg_color(156)))
#         _logger.info('')
#         _logger.info('=' * 90)
#         _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
#         _logger.info('  Genomic Table         : {0}'.format(clr.fmt(self.args.table)))
#
#         if self.args.all_ids or self.args.all_tables:
#             dao = ResourceDataDao()
#             _logger.info('  Rebuild All Records   : {0}'.format(clr.fmt('Yes')))
#             if self.args.all_tables:
#                 tables = [{'name': t, 'ids': list()} for t in GENOMIC_DB_TABLES]
#             else:
#                 tables = [{'name': self.args.table, 'ids': list()}]
#             _logger.info('  Rebuild Table(s)      : {0}'.format(
#                 clr.fmt(', '.join([t['name'] for t in tables]))))
#
#             for table in tables:
#                 with dao.session() as session:
#                     results = session.execute(f'select id from {table["name"]}')
#                     table['ids'] = [r.id for r in results]
#                     _logger.info('  Total Records         : {0} = {1}'.
#                                  format(clr.fmt(table["name"]), clr.fmt(len(table['ids']))))
#
#             for table in tables:
#                 self.update_many_ids(table['name'], table['ids'])
#
#         elif self.args.id:
#             _logger.info('  Record ID             : {0}'.format(clr.fmt(self.args.id)))
#             self.update_single_id(self.args.table, self.args.id)
#         elif self.id_list:
#             _logger.info('  Total Records         : {0}'.format(clr.fmt(len(self.id_list))))
#             if len(self.id_list):
#                 self.update_many_ids(self.args.table, self.id_list)
#
#         return 1


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
                payload = {'batch': batch, 'build_participant_summary': True, 'build_modules': False}

                if self.gcp_env.project == 'localhost':
                    batch_rebuild_participants_task(payload)
                else:
                    task.execute('rebuild_participants_task', payload=payload, in_seconds=30,
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
            payload = {'batch': batch, 'build_participant_summary': True, 'build_modules': False}
            batch_count += 1
            if self.gcp_env.project == 'localhost':
                batch_rebuild_participants_task(payload)
            else:
                task.execute('rebuild_participants_task', payload=payload, in_seconds=30,
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

class MiscResourceClass(object):
    """
    Generic class for miscellaneous RDR tables / PDR data rebuilds
    Uses pre-existing BQ/resource rebuild functions and call signatures, which don't have uniform parameter options.
    """
    # Details for queueing cloud tasks for rebuild (--batch mode).  Keys are RDR table names
    misc_table_batch_task_info = {
        'onsite_id_verification': {
            'pk_col': OnsiteIdVerification.id,
            'task_name': 'onsite_id_verification_batch_rebuild_task',
            'payload_id_list_key': 'onsite_verification_id_list',
        },
        'code': {
            'pk_col': Code.codeId,
            # Pre-existing task definition to rebuild all Code table entries (does not take a list of ids)
            'task_name': 'rebuild_codebook_task',
            'payload_id_list_key': None,
        },
        'hpo': {
            'pk_col': HPO.hpoId,
            'task_name': 'bq_hpo_update_all',
            'payload_id_list_key': None
        },
        'organization': {
            'pk_col': Organization.organizationId,
            'task_name': 'bq_organization_update_all',
            'payload_id_list_key': None
        },
        'site': {
            'pk_col': Site.siteId,
            'task_name': 'bq_site_update_all',
            'payload_id_list_key': None
        },
    }

    # For rebuilds on local server (--all-ids or --batch are not specified), functions that take a list of ids to build
    misc_table_build_id_list_func = {
        'onsite_id_verification': onsite_id_verification_batch_rebuild,
        'code': rebuild_bq_code_ids
    }
    # Pre-existing hpo/site/organization build functions for a single id parameter only
    misc_table_build_by_id_func = {
        'hpo': bq_hpo_update_by_id,
        'organization': bq_organization_update_by_id,
        'site': bq_site_update_by_id
    }

    def __init__(self, args, gcp_env: GCPEnvConfigObject, ids):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.args.id_list = ids if ids else []

    def run(self):
        clr = self.gcp_env.terminal_colors
        if not self.args.table or (self.args.id is None and self.args.id_list is None and not self.args.all_ids):
            _logger.error('Table or id options missing')
            return 1

        batch_mode = self.args.batch and self.gcp_env.project != 'localhost'
        table = self.args.table
        self.gcp_env.activate_sql_proxy()
        ro_dao = ResourceDataDao()
        _logger.info('')
        _logger.info(clr.fmt('\nRebuild records for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Database Table        : {0}'.format(clr.fmt(table)))

        if self.args.all_ids:
            _logger.info('  Rebuild all ids      : {0}'.format(clr.fmt('Yes')))
            batch_mode = True
            # Generate list of all ids for the cloud tasks, if a list of primary key ids is included in payload
            if self.misc_table_batch_task_info[table]['payload_id_list_key']:
                model_id_column = self.misc_table_batch_task_info[table]['pk_col']
                with ro_dao.session() as session:
                    rows = session.query(model_id_column).all()
                    if rows:
                        id_list = [r.id for r in rows]
            else:
                # E.g.: Pre-existing code table batch mode rebuild task, doesn't take an id list
                id_list = []
        else:
            # Get targeted id values specified by --id or --from-file
            id_list = [self.args.id] if self.args.id else self.args.id_list
            _logger.info('  Records to build     : {0}'.format(clr.fmt(len(id_list))))

        if batch_mode:
            task = GCPCloudTask()
            task_name = self.misc_table_batch_task_info[table]['task_name']
            task_id_list_key = self.misc_table_batch_task_info[table]['payload_id_list_key']
            task_count = 0
            if id_list:
                # Cloud tasks max id list size arbitrarily set to 500
                for batch_list in list_chunks(id_list, 500):
                    payload = {task_id_list_key: batch_list}
                    task.execute(task_name, payload=payload, in_seconds=30,
                                 queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)
                    task_count += 1

            # The pre-existing code table rebuild cloud task always builds all the ids, doesn't take an id list
            else:
                task.execute(task_name, in_seconds=30, queue='resource-rebuild',
                             project_id=self.gcp_env.project, quiet=True)
                task_count += 1

            if task_count:
                _logger.info(f'Queued {task_count} rebuild tasks on resource-rebuild queue')

        # Not triggering cloud tasks; rebuild functions called locally
        elif table in self.misc_table_build_by_id_func:
            # hpo/organization/site targeted id builds: can only pass one id at a time to the bq_*_build_by_id() func
            for rec_id in id_list:
                self.misc_table_build_by_id_func[table](rec_id, self.gcp_env.project)
            return 0

        else:
            for ids in list_chunks(id_list, 500):
                self.misc_table_build_id_list_func[table](ids)

        return 0


class RetentionEligibleMetricClass:
    """ Handle Retention Eligible Metric resource data """

    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        :param id_list: list of integer ids from retention eligible metrics table,
                            if --table and --from-file were specified.
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list
        self.res_gen = generators.RetentionEligibleMetricGenerator()

    def update_single_id(self, pid):

        try:
            res = self.res_gen.make_resource(pid)
            res.save()
        except NotFound:
            _logger.error(f'Participant P{pid} not found in retention_eligible_metrics table.')
            return 1
        return 0

    def update_batch(self, pids):

        count = 0
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        if not self.args.debug:
            print_progress_bar(
                count, len(pids), prefix="{0}/{1}:".format(count, len(pids)), suffix="complete"
            )

        for batch in list_chunks(pids, 250):
            if self.gcp_env.project == 'localhost':
                for id_ in batch:
                    self.update_single_id(id_)
            else:
                if isinstance(batch[0], int):
                    payload = {'rebuild_all': False, 'batch': batch}
                else:
                    payload = {'rebuild_all': False, 'batch': [x[0] for x in batch]}
                task.execute('batch_rebuild_retention_eligible_task', payload=payload, in_seconds=30,
                             queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            count += len(batch)
            if not self.args.debug:
                print_progress_bar(
                    count, len(pids), prefix="{0}/{1}:".format(count, len(pids)), suffix="complete"
                )

    def update_many_ids(self, pids):
        if not pids:
            _logger.warning(f'No records found in batch, skipping.')
            return 1

        _logger.info(f'Processing retention eligible metrics batch...')
        if self.args.batch:
            self.update_batch(pids)
            _logger.info(f'Processing retention eligible metrics batch complete.')
            return 0

        total_ids = len(pids)
        count = 0
        errors = 0

        for pid in pids:
            count += 1

            if self.update_single_id(pid) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'ID {pid} not found.')

            if not self.args.debug:
                print_progress_bar(
                    count, total_ids, prefix="{0}/{1}:".format(count, total_ids), suffix="complete"
                )

        if errors > 0:
            _logger.warning(f'\n\nThere were {errors} IDs not found during processing.')

        return 0

    def run(self):
        clr = self.gcp_env.terminal_colors

        if not self.args.pid and not self.args.all_pids and not self.id_list:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild Retention Eligible Records for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))

        if self.args.all_pids :
            dao = ResourceDataDao()
            _logger.info('  Rebuild All Records   : {0}'.format(clr.fmt('Yes')))
            _logger.info('=' * 90)
            with dao.session() as session:
                pids = session.query(RetentionEligibleMetrics.participantId).all()
                self.update_many_ids(pids)
        elif self.args.pid:
            _logger.info('  Participant ID        : {0}'.format(clr.fmt(f'P{self.args.pid}')))
            _logger.info('=' * 90)
            self.update_single_id(self.args.pid)
        elif self.id_list:
            _logger.info('  Total Records         : {0}'.format(clr.fmt(len(self.id_list))))
            _logger.info('=' * 90)
            if len(self.id_list):
                self.update_many_ids(self.id_list)

        return 1

class ConsentMetricClass(object):
    """ Build consent validation metrics records for PDR extract """

    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        :param id_list: list of integer ids from consent_file table, if from_file or modified_since were specified.
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list
        self.res_gen = generators.ConsentMetricGenerator()

    def update_batch(self, _ids):
        """ Batch update of ConsentMetric resource records """

        batch_size = 250
        count = 0
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        batches = len(_ids) // batch_size
        if len(_ids) % batch_size:
            batches += 1

        _logger.info('  Records     : {0}'.format(self.gcp_env.terminal_colors.fmt(len(_ids))))
        _logger.info('  Batch size  : {0}'.format(self.gcp_env.terminal_colors.fmt(batch_size)))
        _logger.info('  Tasks       : {0}'.format(self.gcp_env.terminal_colors.fmt(batches)))

        for batch in list_chunks(_ids, batch_size):
            payload = {'batch': [id for id in batch]}
            if self.gcp_env.project == 'localhost':
                batch_rebuild_consent_metrics_task(payload)
            else:
                task.execute('batch_rebuild_consent_metrics_task', payload=payload, in_seconds=30,
                             queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            count += 1
            if not self.args.debug:
                print_progress_bar(
                    count, batches, prefix="{0}/{1} tasks queued:".format(count, batches, suffix="complete"))

    def run(self):

        self.gcp_env.activate_sql_proxy()
        clr = self.gcp_env.terminal_colors
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild Consent Validation Metrics Records for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Batch mode enabled    : {0}'.format(clr.fmt('Yes' if self.args.batch else 'No')))

        dao = ResourceDataDao(backup=True)
        csv_lines = []
        if self.args.all_ids:
            _logger.info('  Rebuild All Records   : {0}'.format(clr.fmt('Yes')))
            with dao.session() as session:
                self.id_list = [r.id for r in session.query(ConsentFile.id).all()]
        elif hasattr(self.args, 'from_file') and self.args.from_file:
            self.id_list = get_id_list(self.args.from_file)
        elif hasattr(self.args, 'id') and self.args.id:
            # Use a one-element id list for rebuilding single ids
            self.id_list = [self.args.id, ]
        # The --modified-since option is applied last; will be ignored if another option provided the id list
        elif self.args.modified_since:
            with dao.session() as session:
                results = session.query(ConsentFile.id).filter(ConsentFile.modified >= self.args.modified_since).all()
                self.id_list = [r.id for r in results]

        if not (self.id_list and len(self.id_list)):
            _logger.error('Nothing to do')
            return 1

        if self.args.batch:
            self.update_batch(self.id_list)
            return 0
        elif len(self.id_list) > 2500:
            # Issue a warning if a local rebuild will exceed 2500 records; default to aborting
            response = input(f'\n{len(self.id_list)} records will be rebuilt.  Continue without --batch mode? [y/N]')
            if not response or response.upper() == 'N':
                _logger.error('Aborted by user')
                return 1

        results = self.res_gen.get_consent_validation_records(dao=dao, id_list=self.id_list)
        csv_lines = []
        column_headers = []
        count = 0
        if results:
            if self.args.to_file:
                line = "\t".join(column_headers)
                csv_lines.append(f'{line}\n')
            for row in results:
                resource_data = self.res_gen.make_resource(row.id, consent_validation_rec=row)
                if self.args.to_file:
                    # First line should be the column headers
                    if not len(column_headers):
                        column_headers = sorted(resource_data.get_data().keys())
                        line = "\t".join(column_headers)
                        csv_lines.append(f'{line}\n')

                    csv_values = []
                    for key, value in sorted(resource_data.get_data().items()):
                        if key in ['created', 'modified']:
                            csv_values.append(value.strftime("%Y-%m-%d %H:%M:%S") if value else "")
                        elif key in ['consent_authored_date', 'resolved_date']:
                            csv_values.append(value.strftime("%Y-%m-%d") if value else "")
                        elif isinstance(value, bool):
                            csv_values.append("1" if value else "0")
                        elif key == 'participant_id':
                            csv_values.append(value[1:])
                        else:
                            csv_values.append(str(value) if value else "")

                    line = "\t".join(csv_values)
                    csv_lines.append(f'{line}\n')
                else:
                    resource_data.save(w_dao=ResourceDataDao(backup=False))

                count += 1
                if not self.args.debug:
                    print_progress_bar(
                        count, len(results), prefix="{0}/{1}:".format(count, len(results)), suffix="complete"
                    )

                if self.args.to_file:
                    with open(self.args.to_file, "w") as f:
                        f.writelines(csv_lines)
        return 0


class UserEventMetricsClass(object):
    """ Build Color user event metrics records for PDR extract """

    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list: None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        :param id_list: list of integer ids from consent_file table, if from_file or modified_since were specified.
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list
        self.res_gen = generators.GenomicUserEventMetricsSchemaGenerator()

    def update_single_id(self, id_):

        try:
            res = self.res_gen.make_resource(id_)
            res.save()
        except NotFound:
            _logger.error(f'Primary key id {id_} not found in user_event_metrics table.')
            return 1
        return 0

    def update_batch(self, ids):

        count = 0
        task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        if not self.args.debug:
            print_progress_bar(
                count, len(ids), prefix="{0}/{1}:".format(count, len(ids)), suffix="complete"
            )

        for batch in list_chunks(ids, 250):
            if self.gcp_env.project == 'localhost':
                for id_ in batch:
                    self.update_single_id(id_)
            else:
                if isinstance(batch[0], int):
                    payload = {'rebuild_all': False, 'batch': batch}
                else:
                    payload = {'rebuild_all': False, 'batch': [x[0] for x in batch]}
                task.execute('batch_rebuild_user_event_metrics_task', payload=payload, in_seconds=30,
                             queue='resource-rebuild', project_id=self.gcp_env.project, quiet=True)

            count += len(batch)
            if not self.args.debug:
                print_progress_bar(
                    count, len(ids), prefix="{0}/{1}:".format(count, len(ids)), suffix="complete"
                )

    def update_many_ids(self, ids):
        if not ids:
            _logger.warning(f'No records found in batch, skipping.')
            return 1

        _logger.info(f'Processing user event metrics batch...')
        if self.args.batch:
            self.update_batch(ids)
            _logger.info(f'Processing retention eligible metrics batch complete.')
            return 0

        total_ids = len(ids)
        count = 0
        errors = 0

        for id_ in ids:
            count += 1

            if self.update_single_id(id_) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'ID {id_} not found.')

            if not self.args.debug:
                print_progress_bar(
                    count, total_ids, prefix="{0}/{1}:".format(count, total_ids), suffix="complete"
                )

        if errors > 0:
            _logger.warning(f'\n\nThere were {errors} IDs not found during processing.')

        return 0

    def run(self):
        clr = self.gcp_env.terminal_colors

        if not self.args.id and not self.args.all_ids and not self.id_list:
            _logger.error('Nothing to do')
            return 1

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        _logger.info(clr.fmt('\nRebuild User Event Metric Records for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))

        if self.args.all_ids :
            dao = ResourceDataDao()
            _logger.info('  Rebuild All Records   : {0}'.format(clr.fmt('Yes')))
            _logger.info('=' * 90)
            with dao.session() as session:
                ids = session.query(UserEventMetrics.id).all()
                self.update_many_ids(ids)
        elif self.args.id:
            _logger.info('  Primary Key ID        : {0}'.format(clr.fmt(f'{self.args.id}')))
            _logger.info('=' * 90)
            self.update_single_id(self.args.id)
        elif self.id_list:
            _logger.info('  Total Records         : {0}'.format(clr.fmt(len(self.id_list))))
            _logger.info('=' * 90)
            if len(self.id_list):
                self.update_many_ids(self.id_list)

        return 1

def get_id_list(fname):
    """
    Shared helper routine for tool classes that allow input from a file of integer ids (participant ids or
    id values from a specific table).
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
    pid_parser = argparse.ArgumentParser(add_help=False)
    pid_parser.add_argument("--pid", help="rebuild single participant id", type=int, default=None)

    all_pids_parser = argparse.ArgumentParser(add_help=False)
    all_pids_parser.add_argument("--all-pids", help="rebuild all participants", default=False, action="store_true")

    id_parser = argparse.ArgumentParser(add_help=False)
    id_parser.add_argument("--id", help="rebuild single genomic table id", type=int, default=None)

    all_ids_parser = argparse.ArgumentParser(add_help=False)
    all_ids_parser.add_argument("--all-ids", help="rebuild all records", default=False, action="store_true")

    from_file_parser = argparse.ArgumentParser(add_help=False)
    from_file_parser.add_argument("--from-file", help="rebuild resource ids from a file with a list of ids",
                                metavar='FILE', type=str, default=None)

    table_parser = argparse.ArgumentParser(add_help=False)
    table_parser.add_argument("--table", help="db table name to rebuild from", type=str,
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

    # def argument_conflict(args_, ids_, choices=()):
    #     """ Check if common arguments conflict """
    #     if args_.table and args_.all_tables:
    #         _logger.error("Arguments 'table' and 'all-tables' conflict.")
    #         return True
    #     elif args_.all_tables and args_.from_file:
    #         _logger.error("Argument 'from-file' cannot be used with 'all-tables', only with 'table'")
    #         return True
    #     elif args_.id and ids_:
    #         _logger.error("Argument 'from-file' cannot be used if a single 'id' was also specified")
    #         return True
    #     elif ids_ and not args_.table:
    #         _logger.error("Argument 'from-file' was provided without a specified 'table' ")
    #         return True
    #     if args_.table and args_.table not in choices:
    #         _logger.error(f"Argument 'table' value '{args_.table}' is invalid, possible values are:\n   {choices}.")
    #         return True
    #     return False

    # Rebuild participant resources
    rebuild_parser = subparser.add_parser(
        "participant",
        parents=[from_file_parser, batch_parser, pid_parser, all_pids_parser])
    rebuild_parser.add_argument("--no-modules", default=False, action="store_true",
                                help="do not rebuild participant questionnaire response data for pdr_mod_* tables")
    rebuild_parser.add_argument("--modules-only", default=False, action="store_true",
                                help="only rebuild participant questionnaire response data for pdr_mod_* tables")
    rebuild_parser.add_argument("--qc", default=False, action="store_true",
                                help="Goal 1 quality control to compare RDR and PDR enrollment status values")
    update_argument(rebuild_parser, dest='from_file',
                    help="rebuild participant ids from a file with a list of pids")

    # Rebuild EHR receipt resources.
    ehr_parser = subparser.add_parser('ehr-receipt', parents=[batch_parser, from_file_parser])
    ehr_parser.add_argument("--ehr", help="Submit batch to Cloud Tasks", default=False,
                                action="store_true")  # noqa
    update_argument(ehr_parser, dest='from_file',
                    help="rebuild EHR info for specific participant ids read from a file with a list of pids")

    # Rebuild hpo/site/organization tables.  Specify a single table name or all-tables
    misc_parser = subparser.add_parser(
        "misc-tables",
        parents=[table_parser, all_ids_parser, id_parser, from_file_parser, batch_parser]
    )
    update_argument(misc_parser, dest='all_ids', help='rebuild all ids from the code table (default)')
    update_argument(misc_parser, 'table', help='db table name to rebuild from.  All ids will be rebuilt')
    misc_parser.epilog = f'Possible TABLE values: {{{",".join(MISC_TABLES)}}}.'

    # Rebuild Retention Eligibility resources
    retention_parser = subparser.add_parser(
        'retention', parents=[batch_parser, from_file_parser, pid_parser, all_pids_parser])
    update_argument(retention_parser, dest='from_file',
                    help="rebuild retention eligibility records for specific pids read from a file.")

    # Rebuild Consent Validation Metrics resources
    consent_metrics_parser = subparser.add_parser('consent-metrics', parents=[batch_parser,
                                                                              from_file_parser,
                                                                              all_ids_parser,
                                                                              id_parser])
    consent_metrics_parser.add_argument("--modified-since",
                                        dest='modified_since',
                                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'),
                                        help="Modified date in in YYYY-MM-DD format"
                                        )
    consent_metrics_parser.add_argument("--to-file", dest="to_file", type=str, default=None,
                                        help="TSV file to save generated records to instead of saving to database"
                                        )
    update_argument(consent_metrics_parser, dest='from_file',
                    help="rebuild consent metrics data for specific consent_file ids read from a file")
    update_argument(consent_metrics_parser, dest='all_ids',
                    help="rebuild metrics records for all consent_file ids")
    update_argument(consent_metrics_parser, dest='id', help="rebuild metrics for a specific consent_file id record")

    # Rebuild Color user event metrics resources
    user_event_metrics_parser = subparser.add_parser('user-event-metrics', parents=[batch_parser,
                                                                              from_file_parser,
                                                                              all_ids_parser,
                                                                              id_parser])
    update_argument(user_event_metrics_parser, dest='from_file',
                    help="rebuild user event metrics data for specific ids read from a file")
    update_argument(user_event_metrics_parser, dest='all_ids',
                    help="rebuild user event metrics records for all ids")
    update_argument(user_event_metrics_parser, dest='id', help="rebuild user event metrics for a specific id record")

    # Perform cleanup of PDR data records orphaned because of related RDR data table backfills/cleanup
    clean_pdr_data_parser = subparser.add_parser('clean-pdr-data', parents=[from_file_parser, id_parser])
    update_argument(clean_pdr_data_parser, dest='id',
                    help="The id (pk_id for bigquery_sync, resource_pk_id for resource_data) of a record to delete"
                    )
    update_argument(clean_pdr_data_parser, dest='from_file',
                    help="file with PDR data record pk_ids (bigquery_sync) or resource_pk_ids (resource_data) to delete"
                    )

    clean_pdr_data_parser.add_argument('--bq-table-id', type=str, default=None,
                                   help='table_id value whose bigquery_sync records should be cleaned')
    clean_pdr_data_parser.add_argument('--resource-type-id', dest='resource_type_id', type=int, default=None,
                                   help='resource_type_id whose resource_data records should be cleaned')
    clean_pdr_data_parser.add_argument('--pdr-mod-responses', default=False, action='store_true',
                                        help="clean all pdr_mod_* tables based on questionnaire_response_id list")

    bigquery_sync_dryrun_parser = subparser.add_parser('bigquery-sync-dryrun') # pylint: disable=unused-variable

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        ids = None
        if hasattr(args, 'from_file') and args.from_file:
            ids = get_id_list(args.from_file)

        # Rebuild participant resources
        if args.resource == 'participant':
            process = ParticipantResourceClass(args, gcp_env, ids)
            exit_code = process.run()

        # # Rebuild genomic resources.
        # elif args.resource == 'genomic':
        #     if argument_conflict(args, ids, choices=GENOMIC_DB_TABLES):
        #         sys.exit(1)
        #
        #     process = GenomicResourceClass(args, gcp_env, ids)
        #     exit_code = process.run()

        # Rebuild EHR receipt resources.
        elif args.resource == 'ehr-receipt':
            process = EHRReceiptClass(args, gcp_env, ids)
            exit_code = process.run()

        elif args.resource == 'misc-tables':
            process = MiscResourceClass(args, gcp_env, ids)
            exit_code = process.run()

        elif args.resource == 'retention':
            process = RetentionEligibleMetricClass(args, gcp_env, ids)
            exit_code = process.run()

        elif args.resource == 'consent-metrics':
            process = ConsentMetricClass(args, gcp_env, ids)
            exit_code = process.run()

        elif args.resource == 'user-event-metrics':
            process = UserEventMetricsClass(args, gcp_env, ids)
            exit_code = process.run()

        elif args.resource == 'clean-pdr-data':
            process = CleanPDRDataClass(args, gcp_env, ids)
            exit_code = process.run()
        elif args.resource == 'bigquery-sync-dryrun':
            process = BigQuerySyncDryRunClass(args, gcp_env, ids)
            exit_code = process.run()
        else:
            _logger.info('Please select an option to run. For help use "[resource] --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
