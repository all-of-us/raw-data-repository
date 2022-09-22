#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# import json
import logging
from datetime import datetime

import rdr_service.config as config
from rdr_service.resource.constants import SKIP_TEST_PIDS_FOR_PDR

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator, rebuild_bq_participant
from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.model.bq_questionnaires import PDR_MODULE_LIST
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.resource import generators
from rdr_service.resource.generators.participant import rebuild_participant_summary_resource
from rdr_service.resource.generators.consent_metrics import ConsentErrorReportGenerator
from rdr_service.services.system_utils import list_chunks


def batch_rebuild_participants_task(payload, project_id=None):
    """
    Loop through all participants in batch and generate the BQ participant summary data and
    store it in the biguqery_sync table.
    Warning: this will force a rebuild and eventually a re-sync for every participant record.
    :param payload: Dict object with list of participants to work on.
    :param project_id: String identifier for the GAE project
    """
    res_gen = generators.ParticipantSummaryGenerator()

    ps_bqgen = BQParticipantSummaryGenerator()
    pdr_bqgen = BQPDRParticipantSummaryGenerator()
    mod_bqgen = BQPDRQuestionnaireResponseGenerator()
    count = 0

    batch = payload['batch']
    # Boolean/flag fields indicating which elements to rebuild.  Default to True if not specified in payload
    # This is intended to improve performance/efficiency for targeted PDR rebuilds which may only affect (for example)
    # the participant summary data but do not require all the module response data to be rebuilt (or vice versa)
    # TODO: Pass a list of specific modules to build (empty if skipping all modules) instead of a flag
    build_participant_summary = payload.get('build_participant_summary', True)
    build_modules = payload.get('build_modules', True)

    logging.info(f'Start time: {datetime.utcnow()}, batch size: {len(batch)}')
    # logging.info(json.dumps(batch, indent=2))
    if not build_participant_summary:
        logging.info('Skipping rebuild of participant_summary data')
    if not build_modules:
        logging.info('Skipping rebuild of participant module responses')

    for item in batch:
        p_id = item['pid']
        patch_data = item.get('patch', None)
        count += 1

        if int(p_id) in SKIP_TEST_PIDS_FOR_PDR:
            logging.warning(f'Skipping rebuild of test pid {p_id} data')
            continue

        if build_participant_summary:
            rebuild_participant_summary_resource(p_id, res_gen=res_gen, patch_data=patch_data)

            ps_bqr = rebuild_bq_participant(p_id, ps_bqgen=ps_bqgen, pdr_bqgen=pdr_bqgen, patch_data=patch_data,
                                            project_id=project_id)
            # Test to see if participant record has been filtered or we are just patching.
            if not ps_bqr or patch_data:
                continue

        if build_modules:
            # Generate participant questionnaire module response data
            for module in PDR_MODULE_LIST:
                mod = module()
                table, mod_bqrs = mod_bqgen.make_bqrecord(p_id, mod.get_schema().get_module_name())
                if not table:
                    continue

                # TODO: Switch this to ResourceDataDAO, but make sure we don't break anything when the switch is made.
                w_dao = BigQuerySyncDao()
                with w_dao.session() as w_session:
                    for mod_bqr in mod_bqrs:
                        mod_bqgen.save_bqrecord(mod_bqr.questionnaire_response_id, mod_bqr, bqtable=table,
                                                w_dao=w_dao, w_session=w_session, project_id=project_id)

    logging.info(f'End time: {datetime.utcnow()}, rebuilt BigQuery data for {count} participants.')


def batch_rebuild_retention_metrics_task(payload):
    """
    Rebuild all or a batch of Retention Eligible Metrics
    :param payload: Dict object with list of participants to work on.
    """
    res_gen = generators.RetentionEligibleMetricGenerator()
    batch = payload.get('batch')
    count = 0

    for pid in batch:
        res = res_gen.make_resource(pid)
        res.save()
        count += 1

    logging.info(f'End time: {datetime.utcnow()}, rebuilt {count} Retention Metrics records.')

def check_consent_errors_task(payload):
    """
    Review previously unreported consent errors and generate an automated error report
    """
    origin = payload.get('participant_origin', 'vibrent')
    # DA-2611: Generate a list of all previously unreported errors, based on ConsentErrorReport table content
    gen = ConsentErrorReportGenerator()
    id_list = gen.get_unreported_error_ids()
    if id_list and len(id_list):
        gen.create_error_reports(participant_origin=origin, id_list=id_list)
    else:
        logging.info(f'No unreported consent errors found for participants with origin {origin}')


def batch_rebuild_consent_metrics_task(payload):
    """
     Rebuild a batch of consent metrics records based on ids from the consent_file table
     :param payload: Dict object with list of ids to work on.
     """
    res_gen = generators.ConsentMetricGenerator()
    batch = payload.get('batch')

    # Retrieve the consent_file table records by id
    results = res_gen.get_consent_validation_records(id_list=batch)
    for row in results:
        res = res_gen.make_resource(row.id, consent_validation_rec=row)
        res.save()

    logging.info(f'End time: {datetime.utcnow()}, rebuilt {len(results)} ConsentMetric records.')


def batch_rebuild_user_event_metrics_task(payload):
    """
     Rebuild a batch of user event metrics records based on ids
     :param payload: Dict object with list of ids to work on.
     """
    res_gen = generators.GenomicUserEventMetricsSchemaGenerator()
    batch = payload.get('batch')
    count = 0

    for id_ in batch:
        res = res_gen.make_resource(id_)
        res.save()
        count += 1

    logging.info(f'End time: {datetime.utcnow()}, rebuilt {count} User Event Metrics records.')

# TODO:  Look at consolidating dispatch_participant_rebuild_tasks() from offline/bigquery_sync.py and this into a
# generic dispatch routine also available for other resource type rebuilds.  May need to have
# endpoint-specific logic and/or some fancy code to dynamically populate the task.execute() args (or to allow for
# local rebuilds vs. cloud tasks)
def dispatch_rebuild_consent_metrics_tasks(id_list, in_seconds=30, quiet=True, batch_size=150,
                                           project_id=None, build_locally=False):
    """
    Helper method to handle queuing batch rebuild requests for rebuilding consent metrics resource data
    """
    if project_id is None:
        project_id = config.GAE_PROJECT

    if not all(isinstance(id, int) for id in id_list):
        raise (ValueError, "Invalid id list; must be a list that contains only integer consent_file ids")

    if build_locally or project_id == 'localhost':
        batch_rebuild_consent_metrics_task({'batch': id_list})
    else:
        completed_batches = 0
        task = GCPCloudTask()
        for batch in list_chunks(id_list, batch_size):
            payload = {'batch': batch}
            task.execute('batch_rebuild_consent_metrics_task', payload=payload, in_seconds=in_seconds,
                         queue='resource-rebuild', quiet=quiet, project_id=project_id)
            completed_batches += 1

        logging.info(f'Dispatched {completed_batches} batch_rebuild_consent_metrics tasks of max size {batch_size}')

def dispatch_check_consent_errors_task(in_seconds=30, quiet=True, origin=None,
                                       project_id=config.GAE_PROJECT, build_locally=False):
    """
    Create / queue a task that will check for unreported validation errors and generate error reports
    """
    payload = {'participant_origin': origin}
    if build_locally or project_id == 'localhost':
        check_consent_errors_task(payload)
    else:
        task = GCPCloudTask()
        task.execute('check_consent_errors_task', payload=payload, in_seconds=in_seconds,
                     queue='resource-tasks', quiet=quiet, project_id=project_id)

        logging.info(f'Dispatched consent error reporting task to run in {in_seconds} seconds')
