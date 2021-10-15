#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# import json
import logging
from datetime import datetime


from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator, rebuild_bq_participant
from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.model.bq_questionnaires import PDR_MODULE_LIST
from rdr_service.resource import generators
from rdr_service.resource.generators.participant import rebuild_participant_summary_resource


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
