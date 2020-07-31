#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
from datetime import datetime

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator, rebuild_bq_participant
from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.model.bq_questionnaires import BQPDRConsentPII, BQPDRTheBasics, BQPDRLifestyle, BQPDROverallHealth, \
    BQPDREHRConsentPII, BQPDRDVEHRSharing, BQPDRCOPEMay
from rdr_service.resource.generators import ParticipantSummaryGenerator
from rdr_service.resource.generators.participant import rebuild_participant_summary_resource


def batch_rebuild_participants_task(payload):
    """
    Loop through all participants in batch and generate the BQ participant summary data and
    store it in the biguqery_sync table.
    Warning: this will force a rebuild and eventually a re-sync for every participant record.
    :param payload: Dict object with list of participants to work on.
    """
    ps_bqgen = BQParticipantSummaryGenerator()
    res_gen = ParticipantSummaryGenerator()

    pdr_bqgen = BQPDRParticipantSummaryGenerator()
    mod_bqgen = BQPDRQuestionnaireResponseGenerator()
    count = 0

    batch = payload['batch']

    logging.info(f'Start time: {datetime.utcnow()}, batch size: {len(batch)}')

    for item in batch:
        p_id = item['pid']
        count += 1

        rebuild_participant_summary_resource(p_id, res_gen=res_gen)

        ps_bqr = rebuild_bq_participant(p_id, ps_bqgen=ps_bqgen, pdr_bqgen=pdr_bqgen)
        # Test to see if participant record has been filtered.
        if not ps_bqr:
            continue

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
            table, mod_bqrs = mod_bqgen.make_bqrecord(p_id, mod.get_schema().get_module_name())
            if not table:
                continue

            w_dao = BigQuerySyncDao()
            with w_dao.session() as w_session:
                for mod_bqr in mod_bqrs:
                    mod_bqgen.save_bqrecord(mod_bqr.questionnaire_response_id, mod_bqr, bqtable=table,
                                            w_dao=w_dao, w_session=w_session)

    logging.info(f'End time: {datetime.utcnow()}, rebuilt BigQuery data for {count} participants.')
