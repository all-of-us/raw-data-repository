import datetime
import json
import re

from rdr_service import config
from rdr_service.resource.constants import SKIP_TEST_PIDS_FOR_PDR
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_participant_summary import BQParticipantSummarySchema, BQParticipantSummary
from rdr_service.model.bq_pdr_participant_summary import BQPDRParticipantSummary
from rdr_service.resource.generators import ParticipantSummaryGenerator

# helpers to map from the resource participant summary to the bigquery participant summary.
SUB_PREFIXES = {
    'modules': 'mod_',
    'pm': 'pm_',
    'samples': 'bbs_',
    'biobank_orders': 'bbo_'
}

SUB_FIELD_MAP = {
    'module_authored': 'mod_authored',
    'module_created': 'mod_created',
}


class BQParticipantSummaryGenerator(BigQueryGenerator):
    """
    Generate a Participant Summary BQRecord object
    """
    ro_dao = None
    # Retrieve module and sample test lists from config.
    _baseline_modules = [mod.replace('questionnaireOn', '')
                         for mod in config.getSettingList('baseline_ppi_questionnaire_fields')]
    _baseline_sample_test_codes = config.getSettingList('baseline_sample_test_codes')
    _dna_sample_test_codes = config.getSettingList('dna_sample_test_codes')

    def _fix_prefixes(self, st_name, st_data):
        """
        Fix sub-table prefixes, this is a recursive function.
        :param table: sub-table key
        :param data: sub-table dict
        :return: dict
        """
        data = dict()
        for k, v in st_data.items():
            # Add prefixes to each field unless it is a sub-table.
            nk = f'{SUB_PREFIXES[st_name]}{k}' if k not in SUB_FIELD_MAP else SUB_FIELD_MAP[k]
            if k not in SUB_PREFIXES:
                data[nk] = v
            else:
                # Recursively process the next sub-table.  IE: Biobank order samples.
                data[nk] = [self._fix_prefixes(k, r) for r in st_data[k]]
        return data

    def make_bqrecord(self, p_id, convert_to_enum=False):
        """
        Build a Participant Summary BQRecord object for the given participant id.
        :param p_id: participant id
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        # NOTE: Generator code is now only in 'rdr_service/resource/generators/participant.py'.

        res = ParticipantSummaryGenerator().make_resource(p_id)
        summary = res.get_data()

        # Add sub-table field prefixes back in and map a few other fields.
        for k, v in SUB_PREFIXES.items():  # pylint: disable=unused-variable
            if k not in summary:
                continue
            summary[k] = [self._fix_prefixes(k, r) for r in summary[k]]
        # Convert participant id to an integer
        if 'participant_id' in summary and summary['participant_id']:
            summary['participant_id'] = int(re.sub("[^0-9]", "", str(summary['participant_id'])))

        return BQRecord(schema=BQParticipantSummarySchema, data=summary, convert_to_enum=convert_to_enum)

    def patch_bqrecord(self, p_id, data):
        """
        Upsert data into an existing resource.  Warning: No data recalculation is performed in this method.
        Note: This method uses the MySQL JSON_SET function to update the resource field in the backend.
              It does not return the full resource record here.
        https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-set
        :param p_id: participant id
        :param data: dict object
        :return: dict
        """
        sql_json_set_values = ', '.join([f"'$.{k}', :p_{k}" for k, v in data.items()])

        args = {'pid': p_id, 'table_id': 'participant_summary', 'modified': datetime.datetime.utcnow()}
        for k, v in data.items():
            args[f'p_{k}'] = v

        sql = f"""
            update bigquery_sync
                set modified = :modified, resource = json_set(resource, {sql_json_set_values})
               where pk_id = :pid and table_id = :table_id
        """
        dao = BigQuerySyncDao(backup=False)
        with dao.session() as session:
            session.execute(sql, args)

            sql = 'select resource from bigquery_sync where pk_id = :pid and table_id = :table_id limit 1'

            rec = session.execute(sql, args).first()
            if rec:
                return BQRecord(schema=BQParticipantSummarySchema, data=json.loads(rec.resource),
                                convert_to_enum=False)
        return None


def rebuild_bq_participant(p_id, ps_bqgen=None, pdr_bqgen=None, project_id=None, patch_data=None,
                           qc_mode=False):
    """
    Rebuild a BQ record for a specific participant
    :param p_id: participant id
    :param ps_bqgen: BQParticipantSummaryGenerator object
    :param pdr_bqgen: BQPDRParticipantSummaryGenerator object
    :param project_id: Project ID override value.
    :param patch_data: dict of resource values to update/insert.
    :param qc_mode: if True, the BQ data will be generated and returned but will not be saved to the database
    :return:
    """
    # Allow for batch requests to rebuild participant summary data.
    if not ps_bqgen:
        ps_bqgen = BQParticipantSummaryGenerator()
    if not pdr_bqgen:
        from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
        pdr_bqgen = BQPDRParticipantSummaryGenerator()

    # See if this is a partial update.
    if patch_data and isinstance(patch_data, dict):
        ps_bqr = ps_bqgen.patch_bqrecord(p_id, patch_data)
    else:
        ps_bqr = ps_bqgen.make_bqrecord(p_id)

    # Since the PDR participant summary is primarily a subset of the Participant Summary, call the full
    # Participant Summary generator and take what we need from it.
    pdr_bqr = pdr_bqgen.make_bqrecord(p_id, ps_bqr=ps_bqr)

    if not qc_mode:
        w_dao = BigQuerySyncDao()

        with w_dao.session() as w_session:
            # save the participant summary record if this is a full rebuild.
            if not patch_data and isinstance(patch_data, dict):
                ps_bqgen.save_bqrecord(p_id, ps_bqr, bqtable=BQParticipantSummary, w_dao=w_dao, w_session=w_session,
                                       project_id=project_id)
            # save the PDR participant summary record
            pdr_bqgen.save_bqrecord(p_id, pdr_bqr, bqtable=BQPDRParticipantSummary, w_dao=w_dao, w_session=w_session,
                                    project_id=project_id)
            w_session.flush()

    return ps_bqr


def bq_participant_summary_update_task(p_id):
    """
    Cloud task to update the Participant Summary record for the given participant.
    :param p_id: Participant ID
    """

    if p_id not in SKIP_TEST_PIDS_FOR_PDR:
        rebuild_bq_participant(p_id)
