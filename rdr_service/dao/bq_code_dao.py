import logging

from sqlalchemy.sql import text

from rdr_service.app_util import task_auth_required
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_code import BQCode
from rdr_service.model.bq_code import BQCodeSchema
from rdr_service.model.code import Code, CodeType


class BQCodeGenerator(BigQueryGenerator):
    """
    Generate a Code BQRecord object
    """

    def make_bqrecord(self, code_id, convert_to_enum=False):
        """
        Build a BQRecord object from the given code id.
        :param code_id: Primary key value from code table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        ro_dao = BigQuerySyncDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from code where code_id = :id'), {'id': code_id}).first()
            data = ro_dao.to_dict(row)
            # Map the enumerated CodeType value
            data['code_type_id'] = data['code_type']
            data['code_type'] = str(CodeType(data['code_type_id']))
            # In case the (RDR) code.value string has issues BigQuery schemas may complain about (contains whitespace
            # or /, for example), code.value may be converted to a BQ-friendly field name in pdr_mod_* table
            # schemas.  Keep that converted string with the code table record, for reference.
            # (Note: make_bq_field_name() returns a second msg string value, which is ignored/unused here)
            data['bq_field_name'] = BQCodeSchema.make_bq_field_name(data.get('value'), data.get('short_value'))[0]

            return BQRecord(schema=BQCodeSchema, data=data, convert_to_enum=convert_to_enum)

def rebuild_bq_code_ids(_pk_ids):
    """
    Rebuild a list of code table records for PDR (BigQuery)
    :param _pk_ids:  List of primary key values for RDR code table
    """
    w_dao = BigQuerySyncDao()
    gen = BQCodeGenerator()
    with w_dao.session() as w_session:
        for pk_id in _pk_ids:
            bqr = gen.make_bqrecord(pk_id)
            gen.save_bqrecord(pk_id, bqr, bqtable=BQCode, w_dao=w_dao, w_session=w_session)

@task_auth_required
def rebuild_bq_codebook_task():
    """
    Cloud Tasks: Generate all new Codebook records for BQ.
    """
    ro_dao = BigQuerySyncDao(backup=True)
    with ro_dao.session() as ro_session:
        results = ro_session.query(Code.codeId).all()
        if len(results):
            id_list = [r.codeId for r in results]
            logging.info('Code table: rebuilding {0} records...'.format(len(id_list)))
            rebuild_bq_code_ids(id_list)
