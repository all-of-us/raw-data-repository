import logging

from sqlalchemy.sql import text

from dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from model.bq_base import BQRecord
from model.bq_code import BQCode
from model.bq_code import BQCodeSchema
from model.code import Code


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
      return BQRecord(schema=BQCodeSchema, data=data, convert_to_enum=convert_to_enum)


def deferrered_bq_codebook_update():
  """
  Generate all new Codebook records for BQ.
  """
  ro_dao = BigQuerySyncDao(backup=True)
  with ro_dao.session() as ro_session:
    gen = BQCodeGenerator()
    results = ro_session.query(Code.codeId).all()

  w_dao = BigQuerySyncDao()
  with w_dao.session() as w_session:
    logging.info('Code table: rebuilding {0} records...'.format(len(results)))
    for row in results:
      bqr = gen.make_bqrecord(row.codeId)
      gen.save_bqrecord(row.codeId, bqr, bqtable=BQCode, w_dao=w_dao, w_session=w_session)