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
    dao = BigQuerySyncDao()
    with dao.session() as session:
      row = session.execute(text('select * from code where code_id = :id'), {'id': code_id}).first()
      data = dao.to_dict(row)
      return BQRecord(schema=BQCodeSchema, data=data, convert_to_enum=convert_to_enum)


def deferrered_bq_codebook_update():
  """
  Generate all new Codebook records for BQ.
  """
  dao = BigQuerySyncDao()
  with dao.session() as session:
    gen = BQCodeGenerator()
    results = session.query(Code.codeId).all()

    logging.info('Code table: rebuilding {0} records...'.format(len(results)))
    for row in results:
      bqr = gen.make_bqrecord(row.codeId)
      gen.save_bqrecord(row.codeId, bqr, bqtable=BQCode, dao=dao, session=session)