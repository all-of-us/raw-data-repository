import logging

from sqlalchemy.sql import text

from dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from model.bq_base import BQRecord
from model.bq_hpo import BQHPOSchema, BQHPO
from model.hpo import HPO


class BQHPOGenerator(BigQueryGenerator):
  """
  Generate a HPO BQRecord object
  """

  def make_bqrecord(self, hpo_id, convert_to_enum=False):
    """
    Build a BQRecord object from the given hpo id.
    :param hpo_id: Primary key value from hpo table.
    :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
    :return: BQRecord object
    """
    dao = BigQuerySyncDao()
    with dao.session() as session:
      row = session.execute(text('select * from hpo where hpo_id = :id'), {'id': hpo_id}).first()
      data = dao.to_dict(row)
      return BQRecord(schema=BQHPOSchema, data=data, convert_to_enum=convert_to_enum)

def bq_hpo_update():
  """
  Generate all new HPO records for BQ. Since there is called from a tool, this is not deferred.
  """
  dao = BigQuerySyncDao()
  with dao.session() as session:
    gen = BQHPOGenerator()
    results = session.query(HPO.hpoId).all()

    logging.info('BQ HPO table: rebuilding {0} records...'.format(len(results)))
    for row in results:
      bqr = gen.make_bqrecord(row.hpoId)
      gen.save_bqrecord(row.hpoId, bqr, bqtable=BQHPO, dao=dao, session=session)