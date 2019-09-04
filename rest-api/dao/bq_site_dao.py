import logging
from sqlalchemy.sql import text

from dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from model.bq_base import BQRecord
from model.bq_site import BQSiteSchema, BQSite
from model.site import Site


class BQSiteGenerator(BigQueryGenerator):
  """
  Generate a Site BQRecord object
  """

  def make_bqrecord(self, site_id, convert_to_enum=False):
    """
    Build a BQRecord object from the given site id.
    :param site_id: Primary key value from site table.
    :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
    :return: BQRecord object
    """
    dao = BigQuerySyncDao()
    with dao.session() as session:
      row = session.execute(text('select * from site where site_id = :id'), {'id': site_id}).first()
      data = dao.to_dict(row)
      return BQRecord(schema=BQSiteSchema, data=data, convert_to_enum=convert_to_enum)


def bq_site_update():
  """
  Generate all new Site records for BQ. Since there is called from a tool, this is not deferred.
  """
  dao = BigQuerySyncDao()
  with dao.session() as session:
    gen = BQSiteGenerator()
    results = session.query(Site.siteId).all()
    logging.info('BQ Site table: rebuilding {0} records...'.format(len(results)))

    for row in results:
      bqr = gen.make_bqrecord(row.siteId)
      gen.save_bqrecord(row.siteId, bqr, bqtable=BQSite, dao=dao, session=session)