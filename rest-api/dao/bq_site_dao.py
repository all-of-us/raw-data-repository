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

  def make_bqrecord(self, site_id, convert_to_enum=False, backup=True):
    """
    Build a BQRecord object from the given site id.
    :param site_id: Primary key value from site table.
    :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
    :param backup: if True, get from backup database
    :return: BQRecord object
    """
    ro_dao = BigQuerySyncDao(backup=backup)
    with ro_dao.session() as ro_session:
      row = ro_session.execute(text('select * from site where site_id = :id'), {'id': site_id}).first()
      data = ro_dao.to_dict(row)
      return BQRecord(schema=BQSiteSchema, data=data, convert_to_enum=convert_to_enum)


def bq_site_update(project_id=None):
  """
  Generate all new Site records for BQ. Since there is called from a tool, this is not deferred.
  :param project_id: Override the project_id
  """
  ro_dao = BigQuerySyncDao(backup=True)
  with ro_dao.session() as ro_session:
    gen = BQSiteGenerator()
    results = ro_session.query(Site.siteId).all()

  w_dao = BigQuerySyncDao()
  with w_dao.session() as w_session:
    logging.info('BQ Site table: rebuilding {0} records...'.format(len(results)))
    for row in results:
      bqr = gen.make_bqrecord(row.siteId)
      gen.save_bqrecord(row.siteId, bqr, bqtable=BQSite, w_dao=w_dao, w_session=w_session, project_id=project_id)


def bq_site_update_by_id(site_id):
  gen = BQSiteGenerator()
  bqr = gen.make_bqrecord(site_id, backup=False)
  w_dao = BigQuerySyncDao()
  with w_dao.session() as w_session:
    gen.save_bqrecord(site_id, bqr, bqtable=BQSite, w_dao=w_dao, w_session=w_session)
