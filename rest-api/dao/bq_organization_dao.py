import logging
from sqlalchemy.sql import text

from dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from model.bq_base import BQRecord
from model.bq_organization import BQOrganizationSchema, BQOrganization
from model.organization import Organization

class BQOrganizationGenerator(BigQueryGenerator):
  """
  Generate an Organization BQRecord object
  """

  def make_bqrecord(self, organization_id, convert_to_enum=False, backup=True):
    """
    Build a BQRecord object from the given organization id.
    :param organization_id: Primary key value from the organization table.
    :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
    :param backup: if True, get from backup database
    :return: BQRecord object
    """
    ro_dao = BigQuerySyncDao(backup=backup)
    with ro_dao.session() as ro_session:
      row = ro_session.execute(
        text('select * from organization where organization_id = :id'), {'id': organization_id}).first()
      data = ro_dao.to_dict(row)
      return BQRecord(schema=BQOrganizationSchema, data=data, convert_to_enum=convert_to_enum)


def bq_organization_update(project_id=None):
  """
  Generate all new Organization records for BQ. Since there is called from a tool, this is not deferred.
  :param project_id: Override the project_id
  """
  ro_dao = BigQuerySyncDao(backup=True)
  with ro_dao.session() as ro_session:
    gen = BQOrganizationGenerator()
    results = ro_session.query(Organization.organizationId).all()

  w_dao = BigQuerySyncDao()
  with w_dao.session() as w_session:
    logging.info('BQ Organization table: rebuilding {0} records...'.format(len(results)))
    for row in results:
      bqr = gen.make_bqrecord(row.organizationId)
      gen.save_bqrecord(row.organizationId, bqr, bqtable=BQOrganization, w_dao=w_dao, w_session=w_session,
                        project_id=project_id)


def bq_organization_update_by_id(org_id):
  gen = BQOrganizationGenerator()
  bqr = gen.make_bqrecord(org_id, backup=False)
  w_dao = BigQuerySyncDao()
  with w_dao.session() as w_session:
    gen.save_bqrecord(org_id, bqr, bqtable=BQOrganization, w_dao=w_dao, w_session=w_session)
