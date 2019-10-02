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

  def make_bqrecord(self, organization_id, convert_to_enum=False):
    """
    Build a BQRecord object from the given organization id.
    :param organization_id: Primary key value from the organization table.
    :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
    :return: BQRecord object
    """
    dao = BigQuerySyncDao()
    with dao.session() as session:
      row = session.execute(
        text('select * from rdr.organization where organization_id = :id'), {'id': organization_id}).first()
      data = dao.to_dict(row)
      return BQRecord(schema=BQOrganizationSchema, data=data, convert_to_enum=convert_to_enum)


def bq_organization_update(project_id=None):
  """
  Generate all new Organization records for BQ. Since there is called from a tool, this is not deferred.
  :param project_id: Override the project_id
  """
  dao = BigQuerySyncDao()
  with dao.session() as session:
    gen = BQOrganizationGenerator()
    results = session.query(Organization.organizationId).all()
    logging.info('BQ Organization table: rebuilding {0} records...'.format(len(results)))

    for row in results:
      bqr = gen.make_bqrecord(row.organizationId)
      gen.save_bqrecord(row.organizationId, bqr, bqtable=BQOrganization, dao=dao, session=session,
                        project_id=project_id)