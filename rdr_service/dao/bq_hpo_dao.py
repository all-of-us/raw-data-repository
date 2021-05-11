import logging

from sqlalchemy.sql import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_hpo import BQHPOSchema, BQHPO, BQOrganizationTypeEnum, BQObsoleteStatusEnum
from rdr_service.model.hpo import HPO



class BQHPOGenerator(BigQueryGenerator):
    """
    Generate a HPO BQRecord object
    """

    def make_bqrecord(self, hpo_id, convert_to_enum=False, backup=True):
        """
        Build a BQRecord object from the given hpo id.
        :param hpo_id: Primary key value from hpo table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        ro_dao = BigQuerySyncDao(backup=backup)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from hpo where hpo_id = :id'), {'id': hpo_id}).first()
            data = ro_dao.to_dict(row)
            data['is_obsolete_id'] = int(BQObsoleteStatusEnum(data['is_obsolete']))
            data['is_obsolete'] = str(BQObsoleteStatusEnum(data['is_obsolete_id']))
            data['organization_type_id'] = int(BQOrganizationTypeEnum(data['organization_type']))
            data['organization_type'] = str(BQOrganizationTypeEnum(data['organization_type_id']))
            return BQRecord(schema=BQHPOSchema, data=data, convert_to_enum=convert_to_enum)


def bq_hpo_update(project_id=None):
    """
    Generate all new HPO records for BQ. Since there is called from a tool, this is not deferred.
    :param project_id: Override the project_id
    """
    ro_dao = BigQuerySyncDao(backup=True)
    with ro_dao.session() as ro_session:
        gen = BQHPOGenerator()
        results = ro_session.query(HPO.hpoId).all()

    w_dao = BigQuerySyncDao()
    logging.info('BQ HPO table: rebuilding {0} records...'.format(len(results)))
    with w_dao.session() as w_session:
        for row in results:
            bqr = gen.make_bqrecord(row.hpoId)
            gen.save_bqrecord(row.hpoId, bqr, bqtable=BQHPO, w_dao=w_dao, w_session=w_session, project_id=project_id)


def bq_hpo_update_by_id(hpo_id):
    gen = BQHPOGenerator()
    # get from main database in case the backup is not synch in time
    bqr = gen.make_bqrecord(hpo_id, backup=False)
    w_dao = BigQuerySyncDao()
    with w_dao.session() as w_session:
        gen.save_bqrecord(hpo_id, bqr, bqtable=BQHPO, w_dao=w_dao, w_session=w_session)
