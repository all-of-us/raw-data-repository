#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# import logging

from sqlalchemy.sql import text

# from rdr_service.app_util import task_auth_required
from rdr_service.resource import generators, schemas
from rdr_service.dao.resource_dao import ResourceDataDao



# from rdr_service.model.bq_code import BQCode
# from rdr_service.model.bq_code import BQCodeSchema
# from rdr_service.model.code import Code


class CodeGenerator(generators.BaseGenerator):
    """
    Generate a Code BQRecord object
    """
    def make_resource(self, _pk):
        """
        Build a Resource object from the given code id.
        :param _pk: Primary key value from code table.
        :return: ResourceDataObject object
        """
        ro_dao = ResourceDataDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from code where code_id = :pk'), {'pk': _pk}).first()
            data = ro_dao.to_resource_dict(row, schema=schemas.CodeSchema)
            return generators.ResourceRecordSet(schemas.CodeSchema, data)

# TODO: Rewrite this as Resource code.
# @task_auth_required
# def rebuild_bq_codebook_task():
#     """
#     Cloud Tasks: Generate all new Codebook records for BQ.
#     """
#     ro_dao = BigQuerySyncDao(backup=True)
#     with ro_dao.session() as ro_session:
#         gen = BQCodeGenerator()
#         results = ro_session.query(Code.codeId).all()
#
#     w_dao = BigQuerySyncDao()
#     logging.info('Code table: rebuilding {0} records...'.format(len(results)))
#     with w_dao.session() as w_session:
#         for row in results:
#             bqr = gen.make_bqrecord(row.codeId)
#             gen.save_bqrecord(row.codeId, bqr, resource_uri=BQCode, w_dao=w_dao, w_session=w_session)
