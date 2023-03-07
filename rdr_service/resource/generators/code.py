#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from sqlalchemy.sql import text

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.code import Code
from rdr_service.resource import generators, schemas


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

def rebuild_codebook_resources(_pk_ids):
    """
    Build
    """
    gen = CodeGenerator()
    for pk_id in _pk_ids:
        res = gen.make_resource(pk_id)
        res.save()


def rebuild_codebook_resources_task():
    """
    Cloud Tasks: Generate all new Codebook resource records.
    """
    ro_dao = ResourceDataDao(backup=True)
    with ro_dao.session() as ro_session:
        results = ro_session.query(Code.codeId).all()
        if len(results):
            logging.info('Code table: rebuilding {0} resource records...'.format(len(results)))
            id_list = [r.codeId for r in results]
            rebuild_codebook_resources(id_list)
