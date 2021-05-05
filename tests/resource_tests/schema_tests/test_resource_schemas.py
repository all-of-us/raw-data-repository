#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#

from tests.helpers.unittest_base import BaseTestCase
# from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum
from rdr_service.model.bq_code import BQCodeSchema
from rdr_service.resource.schemas import CodeSchema

class ResourceSchemaTest(BaseTestCase):
    """
    Test that the resource schema definitions/fields align with the BigQuery schemas
    """
    def setup(self):
        super().setup()

    def test_code_resource_schema(self):

        bq_code_schema = BQCodeSchema().get_fields()
        rsc_code_schema = CodeSchema()
        self.assertIsNotNone(bq_code_schema)
        self.assertIsNotNone(rsc_code_schema)




