#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID

class BiobankCovidAntibodySampleSchema(Schema):
    id = fields.Int32(required=True)
    sample_id = fields.String(validate=validate.Length(max=80), required=True)
    aou_biobank_id = fields.Int32()
    no_aou_biobank_id = fields.Int32()
    matrix_tube_id = fields.Int32()
    sample_type = fields.String(validate=validate.Length(max=80))
    quantity_ul = fields.Int32()
    storage_location = fields.String(validate=validate.Length(max=200))
    collection_date = fields.DateTime()
    ingest_file_name = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.biobank_covid_antibody_sample
        resource_uri = 'BiobankCovidAntibodySample'
        resource_pk_field = 'id'


class QuestCovidAntibodyTestSchema(Schema):
    id = fields.Int32(required=True)
    accession = fields.String(validate=validate.Length(max=80), required=True)
    specimen_id = fields.String(validate=validate.Length(max=80))
    test_code = fields.Int32()
    test_name = fields.String(validate=validate.Length(max=200))
    run_date_time = fields.DateTime()
    instrument_name = fields.String(validate=validate.Length(max=200))
    position = fields.String(validate=validate.Length(max=80))
    ingest_file_name = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.quest_covid_antibody_test
        resource_uri = 'QuestCovidAntibodyTest'
        resource_pk_field = 'id'


class QuestCovidAntibodyTestResultSchema(Schema):
    id = fields.Int32(required=True)
    accession = fields.String(validate=validate.Length(max=80), required=True)
    result_name = fields.String(validate=validate.Length(max=200))
    result_value = fields.String(validate=validate.Length(max=200))
    ingest_file_name = fields.String(validate=validate.Length(max=80))

    class Meta:
        schema_id = SchemaID.quest_covid_antibody_test_result
        resource_uri = 'QuestCovidAntibodyTestResult'
        resource_pk_field = 'id'
