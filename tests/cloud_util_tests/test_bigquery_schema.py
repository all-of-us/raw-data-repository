import datetime
import json
import unittest
from enum import Enum

from rdr_service.model.bq_base import BQField, BQFieldModeEnum, BQFieldTypeEnum, BQRecord, BQRecordField, BQSchema, \
    BQTable
from rdr_service.model.bq_questionnaires import BQPDRTheBasicsSchema
from tests.test_data import data_path
from tests.helpers.unittest_base import BaseTestCase

class BQTestEnum(Enum):
    FIRST = 1
    SECOND = 2
    THIRD = 3


class BQTestNestedSchema(BQSchema):

    int_field = BQField("int_field", BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    str_field = BQField("str_field", BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    enum_field = BQField("enum_field", BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE, fld_enum=BQTestEnum)


class BQTestSchema(BQSchema):

    descr = BQField("descr", BQFieldTypeEnum.STRING, BQFieldModeEnum.REQUIRED)
    timestamp = BQField("timestamp", BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    nested = BQRecordField("nested", BQTestNestedSchema)


# Simulated schema received from BQ.  Keep identical to BQTestSchema.
schemaFromBQ = [
    {"name": "descr", "type": "STRING", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "DATETIME", "mode": "REQUIRED"},
    {
        "name": "nested",
        "type": "RECORD",
        "mode": "REPEATED",
        "description": "..tests.cloud_util_tests.bigquery_schema_test.BQTestNestedSchema",
        "fields": [
            {"name": "int_field", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "str_field", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "enum_field",
                "type": "INTEGER",
                "mode": "NULLABLE",
                "description": "..tests.cloud_util_tests.bigquery_schema_test.BQTestEnum",
            },
        ],
    },
]


class BQTestTable(BQTable):
    __tablename__ = "test_table"
    __schema__ = BQTestSchema


class BigQuerySchemaTest(unittest.TestCase):
    """ test BigQuery schema structures """

    def test_table_name(self):
        table = BQTestTable()
        name = table.get_name()
        self.assertEqual(name, "test_table")

    def test_schema_from_table_schema(self):
        """ test that we can dump the schema and make an identical schema from the dump """
        table = BQTestTable()
        schema = table.get_schema()

        struct_str = schema.to_json()
        new_schema = BQSchema(json.loads(struct_str))

        self.assertEqual(schema, new_schema)

    @unittest.skip('Schema comparison operation needs to be more robust.')
    def test_schema_from_dict(self):
        """ test we can take a list of fields definitions and make an identical schema """
        table = BQTestTable()
        schema = table.get_schema()

        new_schema = BQSchema(schemaFromBQ)
        self.assertEqual(schema, new_schema)

    def test_schema_getitem(self):
        """ test getting a BQField/BQRecordField object from schema """
        schema = BQTestSchema()
        field = schema["timestamp"]
        self.assertEqual(field, BQTestSchema.timestamp)


class BigQueryRecordTest(unittest.TestCase):
    """ test BigQuery schema data """

    partial_data = {"descr": "str_field data", "timestamp": datetime.datetime.utcnow()}

    full_data = {
        "descr": "str_field data",
        "timestamp": datetime.datetime.utcnow(),
        "nested": [
            {"int_field": 10, "str_field": "first string data", "enum_field": BQTestEnum.FIRST},
            {"int_field": 20, "str_field": "second string data", "enum_field": BQTestEnum.SECOND},
            {"int_field": 30, "str_field": "third string data", "enum_field": BQTestEnum.THIRD},
        ],
    }

    bq_data = {
        "descr": "str_field data",
        "timestamp": "2019-06-26T19:26:42.015372",
        "nested": [
            {"int_field": 10, "enum_field": 1, "str_field": "first string data"},
            {"int_field": 20, "enum_field": 2, "str_field": "second string data"},
            {"int_field": 30, "enum_field": 3, "str_field": "third string data"},
        ],
    }

    def test_schema_no_data(self):
        """ test a BQRecord object with only schema """
        record = BQRecord(schema=BQTestSchema, data=None)
        # add partial data
        record.update_values(self.partial_data)
        self.assertEqual(self.partial_data, record.to_dict())

    def test_schema_with_data(self):
        """ test a BQRecord object with schema and data """
        record = BQRecord(schema=BQTestSchema, data=self.partial_data)
        self.assertEqual(self.partial_data, record.to_dict())

    def test_schema_nested_data(self):
        """ test a BQRecord object with schema and nested data """
        record = BQRecord(schema=BQTestSchema, data=self.full_data, convert_to_enum=False)
        new_data = record.to_dict()

        self.assertEqual(self.full_data, new_data)
        # alter some data and verify we are not equal anymore.
        new_data["nested"][0]["int_field"] = 55
        self.assertNotEqual(self.full_data, new_data)

    @unittest.skip("remove when value casting and constraint enforcement are in bq_base.BQRecord.update_values()")
    def test_record_from_bq_data(self):
        """ test receiving data from bigquery """
        schema = BQSchema(schemaFromBQ)
        record = BQRecord(schema=schema, data=self.bq_data)
        new_data = record.to_dict()
        self.assertEqual(self.full_data, new_data)

    def test_schema_sql_field_list(self):
        """ Test generating a sql field list from schema """
        sql_fields = BQTestSchema.get_sql_field_names()
        self.assertEqual('descr, timestamp, nested', sql_fields)

        sql_fields = BQTestSchema.get_sql_field_names(exclude_fields=['timestamp'])
        self.assertEqual('descr, nested', sql_fields)


def _questionnaire_response_url(participant_id):
    return "Participant/%s/QuestionnaireResponse" % participant_id

class BigQuerySchemaDataTest(BaseTestCase):

    participant_id = None

    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super().setUp(with_data=with_data, with_consent_codes=with_consent_codes)

        # Create a participant.
        self.participant_id = self.create_participant()
        self.send_consent(self.participant_id)

        # Load TheBasics questionnaire.
        questionnaire_id = self.create_questionnaire("questionnaire_the_basics.json")
        with open(data_path("questionnaire_the_basics_resp.json")) as f:
            resource = json.load(f)

        # Submit a TheBasics questionnaire response for the participant.
        resource["subject"]["reference"] = resource["subject"]["reference"].format(participant_id=self.participant_id)
        resource["questionnaire"]["reference"] = resource["questionnaire"]["reference"].format(
            questionnaire_id=questionnaire_id
        )
        resource["authored"] = datetime.datetime.now().isoformat()
        self.send_post(_questionnaire_response_url(self.participant_id), resource)

    def test_included_fields_from_the_basics(self):
        """ Ensure that when we generate a schema the included fields are in it."""

        schema = BQPDRTheBasicsSchema()
        schema_fields = schema.get_fields()

        # Build a simple list of field names in the schema.
        fields = list()
        for schema_field in schema_fields:
            fields.append(schema_field['name'])

        included_fields = ['id', 'created', 'modified', 'authored', 'language', 'participant_id',
                           'questionnaire_response_id', 'Race_WhatRaceEthnicity']

        for included_field in included_fields:
            self.assertIn(included_field, fields)

    def test_excluded_fields_from_the_basics(self):
        """ Ensure that when we generate a schema the excluded fields are not in it."""
        schema = BQPDRTheBasicsSchema()
        schema_fields = schema.get_fields()

        # Build a simple list of field names in the schema.
        fields = list()
        for schema_field in schema_fields:
            fields.append(schema_field['name'])

        for excluded_field in schema._excluded_fields:
            self.assertNotIn(excluded_field, fields)


    # TODO: Future: Test REQUIRED/NULLABLE BQ constraints when combining schema and data.
