import os
from google.cloud import bigquery

default_rdr_ods_tables = [
    {"table_name": "data_element",
     "fields": [
         bigquery.SchemaField('id', 'STRING', 'NULLABLE', 'UUID created at insert', ()),
         bigquery.SchemaField('source_system', 'STRING', 'NULLABLE',
                              'data element source system, i.e. rdr_genomic, rdr_survey.',
                              ()),
         bigquery.SchemaField('source_target', 'STRING', 'NULLABLE',
                              'data element source object and attribute in source system, '
                              'i.e. GenomicSetMember.qcStatus, ParticipantSummary.consent_for_study_enrollment, etc.',
                              ()),
         bigquery.SchemaField('value_datatype', 'STRING', 'NULLABLE', 'datatype of source data element.', ()),
     ]},

    {"table_name": "data_element_registry",
     "fields": [
         bigquery.SchemaField('data_element_id', 'STRING', 'NULLABLE', 'UUID from rdr_ods.data_element', ()),
         bigquery.SchemaField('active_flag', 'BOOLEAN', 'NULLABLE', 'on-off switch for record.', ()),
         bigquery.SchemaField('target_table', 'STRING', 'NULLABLE', 'rdr_ods table to receive data for data element.',
                              ()),
         bigquery.SchemaField('normalization_rule', 'STRING', 'REPEATED',
                              'Array of string values that represent a normalization rule.', ()),
     ]},

    {"table_name": "export_schema",
     "fields": [
         bigquery.SchemaField('schema_name', 'STRING', 'NULLABLE', 'name of export schema record belongs to.', ()),
         bigquery.SchemaField('destination_mart', 'STRING', 'NULLABLE', 'name of the destination data mart', ()),
         bigquery.SchemaField('destination_target_table', 'STRING', 'NULLABLE',
                              'Table this field is added to in the data mart. '
                              'Based on the snapshot naming convention',
                              ()),
     ]},

    {"table_name": "export_schema_data_element",
     "fields": [
         bigquery.SchemaField('schema_name', 'STRING', 'NULLABLE', 'name of schema', ()),
         bigquery.SchemaField('data_element_id', 'STRING', 'NULLABLE', 'UUID of data element', ()),
         bigquery.SchemaField('display_name', 'STRING', 'NULLABLE', 'Output field name in data mart', ()),
         bigquery.SchemaField('active_flag', 'BOOLEAN', 'NULLABLE', 'On-Off switch for inclusion in export', ()),
     ]},

    {"table_name": "participant_survey_data_element",
     "fields": [
         bigquery.SchemaField('participant_id', 'STRING', 'NULLABLE', 'participant_id from RDR.', ()),
         bigquery.SchemaField('research_id', 'STRING', 'NULLABLE', 'research_id from RDR.', ()),
         bigquery.SchemaField(
             'data_element_id', 'STRING', 'NULLABLE', 'data_element_id from rdr_ods.data_element', ()),
         bigquery.SchemaField('value_string', 'STRING', 'NULLABLE', 'value of of data element', ()),
         bigquery.SchemaField('created_timestamp', 'TIMESTAMP', 'NULLABLE', 'timestamp of record insertion', ()),
         bigquery.SchemaField('authored_timestamp', 'TIMESTAMP', 'NULLABLE',
                              'timestamp from questionnaire_response.authored', ()),
     ]},
    {"table_name": "participant_consent_data_element",
     "fields": [
         bigquery.SchemaField('participant_id', 'STRING', 'NULLABLE', 'participant_id from RDR.', ()),
         bigquery.SchemaField('research_id', 'STRING', 'NULLABLE', 'research_id from RDR.', ()),
         bigquery.SchemaField(
             'data_element_id', 'STRING', 'NULLABLE', 'data_element_id from rdr_ods.data_element', ()),
         bigquery.SchemaField('value_string', 'STRING', 'NULLABLE', 'value of of data element', ()),
         bigquery.SchemaField('created_timestamp', 'TIMESTAMP', 'NULLABLE', 'timestamp of record insertion', ()),
         bigquery.SchemaField('authored_timestamp', 'TIMESTAMP', 'NULLABLE',
                              'timestamp from consent authored field', ()),
     ]},
    {"table_name": "sample_data_element",
     "fields": [
         bigquery.SchemaField('sample_id', 'STRING', 'NULLABLE',
                              'sample_id from RDR genomics pipeline (genomic_set_member)', ()),
         bigquery.SchemaField('participant_id', 'STRING', 'NULLABLE', 'participant_id from RDR.', ()),
         bigquery.SchemaField('research_id', 'STRING', 'NULLABLE', 'research_id from RDR.', ()),
         bigquery.SchemaField(
             'data_element_id', 'STRING', 'NULLABLE', 'data_element_id from rdr_ods.data_element', ()),
         bigquery.SchemaField('value_string', 'STRING', 'NULLABLE', 'value of of data element.', ()),
         bigquery.SchemaField('created_timestamp', 'TIMESTAMP', 'NULLABLE', 'timestamp of record insertion.', ()),
         bigquery.SchemaField('genome_type', 'STRING', 'NULLABLE', 'genome_type from RDR for sample_id', ()),
     ]}
]
data_file_path = os.path.join(os.path.dirname(__file__), "data")
default_rdr_ods_table_data = [
    {
        "table_name": "data_element",
        "data": os.path.join(data_file_path, "data_element.json")
    },
    {
        "table_name": "data_element_registry",
        "data": os.path.join(data_file_path, "data_element_registry.json")
    },
    {
        "table_name": "export_schema",
        "data": os.path.join(data_file_path, "export_schema.json")
    },
    {
        "table_name": "export_schema_data_element",
        "data": os.path.join(data_file_path, "export_schema_data_element_full.json")
    },
]
