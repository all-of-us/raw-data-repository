"""
Create a genomic biobank manifest CSV file and uploads to biobank samples bucket subfolders.
"""

import clock
import config
import pytz
from offline.sql_exporter import SqlExporter
from config import GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME

_US_CENTRAL = pytz.timezone('US/Central')
_UTC = pytz.utc
OUTPUT_CSV_TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
_MANIFEST_FILE_NAME_PREFIX = 'Genomic-Manifest-AoU'


def create_and_upload_genomic_biobank_manifest_file(genomic_set_id, timestamp=None):
  result_filename = _get_output_manifest_file_name(genomic_set_id, timestamp)
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
  exporter = SqlExporter(bucket_name)
  export_sql = """
      SELECT 
        biobank_order_client_Id as value,
        biobank_id,
        sex_at_birth,
        genome_type,
        CASE
          WHEN ny_flag IS TRUE THEN 'Y' ELSE 'N'
        END AS ny_flag,
        '' AS request_id,
        '' AS package_id
      FROM genomic_set_member
      WHERE genomic_set_id=:genomic_set_id
      ORDER BY id
    """
  query_params = {'genomic_set_id': genomic_set_id}
  exporter.run_export(result_filename, export_sql, query_params)


def _get_output_manifest_file_name(genomic_set_id, timestamp=None):
  file_timestamp = timestamp if timestamp else clock.CLOCK.now()
  now_cdt_str = _UTC.localize(file_timestamp).astimezone(_US_CENTRAL).replace(tzinfo=None)\
    .strftime(OUTPUT_CSV_TIME_FORMAT)
  folder_name = config.getSetting(GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME)
  return folder_name + '/' + _MANIFEST_FILE_NAME_PREFIX + '-' + str(genomic_set_id) + '-v1' + \
         now_cdt_str + '.CSV'

