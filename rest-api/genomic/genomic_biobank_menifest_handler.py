"""
Create a genomic biobank manifest CSV file and uploads to biobank samples bucket subfolders.
"""

import clock
import config
import pytz
from offline.sql_exporter import SqlExporter
from code_constants import GENOME_TYPE
from config import GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME

_US_CENTRAL = pytz.timezone('US/Central')
_UTC = pytz.utc
OUTPUT_CSV_TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
_MANIFEST_FILE_NAME_ARRAY_PREFIX = 'Genomic-Manifest-AoU_Array'
_MANIFEST_FILE_NAME_WGS_PREFIX = 'Genomic-Manifest-AoU_WGS'

def create_and_upload_genomic_biobank_manifest_file(genomic_set_id, genome_type, timestamp=None):
  result_filename = _get_output_manifest_file_name(genomic_set_id, genome_type, timestamp)
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
  exporter = SqlExporter(bucket_name)
  export_sql = """
      SELECT 
        biobank_order_id,
        sex_at_birth,
        genome_type,
        ny_flag,
        '' AS request_id,
        '' AS sample_storage_retrival_status,
        '' AS sample_storage_retrival_timestamp,
        '' AS sample_storage_retrival_comment,
        '' AS sample_suitability_status,
        '' AS sample_suitability_timestamp,
        '' AS sample_suitability_comment,
        '' AS sample_plated_status,
        '' AS sample_plated_timestamp,
        '' AS sample_plated_comment
      FROM genomic_set_member
      WHERE genomic_set_id=:genomic_set_id AND genome_type=:genome_type
      ORDER BY id
    """
  query_params = {'genomic_set_id': genomic_set_id, 'genome_type': genome_type}
  exporter.run_export(result_filename, export_sql, query_params)

def _get_output_manifest_file_name(genomic_set_id, genome_type, timestamp=None):
  file_timestamp = timestamp if timestamp else clock.CLOCK.now()
  now_cdt_str = _UTC.localize(file_timestamp).astimezone(_US_CENTRAL).replace(tzinfo=None)\
    .strftime(OUTPUT_CSV_TIME_FORMAT)
  folder_name = config.getSetting(GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME)
  if genome_type == GENOME_TYPE[0]:
    return folder_name + '/' + _MANIFEST_FILE_NAME_ARRAY_PREFIX + '-' + \
           str(genomic_set_id) + '-v1' + now_cdt_str + '.CSV'
  elif genome_type == GENOME_TYPE[1]:
    return folder_name + '/' + _MANIFEST_FILE_NAME_WGS_PREFIX + '-' + \
           str(genomic_set_id) + '-v1' + now_cdt_str + '.CSV'
  else:
    raise ValueError('invalid value for genome_type parameter')

