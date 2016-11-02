"""Pipeline that populates BiobankSamples for participants

This is a mapreduce that reads CSV data uploaded to a GCS bucket and generates
BiobankSamples entries under participants in datastore.
"""

import ast
import api_util
import biobank_sample
import csv
import config
import pipeline
from cloudstorage import cloudstorage_api

from mapreduce import mapreduce_pipeline
from mapreduce.lib.input_reader._gcs import GCSInputReader

BIOBANK_SAMPLE_FIELDS = [
  'family_id',
  'sample_id',
  'event_name',
  'storage_status',
  'type',
  'treatments',
  'expected_volume',
  'quantity',
  'container_type',
  'collection_date',
  'disposal_status',
  'disposed_date',
  'parent_sample_id',
  'confirmed_date'
]

class BiobankSamplesPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME,
                                    'pmi-drc-biobank-test.appspot.com')
        
    newest_filename = None
    newest_timestamp = 0
    for gcs_file in cloudstorage_api.listbucket('/' + bucket_name):
      if gcs_file.filename.endswith(".csv") and gcs_file.st_ctime > newest_timestamp:
        newest_filename = gcs_file.filename.split('/')[2]
        newest_timestamp = gcs_file.st_ctime
    
    if not newest_filename:
      print 'No CSV files found in bucket {}; aborting pipeline.'.format(bucket_name)
                            
    print '======= Starting Biobank Samples Pipeline with file {} in bucket {}'.format(newest_filename, bucket_name)
    mapper_params = {
        'input_reader': {
           GCSInputReader.BUCKET_NAME_PARAM: bucket_name,
           GCSInputReader.OBJECT_NAMES_PARAM: [ newest_filename ] 
        }             
    }
    num_shards = int(config.getSetting(config.BIOBANK_SAMPLES_SHARDS, 1))
    #The result of yield is a future that will contain the files that were
    #produced by MapreducePipeline.
    yield mapreduce_pipeline.MapreducePipeline(
        'Import Biobank Samples',
        mapper_spec='offline.biobank_samples_pipeline.map_samples',
        input_reader_spec='mapreduce.input_readers.GoogleCloudStorageInputReader',
        mapper_params=mapper_params,
        reducer_spec='offline.biobank_samples_pipeline.reduce_samples',
        shards=num_shards)

def map_samples(buffer):
  reader = csv.reader(buffer)
  # Skip the header row
  reader.next()
  for row in reader: 
    yield (row[0].strip("'"), row[1:])    
  
def reduce_samples(participant_id, samples):
  # Are participant IDs actually going to start with 'B'?
  # TODO: fetch existing samples, don't write when nothing changes  
  sample_dicts = []
  for sample in samples:
    sample_arr = ast.literal_eval(sample)
    sample_dict = {}
    i = 0
    for sample_value in sample_arr:      
      stripped_value = sample_value.strip("'")
      if stripped_value:
        sample_dict[BIOBANK_SAMPLE_FIELDS[i]] = stripped_value        
      i = i + 1
    sample_dicts.append(sample_dict)
  biobank_samples_dict = { 'samples': sample_dicts }  
  biobank_samples = biobank_sample.DAO.from_json(biobank_samples_dict,
                                                 participant_id, "0")
  biobank_sample.DAO.store(biobank_samples)  
