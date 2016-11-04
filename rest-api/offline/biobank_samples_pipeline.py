"""Pipeline that populates BiobankSamples for participants

This is a mapreduce that reads CSV data uploaded to a GCS bucket and generates
BiobankSamples entries under participants in datastore.
"""

import ast
import api_util
import biobank_sample
import csv
import config
import participant
import pipeline
from cloudstorage import cloudstorage_api

from mapreduce import mapreduce_pipeline
from mapreduce.lib.input_reader._gcs import GCSInputReader
from mapreduce import operation as op

BIOBANK_SAMPLE_FIELDS = [
  'familyId',
  'sampleId',
  'eventName',
  'storageStatus',
  'type',
  'treatments',
  'expectedVolume',
  'quantity',
  'containerType',
  'collectionDate',
  'disposalStatus',
  'disposedDate',
  'parentSampleId',
  'confirmedDate'
]

EXPECTED_HEADERS = [
    'External Participant Id', 'Sample Family Id', 'Sample Id',
    'Participant Event Name', 'Sample Storage Status', 'Sample Type',
    'Sample Treatments', 'Parent Expected Volume', 'Sample Quantity',
    'Sample Container Type', 'Sample Family Collection Date',
    'Sample Disposal Status', 'Sample Disposed Date', 'Parent Sample Id',
    'Sample Confirmed Date'
]

class BiobankSamplesPipeline(pipeline.Pipeline):

  def run(self, *args, **kwargs):
    bucket_name = args[0]
    newest_filename = None
    newest_timestamp = 0
    for gcs_file in cloudstorage_api.listbucket('/' + bucket_name):
      if gcs_file.filename.endswith(
          ".csv") and gcs_file.st_ctime > newest_timestamp:
        newest_filename = gcs_file.filename.split('/')[2]
        newest_timestamp = gcs_file.st_ctime

    if not newest_filename:
      print 'No CSV files found in bucket {}; aborting pipeline.'.format(
          bucket_name)

    print '======= Starting Biobank Samples Pipeline with file {} in bucket {}'.format(
        newest_filename, bucket_name)
    mapper_params = {
        'input_reader': {
            GCSInputReader.BUCKET_NAME_PARAM: bucket_name,
            GCSInputReader.OBJECT_NAMES_PARAM: [newest_filename]
        }
    }
    num_shards = int(config.getSetting(config.BIOBANK_SAMPLES_SHARDS, 1))
    # The result of yield is a future that will contain the files that were
    # produced by MapreducePipeline.
    # Note that GoogleCloudStorageInputReader uses only one shard for reading
    # the input CSV file. If this becomes too slow in future, we could consider
    # dividing the file into pieces before processing.
    yield mapreduce_pipeline.MapreducePipeline(
        'Import Biobank Samples',
        mapper_spec='offline.biobank_samples_pipeline.map_samples',
        input_reader_spec='mapreduce.input_readers.GoogleCloudStorageInputReader',
        mapper_params=mapper_params,
        reducer_spec='offline.biobank_samples_pipeline.reduce_samples',
        shards=num_shards)

def map_samples(buffer):
  reader = csv.reader(buffer)
  header_row = reader.next()
  trimmed_header_row = []
  for header in header_row:
    trimmed_header_row.append(header.strip("'"))
  if trimmed_header_row != EXPECTED_HEADERS:
    print 'Unexpected headers: {}; aborting.'.format(trimmed_header_row)
    return
  for row in reader:
    yield (row[0].strip("'"), row[1:])

def reduce_samples(biobank_id, samples):
  # TODO: fetch existing samples, don't write when nothing changes
  sample_dicts = []
  participant_id = participant.DAO.find_participant_id_by_biobank_id(biobank_id)
  if not participant_id:
    print 'Participant with biobank ID {} not found; skipping.'.format(
        biobank_id)
    return

  for sample in samples:
    sample_arr = ast.literal_eval(sample)
    sample_dict = {}
    for i, sample_value in enumerate(sample_arr):
      stripped_value = sample_value.strip("'")
      if stripped_value:
        sample_dict[BIOBANK_SAMPLE_FIELDS[i]] = stripped_value
    sample_dicts.append(sample_dict)
  biobank_samples_dict = {'samples': sample_dicts}
  biobank_samples = biobank_sample.DAO.from_json(biobank_samples_dict,
                                                 participant_id,
                                                 biobank_sample.SINGLETON_SAMPLES_ID)
  yield op.db.Put(biobank_samples)
