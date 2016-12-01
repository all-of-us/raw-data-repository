"""Pipeline that populates BiobankSamples for participants

This is a mapreduce that reads CSV data uploaded to a GCS bucket and generates
BiobankSamples entries under participants in datastore.
"""

import ast
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
  'storageStatus',
  'type',
  'testCode',
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
    "'External Participant Id'", "'Sample Family Id'", "'Sample Id'",
    "'Sample Storage Status'", "'Sample Type'", "'Test Code'",
    "'Sample Treatments'", "'Parent Expected Volume'", "'Sample Quantity'",
    "'Sample Container Type'", "'Sample Family Collection Date'",
    "'Sample Disposal Status'", "'Sample Disposed Date'", "'Parent Sample Id'",
    "'Sample Confirmed Date'"
]

class BiobankSamplesPipeline(pipeline.Pipeline):

  def run(self, *args, **kwargs):
    bucket_name = args[0]
    newest_filename = None
    newest_timestamp = 0
    for gcs_file in cloudstorage_api.listbucket('/' + bucket_name):
      if gcs_file.filename.lower().endswith(
          ".csv") and gcs_file.st_ctime > newest_timestamp:
        newest_filename = gcs_file.filename.split('/')[2]
        newest_timestamp = gcs_file.st_ctime

    if not newest_filename:
      print 'No CSV files found in bucket {}; aborting pipeline.'.format(
          bucket_name)
      return

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
  reader = csv.DictReader(buffer, delimiter='\t')
  headers = set(reader.fieldnames)
  expected_headers_set = set(EXPECTED_HEADERS)
  missing_headers = expected_headers_set - headers
  if len(missing_headers) > 0:
    print 'Missing headers: {}; aborting.'.format(missing_headers)
    return
  else:
    extra_headers = headers - expected_headers_set
    if len(extra_headers) > 0:
      print 'Warning -- unexpected extra headers: {}'.format(extra_headers)
  for dict in reader:
    participant_id = dict.get("'External Participant Id'").strip("'");
    if participant_id:
      values = []
      for header in EXPECTED_HEADERS:
        if header != "'External Participant Id'":
          values.append(dict.get(header).strip("'"));
      yield (participant_id, values)

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
