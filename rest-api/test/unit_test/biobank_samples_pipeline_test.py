"""Tests for biobank_samples_pipeline."""

import csv
import os
import biobank_sample
import participant

from offline.biobank_samples_pipeline import BiobankSamplesPipeline

from cloudstorage import cloudstorage_api
from google.appengine.ext import ndb
from mapreduce import test_support
from testlib import testutil
from test.unit_test.unit_test_util import to_dict_strip_last_modified

class BiobankSamplesPipelineTest(testutil.CloudStorageTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    ndb.get_context().set_cache_policy(False)


  def test_end_to_end(self):
    # Insert participants to generate biobank IDs
    participant.DAO.insert(participant.DAO.from_json({}, None, 'P1'))
    participant_1 = participant.DAO.load('P1')
    participant.DAO.insert(participant.DAO.from_json({}, None, 'P2'))
    participant_2 = participant.DAO.load('P2')

    with open(_data_path('biobank_samples_1.csv'), 'rb') as src, \
        cloudstorage_api.open('/pmi-drc-biobank-test.appspot.com/biobank_samples_1.CSV', mode='w') as dest:
      reader = csv.reader(src, delimiter='\t')
      writer = csv.writer(dest, delimiter='\t')
      header_row = reader.next()
      participant_id_index = header_row.index('External Participant Id')
      writer.writerow(header_row)
      for line in reader:
        # Put biobank IDs in the CSV being imported
        line[participant_id_index] = line[participant_id_index].replace("{biobank_id_1}", participant_1.biobankId)
        line[participant_id_index] = line[participant_id_index].replace("{biobank_id_2}", participant_2.biobankId);
        writer.writerow(line)
    BiobankSamplesPipeline('pmi-drc-biobank-test.appspot.com').start()
    test_support.execute_until_empty(self.taskqueue)

    biobank_samples_1 = biobank_sample.DAO.load(biobank_sample.SINGLETON_SAMPLES_ID, 'P1')
    expected_sample_dict_1 = {
        'familyId': 'SF161129-000713',
        'sampleId': '16334002110',
        'storageStatus': 'Disposed',
        'type': 'Whole Blood',
        'testCode': '1ED10',
        'treatments': 'EDTA',
        'expectedVolume': '10 mL',
        'quantity': '10 mL',
        'containerType': 'Vacutainer Tube, 10m',
        'collectionDate': '2016/11/28 02:00:00',
        'confirmedDate': '2016/11/29 12:19:32',
        'disposalStatus': 'Accessioning Error',
        'disposedDate': '2016/11/30 12:17:33' }
    expected_samples_1 = biobank_sample.DAO.from_json(
        { 'samples': [ expected_sample_dict_1 ]},
        'P1', biobank_sample.SINGLETON_SAMPLES_ID).to_dict()
    del expected_samples_1['last_modified']
    self.assertEquals(expected_samples_1, to_dict_strip_last_modified(biobank_samples_1))

def test_end_to_end_missing_field(self):
    # Insert participants to generate biobank IDs
    participant.DAO.insert(participant.DAO.from_json({}, None, 'P1'))
    participant_1 = participant.DAO.load('P1')
    participant.DAO.insert(participant.DAO.from_json({}, None, 'P2'))
    participant_2 = participant.DAO.load('P2')

    with open(_data_path('biobank_samples_missing_field.csv'), 'rb') as src, \
        cloudstorage_api.open('/pmi-drc-biobank-test.appspot.com/biobank_samples_1.CSV', mode='w') as dest:
      reader = csv.reader(src)
      writer = csv.writer(dest)
      for line in reader:
        # Put biobank IDs in the CSV being imported
        line[0] = line[0].replace("{biobank_id_1}", participant_1.biobankId)
        line[0] = line[0].replace("{biobank_id_2}", participant_2.biobankId);
        writer.writerow(line)
    BiobankSamplesPipeline('pmi-drc-biobank-test.appspot.com').start()
    test_support.execute_until_empty(self.taskqueue)

    biobank_samples_1 = biobank_sample.DAO.load_if_present(biobank_sample.SINGLETON_SAMPLES_ID,
                                                           'P1')
    self.assertNot(biobank_samples_1)

def _data_path(filename):
  return os.path.join(os.path.dirname(__file__), '..', 'test-data', filename)
