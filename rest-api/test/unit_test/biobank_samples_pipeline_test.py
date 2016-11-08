"""Tests for biobank_samples_pipeline."""

import csv
import os
import biobank_sample
import participant

from offline.biobank_samples_pipeline import BiobankSamplesPipeline

from cloudstorage import cloudstorage_api
from mapreduce import test_support
from testlib import testutil
from test.unit_test.unit_test_util import to_dict_strip_last_modified

class BiobankSamplesPipelineTest(testutil.CloudStorageTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)

  def test_end_to_end(self):
    # Insert participants to generate biobank IDs
    participant.DAO.insert(participant.DAO.from_json({}, None, 'P1'))
    participant_1 = participant.DAO.load('P1')
    participant.DAO.insert(participant.DAO.from_json({}, None, 'P2'))
    participant_2 = participant.DAO.load('P2')

    with open(_data_path('biobank_samples_1.csv'), 'rb') as src, \
        cloudstorage_api.open('/pmi-drc-biobank-test.appspot.com/biobank_samples_1.csv', mode='w') as dest:
      reader = csv.reader(src)
      writer = csv.writer(dest)
      for line in reader:
        # Put biobank IDs in the CSV being imported
        line[0] = line[0].replace("{biobank_id_1}", participant_1.biobank_id)
        line[0] = line[0].replace("{biobank_id_2}", participant_2.biobank_id);
        writer.writerow(line)
    BiobankSamplesPipeline('pmi-drc-biobank-test.appspot.com').start()
    test_support.execute_until_empty(self.taskqueue)

    biobank_samples_1 = biobank_sample.DAO.load(biobank_sample.SINGLETON_SAMPLES_ID, 'P1')
    expected_sample_dict_1 = {
        'familyId': 'SF160914-000001',
        'sampleId': '16258000008',
        'eventName': 'DRC-00123',
        'storageStatus': 'In Prep',
        'type': 'Urine',
        'treatments': 'No Additive',
        'expectedVolume': '10 mL',
        'quantity': '1 mL',
        'containerType': 'TS - Matrix 1.4mL',
        'collectionDate': '2016/09/13 09:47:00',
        'parentSampleId': '16258000001',
        'confirmedDate': '2016/09/14 09:49:00' }
    expected_samples_1 = biobank_sample.DAO.from_json(
        { 'samples': [ expected_sample_dict_1 ]}, 
        'P1', biobank_sample.SINGLETON_SAMPLES_ID).to_dict()
    del expected_samples_1['last_modified']    
    self.assertEquals(expected_samples_1, to_dict_strip_last_modified(biobank_samples_1))

def _data_path(filename):
  return os.path.join(os.path.dirname(__file__), '..', 'test-data', filename)
