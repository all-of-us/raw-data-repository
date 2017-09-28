import csv
import clock
import config
import datetime
import logging
import random

from cloudstorage import cloudstorage_api
from code_constants import BIOBANK_TESTS
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_dao import ParticipantDao
from model.utils import to_client_biobank_id
from offline.biobank_samples_pipeline import INPUT_CSV_TIME_FORMAT

# 80% of participants with orders have corresponding stored samples.
_PARTICIPANTS_WITH_STORED_SAMPLES = 0.8
# 10% of individual stored samples are missing
_MISSING_STORED_SAMPLE = 0.1
# 1% of participants have samples with no associated order
_PARTICIPANTS_WITH_ORPHAN_SAMPLES = 0.01
# Max amount of time between collected ordered samples and confirmed biobank stored samples.
_MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_CONFIRMED = 72 * 60
# Max amount of time between creating a participant and orphaned biobank samples
_MAX_MINUTES_BETWEEN_PARTICIPANT_CREATED_AND_CONFIRMED = 30 * 24 * 60

_TIME_FORMAT = "%Y/%m/%d %H:%M:%S"

#_GET_ORDERED_SAMPLES_SQL = """
#  SELECT participant.biobank_id, sample.collected, sample.test
#    FROM participant, biobank_ordered_sample sample, biobank_order order
#   WHERE participant.participant_id = biobank_order.participant_id
#     AND biobank_ordered_sample.biobank_order_id = biobank_order.biobank_order_id
#     AND participant.biobank_id %% 100 > %s
#""" % _NO_SAMPLES_FOR_ORDER

_BATCH_SIZE = 1000

_HEADERS = ['Sample Id', 'Parent Sample Id', 'Sample Confirmed Date', 'External Participant Id',
            'Test Code']

def generate_samples():
  """Creates fake sample CSV data in GCS."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
  now = clock.CLOCK.now()
  file_name = '/%s/fake_%s.csv' % (bucket_name, now.strftime(INPUT_CSV_TIME_FORMAT))
  num_rows = 0
  sample_id_start = random.randint(1000000, 10000000)
  with cloudstorage_api.open(file_name, mode='w') as dest:
    writer = csv.writer(dest, delimiter="\t")
    writer.writerow(_HEADERS)
    biobank_order_dao = BiobankOrderDao()
    with biobank_order_dao.session() as session:
      rows = biobank_order_dao.get_ordered_samples_sample(session,
                                                          _PARTICIPANTS_WITH_STORED_SAMPLES,
                                                          _BATCH_SIZE)
      for biobank_id, collected_time, test in rows:
        if collected_time is None or random.random() <= _MISSING_STORED_SAMPLE:
          continue
        minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_CONFIRMED)
        confirmed_time = collected_time + datetime.timedelta(minutes=minutes_delta)
        writer.writerow([sample_id_start + num_rows, None,
                         confirmed_time.strftime(_TIME_FORMAT),
                         to_client_biobank_id(biobank_id), test])
        num_rows += 1
    participant_dao = ParticipantDao()
    with participant_dao.session() as session:
      rows = participant_dao.get_biobank_ids_sample(session,
                                                    _PARTICIPANTS_WITH_ORPHAN_SAMPLES,
                                                    _BATCH_SIZE)
      for biobank_id, sign_up_time in rows:
        minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_PARTICIPANT_CREATED_AND_CONFIRMED)
        confirmed_time = sign_up_time + datetime.timedelta(minutes=minutes_delta)
        tests = random.sample(BIOBANK_TESTS, random.randint(1, len(BIOBANK_TESTS)))
        for test in tests:
          writer.writerow([sample_id_start + num_rows, None,
                           confirmed_time.strftime(_TIME_FORMAT),
                           to_client_biobank_id(biobank_id), test])
          num_rows += 1
  logging.info("Generated %d samples in %s.", num_rows, file_name)
