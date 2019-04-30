#
# biobank order data generator.
#
import clock
import datetime
import logging
import random

from data_gen.generators.base_gen import BaseGen
from data_gen.generators.hpo import HPOGen

_logger = logging.getLogger('rdr_logger')

class BioBankOrderGen(BaseGen):
  """
  Fake biobank order data generator
  """
  _participant_id = None
  _site = None
  _sample_test = None

  def __init__(self):
    """ initialize biobank order generator """
    super(BioBankOrderGen, self).__init__(load_data=False)

  def new(self, participant_id, sample_test, site=None):
    """
    Return a new biobank order object with the assigned site.
    :param participant_id: participant id
    :param sample_test: sample test code
    :param site: HPOSiteGen object
    :return: return cloned BioBankOrderGen object
    """
    clone = self.__class__()
    clone._participant_id = participant_id
    clone._sample_test = sample_test
    if site:
      clone._site = site
    else:
      clone._site = HPOGen().get_random_site()

    return clone

  def make_fhir_document(self):
    """
    build a FHIR bundle with physical measurement resource objects.
    :return: FHIR bundle object
    """
    doc, finalized = self._make_biobank_order_request()
    return doc, finalized

  def _make_biobank_order_request(self):
    samples = []
    order_id_suffix = '{0}-{1}'.format(self._participant_id, random.randint(0, 100000000))
    created_time = clock.CLOCK.now()

    handling_info = {
      "author": {
         "system": "https://www.pmi-ops.org/healthpro-username",
         "value": "nobody@pmi-ops.org"
      },
      "site": {
        "system": "https://www.pmi-ops.org/site-id",
        "value": self._site.id
      }
    }
    document = {
        "subject": "Patient/{0}".format(self._participant_id),
        "identifier": [
            {"system": "https://www.pmi-ops.org",
             "value": "healthpro-order-id-123{0}".format(order_id_suffix)},
            {"system": "https://orders.mayomedicallaboratories.com",
             "value": "WEB1YLHV{0}".format(order_id_suffix)},
            {"system": "https://orders.mayomedicallaboratories.com/kit-id",
             "value": "KIT-{0}".format(order_id_suffix)},
            {"system": "https://orders.mayomedicallaboratories.com/tracking-number",
             "value": "177{0}".format(order_id_suffix)}],
        "createdInfo": handling_info,
        "processedInfo": handling_info,
        "collectedInfo": handling_info,
        "finalizedInfo": handling_info,
        "created": created_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "samples": samples,
        "notes": {
          "collected": "Collected notes",
          "processed": "Processed notes",
          "finalized": "Finalized notes"
        }
       }

    minutes_delta = random.randint(0, (72 * 60))
    collected_time = created_time + datetime.timedelta(minutes=minutes_delta)
    minutes_delta = random.randint(0, (72 * 60))
    processed_time = collected_time + datetime.timedelta(minutes=minutes_delta)
    minutes_delta = random.randint(0, (72 * 60))
    finalized_time = processed_time + datetime.timedelta(minutes=minutes_delta)
    processing_required = True if random.random() <= 0.5 else False
    samples.append({"test": self._sample_test,
                    "description": "Description for {0}".format(self._sample_test),
                    "collected": collected_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "processed": processed_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "finalized": finalized_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "processingRequired": processing_required})
    return document, finalized_time

  # def _submit_biobank_order(self, participant_id, start_time):
  #   num_samples = random.randint(1, len(BIOBANK_TESTS))
  #   order_tests = random.sample(BIOBANK_TESTS, num_samples)
  #   days_delta = random.randint(0, 60)
  #   created_time = start_time + datetime.timedelta(days=days_delta)
  #   order_json = self._make_biobank_order_request(participant_id, order_tests, created_time)
  #   self._client.request_json(
  #       _biobank_order_url(participant_id),
  #       method='POST',
  #       body=order_json,
  #       pretend_date=created_time)
  #   return created_time
  #
  # def _submit_biobank_data(self, participant_id, consent_time, force_measurement=False):
  #   if random.random() <= _NO_BIOBANK_ORDERS and not force_measurement:
  #     return consent_time
  #   last_request_time = self._submit_biobank_order(participant_id, consent_time)
  #   if random.random() <= _MULTIPLE_BIOBANK_ORDERS:
  #     last_request_time = self._submit_biobank_order(participant_id, last_request_time)
  #   return last_request_time
