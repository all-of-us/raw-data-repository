"""User behavior definition for load-testing via Locust. Run using tools/load_test.sh."""

import json
import os
import re
import time

from locust import Locust, TaskSet, events, task

from client.client import Client, HttpException
from data_gen.fake_participant_generator import FakeParticipantGenerator


class _ReportingClient(Client):
  """Wrapper around the API Client which reports request stats to Locust."""
  def request_json(self, path, **kwargs):
    event_data = {'request_type': 'REST JSON', 'name': self._clean_up_url(path)}
    event = events.request_failure
    try:
      start_seconds = time.time()
      resp = super(_ReportingClient, self).request_json(path, **kwargs)
      event = events.request_success
      event_data['response_length'] = len(json.dumps(resp))
      return resp
    except HttpException as e:
      event_data['exception'] = e
    finally:
      event_data['response_time'] = int(1000 * (time.time() - start_seconds))
      event.fire(**event_data)

  def _clean_up_url(self, path):
    # Replace varying IDs with a placeholder so counts get aggregated.
    name = re.sub('P[0-9]+', ':participant_id', path)
    # Convert absolute URLs to relative.
    strip_prefix = '%s/%s/' % (self.instance, self.base_path)
    if name.startswith(strip_prefix):
      name = name[len(strip_prefix):]
    # Prefix relative URLs with the root path for readability.
    if not name.startswith('http'):
      name = '/' + name
    return name


class _AuthenticatedLocust(Locust):
  def __init__(self, *args, **kwargs):
    super(_AuthenticatedLocust, self).__init__(*args, **kwargs)
    creds_file = os.environ['LOCUST_CREDS_FILE']
    instance = os.environ['LOCUST_TARGET_INSTANCE']
    # The "client" field gets copied to TaskSet instances.
    self.client = _ReportingClient(
        creds_file=creds_file, default_instance=instance, parse_cli=False)
    self.participant_generator = FakeParticipantGenerator(self.client)


class VersionCheckUser(_AuthenticatedLocust):
  # 1 out of 100 users (Locust count of 100 recommended in load_test.sh).
  weight = 1
  # Hit the root/version endpoint once per minute.
  min_wait = 1000 * 60
  max_wait = 1000 * 60
  class task_set(TaskSet):  # The "task_set" field name is what's used by the Locust superclass.
    @task(1)  # task weight: larger number == pick this task more often
    def index(self):
      self.client.request_json('')


class SyncPhysicalMeasurementsUser(_AuthenticatedLocust):
  weight = 1
  # We expect 1 sync request/minute.
  min_wait = 1000 * 60
  max_wait = 1000 * 60
  class task_set(TaskSet):
    @task(1)
    def get_sync(self):
      next_url = 'PhysicalMeasurements/_history'
      absolute_path = False
      while next_url:
        history = self.client.request_json(next_url, absolute_path=absolute_path)
        link = history.get('link')
        if link and link[0]['relation'] == 'next':
          next_url = link[0]['url']
          absolute_path = True
        else:
          next_url = None


class SignupUser(_AuthenticatedLocust):
  weight = 98
  # We estimate 100-1000 signups/day or 80-800s between signups (across all users).
  min_wait = weight * 1000 * 80
  max_wait = weight * 1000 * 800
  class task_set(TaskSet):
    @task(1)
    def register_participant(self):
      self.locust.participant_generator.generate_participant(
          True,  # include_physical_measurements
          True)  # include_biobank_orders
