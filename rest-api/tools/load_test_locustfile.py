import json
import os
import re
import time

from locust import Locust, TaskSet, events, task

from client.client import Client, HttpException
from data_gen.fake_participant_generator import FakeParticipantGenerator


class _RdrUserBehavior(TaskSet):
  @task(1)
  def index(self):
    self.client.request_json('')

  @task(50)
  def participant(self):
    self.locust.participant_generator.generate_participant(
        True, # include_physical_measurements
        False)  # include_biobank_orders


class _ReportingClient(Client):
  """Wrapper around the API Client which reports request stats to Locust."""
  def request_json(self, path, **kwargs):
    name = '/' + re.sub('P[0-9]+', ':participant_id', path)
    event_data = {'request_type': 'REST JSON', 'name': name}
    try:
      start_seconds = time.time()
      resp = super(_ReportingClient, self).request_json(path, **kwargs)
      event = events.request_success
      event_data['response_length'] = len(json.dumps(resp))
      return resp
    except HttpException as e:
      event = events.request_failure
      event_data['exception'] = e
    finally:
      event_data['response_time'] = int(1000 * (time.time() - start_seconds))
      event.fire(**event_data)


class RdrUser(Locust):
  task_set = _RdrUserBehavior
  min_wait = 500
  max_wait = 1500

  def __init__(self, *args, **kwargs):
    super(RdrUser, self).__init__(*args, **kwargs)
    creds_file = os.environ['LOCUST_CREDS_FILE']
    instance = os.environ['LOCUST_TARGET_INSTANCE']
    # The "client" field gets copied to TaskSet instances.
    self.client = _ReportingClient(
        creds_file=creds_file, default_instance=instance, parse_cli=False)
    self.participant_generator = FakeParticipantGenerator(self.client)
