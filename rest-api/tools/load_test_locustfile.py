"""User behavior definition for load-testing via Locust. Run using tools/load_test.sh.

We expect very low traffic (100-1K qpd for most endpoints). These load tests generate much more
traffic (around 10qps) to stress test the system / simulate traffic spikes.
"""

import json
import os
import random
import re
import time
from urllib import urlencode
import urlparse

from locust import Locust, TaskSet, events, task

from client.client import Client, HttpException
from data_gen.fake_participant_generator import FakeParticipantGenerator


class _ReportingClient(Client):
  """Wrapper around the API Client which reports request stats to Locust."""
  def request_json(self, path, **kwargs):
    event_data = {'request_type': 'REST', 'name': self._clean_up_url(path)}
    event = None
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
      if event is not None:
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

    # Replace query parameters with non-varying placeholders.
    parsed = urlparse.urlparse(name)
    query = urlparse.parse_qs(parsed.query)
    for k in query.keys():
      query[k] = 'X'
    name = parsed._replace(query=urlencode(query)).geturl()

    return name


class _AuthenticatedLocust(Locust):
  """Base for authenticated RDR REST API callers."""
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
  # Hit the root/version endpoint every 10s.
  min_wait = 1000 * 10
  max_wait = min_wait
  class task_set(TaskSet):  # The "task_set" field name is what's used by the Locust superclass.
    @task(1)  # task weight: larger number == pick this task more often
    def index(self):
      self.client.request_json('')


class SyncPhysicalMeasurementsUser(_AuthenticatedLocust):
  weight = 1
  # In practice we expect 1 sync request/minute. Use the default 1s wait time here.
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
  weight = 88
  # We estimate 100-1000 signups/day or 80-800s between signups (across all users).
  # Simulate 2 signups/s across a number of users for the load test.
  min_wait = weight * 500
  max_wait = min_wait
  class task_set(TaskSet):
    @task(1)
    def register_participant(self):
      self.locust.participant_generator.generate_participant(
          True,  # include_physical_measurements
          True)  # include_biobank_orders


class HealthProUser(_AuthenticatedLocust):
  """Queries run by HealthPro: look up user by name + dob or ID, and get summaries."""
  weight = 10
  # We (probably over)estimate 100-1000 summary or participant queries/day (per task below).
  min_wait = weight * 1000 * 40
  max_wait = min_wait

  def __init__(self, *args, **kwargs):
    super(HealthProUser, self).__init__(*args, **kwargs)
    self.participant_ids = []
    self.participant_name_dobs = []
    absolute_path = False
    summary_url = 'ParticipantSummary?hpoId=PITT'
    for _ in xrange(3):  # Fetch a few pages of participants.
      resp = self.client.request_json(summary_url, absolute_path=absolute_path)
      for summary in resp['entry']:
        resource = summary['resource']
        self.participant_ids.append(resource['participantId'])
        try:
          self.participant_name_dobs.append(
              [resource[k] for k in ('firstName', 'lastName', 'dateOfBirth')])
        except KeyError:
          pass  # Ignore some participants, missing DOB.
      if 'link' in resp and resp['link'][0]['relation'] == 'next':
        summary_url = resp['link'][0]['url']
        absolute_path = True
      else:
        break

  class task_set(TaskSet):
    @task(1)
    def get_participant_by_id(self):
      self.client.request_json('Participant/%s' % random.choice(self.locust.participant_ids))

    @task(1)
    def get_participant_summary_by_id(self):
      self.client.request_json(
          'Participant/%s/Summary' % random.choice(self.locust.participant_ids))

    @task(1)
    def look_up_participant_by_name_dob(self):
      _, last_name, dob = random.choice(self.locust.participant_name_dobs)
      self.client.request_json('ParticipantSummary?dateOfBirth=%s&lastName=%s' % (dob, last_name))

    @task(1)
    def query_summary(self):
      available_params = (
        ('hpoId', random.choice(('PITT', 'UNSET', 'COLUMBIA'))),
        ('ageRange', random.choice(('0-17', '18-25', '66-75'))),
        ('physicalMeasurementsStatus', 'COMPLETED'),
        ('race', random.choice(('UNSET', 'ASIAN', 'WHITE', 'HISPANIC_LATINO_OR_SPANISH'))),
        ('state', random.choice(('PIIState_MA', 'PIIState_CA', 'PIIState_TX'))),
      )
      search_params = dict(random.sample(
          available_params,
          random.randint(1, len(available_params))))
      self.client.request_json('ParticipantSummary?%s' % urlencode(search_params))
