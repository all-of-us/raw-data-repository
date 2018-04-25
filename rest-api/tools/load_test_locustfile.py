"""User behavior definition for load-testing via Locust. Run using tools/load_test.sh.

Locust docs: http://docs.locust.io/en/latest/writing-a-locustfile.html

Instructions:
*   In your venv, run easy_install install locustio
*   Run "export sdk_dir" to export the path to your locally installed Google Cloud SDK.
*   Run load_test.sh, which wraps this and starts a locust server, e.g.:
   tools/load_test.sh --project all-of-us-rdr-staging --account dan.rodney@pmi-ops.org
*   Once started, locust prints "Starting web monitor at *:8089". Open
    http://localhost:8089 to view the control/status page.
*   Set the number of users to 100 (and hatch/sec to an arbitrary number, using 100 will start all
    workers immediately). With 100 locusts, weights can be thought of as "number of workers."
    *   Each worker will run a task and then pause (somewhere from `min_wait` to `max_wait`
        milliseconds). It picks one of its class methods to run, which in turn are weighted by
        the argument to `@task`.
*   Click run, locusts hatch and run, gather stats, click stop.

We expect very low traffic (100-1K qpd for most endpoints). These load tests generate much more
traffic to stress test the system / simulate traffic spikes.
"""

import json
import os
import random
import re
import time
from urllib import urlencode
import urlparse

from locust import Locust, TaskSet, events, task

from client import Client, HttpException
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
    self.participant_generator = FakeParticipantGenerator(self.client, use_local_files=True)


class VersionCheckUser(_AuthenticatedLocust):
  # 2 out of 100 users (Locust count of 100 recommended in load_test.sh).
  weight = 2
  # Hit the root/version endpoint every 10s.
  min_wait = 1000 * 10 * weight
  max_wait = min_wait
  class task_set(TaskSet):  # The "task_set" field name is what's used by the Locust superclass.
    @task(1)  # task weight: larger number == pick this task more often
    def index(self):
      self.client.request_json('')


class SyncPhysicalMeasurementsUser(_AuthenticatedLocust):
  weight = 2
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
  weight = 2
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
  weight = 94
  # As of 2017 August, in 24h we see about 1000 ParticipantSummary and a similar number of
  # individual summary requests. Simulate more load than that (about 1M/day) to force resource
  # contention.
  min_wait = 1000
  max_wait = 10000

  class task_set(TaskSet):
    def __init__(self, *args, **kwargs):
      super(HealthProUser.task_set, self).__init__(*args, **kwargs)
      self.participant_ids = []
      self.participant_name_dobs = []

    def on_start(self):
      """Fetches some participant data from the work queue API for subsequent tasks."""
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

    @task(10)  # ParticipantSummary is the most popular API endpoint.
    def query_summary(self):
      search_params = {
          # HealthPro always requests 1000 for the work queue.
          '_sort%3Adesc': 'consentForStudyEnrollmentTime',
          '_count': '1000',
          'hpoId': random.choice(('PITT', 'UNSET', 'COLUMBIA')),
      }
      self.client.request_json('ParticipantSummary?%s' % urlencode(search_params))

    @task(1)
    def get_participant_by_id(self):
      self.client.request_json('Participant/%s' % random.choice(self.participant_ids))

    @task(1)
    def get_participant_summary_by_id(self):
      self.client.request_json(
          'Participant/%s/Summary' % random.choice(self.participant_ids))

    @task(1)
    def look_up_participant_by_name_dob(self):
      _, last_name, dob = random.choice(self.participant_name_dobs)
      self.client.request_json('ParticipantSummary?dateOfBirth=%s&lastName=%s' % (dob, last_name))
