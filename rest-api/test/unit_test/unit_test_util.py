import StringIO
import collections
import contextlib
import copy
import faker
import httplib
import json
import mock
import os
import unittest
import uuid
import dao.database_factory
from dao.organization_dao import OrganizationDao

from google.appengine.api import app_identity
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mock import patch
from model.organization import Organization
from testlib import testutil

import api_util
import config
import config_api
import main
import dao.base_dao
import singletons

from code_constants import PPI_SYSTEM
from concepts import Concept
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from dao.participant_dao import ParticipantDao
from dao.site_dao import SiteDao
from model.code import Code
from model.hpo import HPO
from model.site import Site
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from offline import sql_exporter
from participant_enums import UNSET_HPO_ID, WithdrawalStatus, SuspensionStatus, EnrollmentStatus
from participant_enums import OrganizationType
from test.test_data import data_path
from unicode_csv import UnicodeDictReader

PITT_HPO_ID = 2


class TestBase(unittest.TestCase):
  """Base class for unit tests."""
  def setUp(self):
    # Allow printing the full diff report on errors.
    self.maxDiff = None
    self.setup_fake()

  def setup_fake(self):
    # Make a faker which produces unicode text available.
    self.fake = faker.Faker('ru_RU')
    self.fake.seed(1)

    # Always add codes if missing when handling questionnaire responses.
    dao.questionnaire_dao._add_codes_if_missing = lambda: True
    dao.questionnaire_response_dao._add_codes_if_missing = lambda email:True

  @staticmethod
  def _participant_with_defaults(**kwargs):
    """Creates a new Participant model, filling in some default constructor args.

    This is intended especially for updates, where more fields are required than for inserts.
    """
    common_args = {
      'hpoId': UNSET_HPO_ID,
      'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
      'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
    }
    common_args.update(kwargs)
    return Participant(**common_args)

  @staticmethod
  def _participant_summary_with_defaults(**kwargs):
    common_args = {
      'hpoId': UNSET_HPO_ID,
      'numCompletedPPIModules': 0,
      'numCompletedBaselinePPIModules': 0,
      'numBaselineSamplesArrived': 0,
      'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
      'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
      'enrollmentStatus': EnrollmentStatus.INTERESTED
    }
    common_args.update(kwargs)
    return ParticipantSummary(**common_args)

  @staticmethod
  def _participant_history_with_defaults(**kwargs):
    common_args = {
      'hpoId': UNSET_HPO_ID,
      'version': 1,
      'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
      'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
    }
    common_args.update(kwargs)
    return ParticipantHistory(**common_args)

  def submit_questionnaire_response(self, participant_id, questionnaire_id,
                                    race_code, gender_code, state,
                                    date_of_birth):
    code_answers = []
    date_answers = []
    if race_code:
      code_answers.append(('race', Concept(PPI_SYSTEM, race_code)))
    if gender_code:
      code_answers.append(('genderIdentity', Concept(PPI_SYSTEM, gender_code)))
    if date_of_birth:
      date_answers.append(('dateOfBirth', date_of_birth))
    if state:
      code_answers.append(('state', Concept(PPI_SYSTEM, state)))
    qr = make_questionnaire_response_json(
        participant_id,
        questionnaire_id,
        code_answers=code_answers,
        date_answers=date_answers)
    self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def submit_consent_questionnaire_response(
      self, participant_id, questionnaire_id, ehr_consent_answer):
    code_answers = [('ehrConsent', Concept(PPI_SYSTEM, ehr_consent_answer))]
    qr = make_questionnaire_response_json(
        participant_id, questionnaire_id, code_answers=code_answers)
    self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)

  def participant_summary(self, participant):
    summary = ParticipantDao.create_summary_for_participant(participant)
    summary.firstName = self.fake.first_name()
    summary.lastName = self.fake.last_name()
    summary.email = self.fake.email()
    return summary

class TestbedTestBase(TestBase):
  """Base class for unit tests that need the testbed."""
  def setUp(self):
    super(TestbedTestBase, self).setUp()
    # Reset singletons, including the database, between tests.
    singletons.reset_for_tests()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_taskqueue_stub()
    self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

  def tearDown(self):
    self.testbed.deactivate()
    super(TestbedTestBase, self).tearDown()


class _TestDb(object):
  """Container for common testing database setup/teardown, using SQLite or MySQL.

  SQLite uses a fast/lightweight in-memory database. MySQL requires a local mysqldb configured with
  tools/setup_local_database.sh, and is slower but matches deployed environments; it uses a
  temporary database.
  """
  def __init__(self, use_mysql=False):
    self.__use_mysql = use_mysql
    if self.__use_mysql:
      uid = uuid.uuid4().hex
      self.__temp_db_name = 'unittestdb' + uid
      self.__temp_metrics_db_name = 'unittestdb_metrics' + uid

  def setup(self, with_data=True, with_views=False):
    singletons.reset_for_tests()  # Clear the db connection cache.
    if self.__use_mysql:
      if 'CIRCLECI' in os.environ:
        # Default no-pw login, according to https://circleci.com/docs/1.0/manually/#databases .
        mysql_login = 'ubuntu'
      else:
        # Match setup_local_database.sh which is run locally.
        mysql_login = 'root:root'
      dao.database_factory.DB_CONNECTION_STRING = (
          'mysql+mysqldb://%s@localhost/?charset=utf8' % mysql_login)
      db = dao.database_factory.get_database()
      dao.database_factory.SCHEMA_TRANSLATE_MAP = {
        'rdr': self.__temp_db_name,
        'metrics': self.__temp_metrics_db_name
      }
      # Keep in sync with tools/setup_local_database.sh.
      db.get_engine().execute(
          'CREATE DATABASE %s CHARACTER SET utf8 COLLATE utf8_general_ci' % self.__temp_db_name)
      db.get_engine().execute(
          'CREATE DATABASE %s CHARACTER SET utf8 COLLATE utf8_general_ci' % self.__temp_metrics_db_name)

      dao.database_factory.DB_CONNECTION_STRING = (
          'mysql+mysqldb://%s@localhost/%s?charset=utf8' % (mysql_login, self.__temp_db_name))
      singletons.reset_for_tests()
    else:
      dao.database_factory.DB_CONNECTION_STRING = 'sqlite:///:memory:'
      dao.database_factory.SCHEMA_TRANSLATE_MAP = {
        'rdr': None,
        'metrics': None
      }
    dao.database_factory.get_database().create_schema()
    dao.database_factory.get_generic_database().create_metrics_schema()
    if with_data:
      self._setup_hpos()
    if with_views:
      self._setup_views()

  def teardown(self):
    db = dao.database_factory.get_database()
    if self.__use_mysql:
      db.get_engine().execute('DROP DATABASE IF EXISTS %s' % self.__temp_db_name)
    db.get_engine().dispose()
    dao.database_factory.SCHEMA_TRANSLATE_MAP = None
    # Reconnecting to in-memory SQLite (because singletons are cleared above)
    # effectively clears the database.

  def _setup_hpos(self):
    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=UNSET_HPO_ID, name='UNSET', displayName='Unset',
                       organizationType=OrganizationType.UNSET))
    hpo_dao.insert(HPO(hpoId=PITT_HPO_ID, name='PITT', displayName='Pittsburgh',
                       organizationType=OrganizationType.HPO))
    self.hpo_id = PITT_HPO_ID

    site_dao = SiteDao()
    created_site = site_dao.insert(Site(
        siteName='Monroeville Urgent Care Center',
        googleGroup='hpo-site-monroeville',
        mayolinkClientNumber=7035769,
        hpoId=PITT_HPO_ID))
    self.site_id = created_site.siteId
    site_dao.insert(Site(
        siteName='Phoenix Urgent Care Center',
        googleGroup='hpo-site-bannerphoenix',
        mayolinkClientNumber=7035770,
        hpoId=PITT_HPO_ID))

    org_dao = OrganizationDao()
    created_org = org_dao.insert(Organization(
      externalId='AZ_TUCSON_BANNER_HEALTH',
      displayName='Banner Health',
      hpoId=PITT_HPO_ID))
    self.org_id = created_org.organizationId

  def _setup_views(self):
    """
    Sets up operational DB views.

    This is a minimal recreation of the true views, which are encoded in Alembic DB migrations only
    (not via SQLAlchemy) and aren't currently compatible with SQLite.
    """
    db = dao.database_factory.get_database()
    db.get_engine().execute("""
CREATE VIEW ppi_participant_view AS
 SELECT
   p.participant_id,
   hpo.name hpo,
   ps.enrollment_status
 FROM
   participant p
     LEFT OUTER JOIN hpo ON p.hpo_id = hpo.hpo_id
     LEFT OUTER JOIN participant_summary ps ON p.participant_id = ps.participant_id
""")


class SqlTestBase(TestbedTestBase):
  """Base class for unit tests that use the SQL database."""
  def setUp(self, with_data=True, use_mysql=False):
    super(SqlTestBase, self).setUp()
    self._test_db = _TestDb(use_mysql=use_mysql)
    self._test_db.setup(with_data=with_data)
    self.database = dao.database_factory.get_database()

  def tearDown(self):
    self._test_db.teardown()
    super(SqlTestBase, self).tearDown()

  @staticmethod
  def setup_codes(values, code_type):
    code_dao = CodeDao()
    for value in values:
      code_dao.insert(Code(system=PPI_SYSTEM, value=value, codeType=code_type, mapped=True))

  def assertObjEqualsExceptLastModified(self, obj1, obj2):
    dict1 = obj1.asdict()
    dict2 = obj2.asdict()
    del dict1['lastModified']
    del dict2['lastModified']
    self.assertEquals(dict1, dict2)

  def assertListAsDictEquals(self, list_a, list_b):
    if len(list_a) != len(list_b):
      self.fail("List lengths don't match: %d != %d; %s, %s" % (len(list_a), len(list_b),
                                                                list_as_dict(list_a),
                                                                list_as_dict(list_b)))
    for i in range(0, len(list_a)):
      self.assertEquals(list_a[i].asdict(), list_b[i].asdict())


class InMemorySqlExporter(sql_exporter.SqlExporter):
  """Store rows that would be written to GCS CSV in a StringIO instead.

  Provide some assertion helpers related to CSV contents.
  """
  def __init__(self, test):
    super(InMemorySqlExporter, self).__init__('inmemory')  # fake bucket name
    self._test = test
    self._path_to_buffer = collections.defaultdict(StringIO.StringIO)

  @contextlib.contextmanager
  def open_writer(self, file_name, predicate=None):
    yield sql_exporter.SqlExportFileWriter(self._path_to_buffer[file_name], predicate,
                                           use_unicode=True)

  def assertFilesEqual(self, paths):
    self._test.assertItemsEqual(paths, self._path_to_buffer.keys())

  def _get_dict_reader(self, file_name):
    return UnicodeDictReader(
        StringIO.StringIO(self._path_to_buffer[file_name].getvalue()),
        delimiter=sql_exporter.DELIMITER)

  def assertColumnNamesEqual(self, file_name, col_names):
    self._test.assertItemsEqual(col_names, self._get_dict_reader(file_name).fieldnames)

  def assertRowCount(self, file_name, n):
    rows = list(self._get_dict_reader(file_name))
    self._test.assertEquals(
        n, len(rows), 'Expected %d rows in %r but found %d: %s.' % (n, file_name, len(rows), rows))

  def assertHasRow(self, file_name, expected_row):
    """Asserts that the writer got a row that has all the values specified in the given row.

    Args:
      file_name: The bucket-relative path of the file that should have the row.
      expected_row: A dict like {'biobank_id': 557741928, sent_test: None} specifying a subset of
          the fields in a row that should have been written.
    Returns:
      The matched row.
    """
    rows = list(self._get_dict_reader(file_name))
    for row in rows:
      found_all = True
      for required_k, required_v in expected_row.iteritems():
        if required_k not in row or row[required_k] != required_v:
          found_all = False
          break
      if found_all:
        return row
    self._test.fail(
        'No match found for expected row %s among %d rows: %s'
        % (expected_row, len(rows), rows))


class NdbTestBase(SqlTestBase):
  """Base class for unit tests that need the NDB testbed."""
  _AUTH_USER = 'authorized@gservices.act'  # authorized for typical API usage roles
  _CONFIG_USER_INFO = {
    _AUTH_USER: {
      'roles': api_util.ALL_ROLES,
    },
  }

  def setUp(self, use_mysql=False):
    super(NdbTestBase, self).setUp(use_mysql=use_mysql)
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()
    self.doSetUp()

  def doSetUp(self):
    dev_config = read_dev_config()
    dev_config[config.USER_INFO] = self._CONFIG_USER_INFO
    config.store_current_config(dev_config)
    config.CONFIG_OVERRIDES = {}


class CloudStorageSqlTestBase(testutil.CloudStorageTestBase):
  """Merge AppEngine's provided CloudStorageTestBase and our SqlTestBase.

  Both try to set up a testbed (which stubs out various AppEngine APIs, including cloudstorage_api).
  """
  def setUp(self, use_mysql=False, with_data=True, with_views=False):
    super(CloudStorageSqlTestBase, self).setUp()
    self._test_db = _TestDb(use_mysql=use_mysql)
    self._test_db.setup(with_data=with_data, with_views=with_views)
    self.database = dao.database_factory.get_database()
    self.taskqueue_stub = self.testbed.get_stub(testbed.TASKQUEUE_SERVICE_NAME)

  def tearDown(self):
    super(CloudStorageSqlTestBase, self).tearDown()
    self._test_db.teardown()


def read_dev_config():
  with open(os.path.join(os.path.dirname(__file__), '../../config/config_dev.json')) as config_file:
    with open(os.path.join(os.path.dirname(__file__), '../../config/base_config.json')) as b_cfg:
      config_json = json.load(b_cfg)
      config_json.update(json.load(config_file))
      return config_json


class FlaskTestBase(NdbTestBase):
  """Provide a local flask server to exercise handlers and storage."""
  _ADMIN_USER = 'config_admin@fake.google.com'  # allowed to update config

  def doSetUp(self):
    super(FlaskTestBase, self).doSetUp()
    # http://flask.pocoo.org/docs/0.12/testing/
    main.app.config['TESTING'] = True
    self._app = main.app.test_client()

    self._patchers = []
    mock_oauth = mock.patch('app_util.get_oauth_id')
    self._mock_get_oauth_id = mock_oauth.start()
    self._patchers.append(mock_oauth)

    config_api.CONFIG_ADMIN_MAP[app_identity.get_application_id()] = self._AUTH_USER

    self.set_auth_user(self._AUTH_USER)
    self._consent_questionnaire_id = None

  def tearDown(self):
    super(FlaskTestBase, self).tearDown()
    self.doTearDown()

  def doTearDown(self):
    for patcher in self._patchers:
      patcher.stop()

  def set_auth_user(self, auth_user):
    self._mock_get_oauth_id.return_value = auth_user

  def send_post(self, *args, **kwargs):
    return self.send_request('POST', *args, **kwargs)

  def send_put(self, *args, **kwargs):
    return self.send_request('PUT', *args, **kwargs)

  def send_get(self, *args, **kwargs):
    return self.send_request('GET', *args, **kwargs)

  def send_request(self, method, local_path, request_data=None, query_string=None,
                   expected_status=httplib.OK, headers=None, expected_response_headers=None):
    """Makes a JSON API call against the test client and returns its response data.

    Args:
      method: HTTP method, as a string.
      local_path: The API endpoint's URL (excluding main.PREFIX).
      request_data: Parsed JSON payload for the request.
      expected_status: What HTTP status to assert, if not 200 (OK).
    """
    response = self._app.open(
        main.PREFIX + local_path,
        method=method,
        data=json.dumps(request_data) if request_data is not None else None,
        query_string=query_string,
        content_type='application/json',
        headers=headers)
    self.assertEquals(response.status_code, expected_status, response.data)
    if expected_response_headers:
      self.assertTrue(set(expected_response_headers.items())
                          .issubset(set(response.headers.items())),
                      "Expected response headers: %s; actual: %s" %
                      (expected_response_headers, response.headers))
    if expected_status == httplib.OK:
      return json.loads(response.data)
    return None

  def create_participant(self):
    response = self.send_post('Participant', {})
    return response['participantId']

  def send_consent(self, participant_id, email=None):
    if not self._consent_questionnaire_id:
      self._consent_questionnaire_id = self.create_questionnaire('study_consent.json')
    self.first_name = self.fake.first_name()
    self.last_name = self.fake.last_name()
    if not email:
      self.email = self.fake.email()
      email = self.email
    qr_json = make_questionnaire_response_json(participant_id, self._consent_questionnaire_id,
                                               string_answers=[("firstName", self.first_name),
                                                               ("lastName", self.last_name),
                                                               ("email", email)])
    self.send_post(questionnaire_response_url(participant_id), qr_json)

  def create_questionnaire(self, filename):
    with open(data_path(filename)) as f:
      questionnaire = json.load(f)
      response = self.send_post('Questionnaire', questionnaire)
      return response['id']

  def create_and_verify_created_obj(self, path, resource):
    response = self.send_post(path, resource)
    resource_id = response['id']
    del response['id']
    self.assertJsonResponseMatches(resource, response)

    response = self.send_get('{}/{}'.format(path, resource_id))
    del response['id']
    self.assertJsonResponseMatches(resource, response)

  def assertJsonResponseMatches(self, obj_a, obj_b):
    self.assertMultiLineEqual(
        _clean_and_format_response_json(obj_a), _clean_and_format_response_json(obj_b))

  def assertBundle(self, expected_entries, response, has_next=False):
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    if len(expected_entries) != len(response['entry']):
      self.fail("Expected %d entries, got %d: %s" % (len(expected_entries), len(response['entry']),
                                                     response['entry']))
    for i in range(0, len(expected_entries)):
      self.assertJsonResponseMatches(expected_entries[i], response['entry'][i])
    if has_next:
      self.assertEquals('next', response['link'][0]['relation'])
      return response['link'][0]['url']
    else:
      self.assertIsNone(response.get('link'))
      return None

def _clean_and_format_response_json(input_obj):
  obj = sort_lists(copy.deepcopy(input_obj))
  for ephemeral_key in ('meta', 'lastModified'):
    if ephemeral_key in obj:
      del obj[ephemeral_key]
  s = pretty(obj)
  # TODO(DA-226) Make sure times are not skewed on round trip to CloudSQL. For now, strip tzinfo.
  s = s.replace('+00:00', '')
  s = s.replace('Z",', '",')
  return s

def list_as_dict(items):
  return [item.asdict() for item in items]

def to_dict_strip_last_modified(obj):
  assert obj.last_modified, 'Missing last_modified: {}'.format(obj)
  obj_json = obj.to_dict()
  del obj_json['last_modified']
  if obj_json.get('signUpTime'):
    del obj_json['signUpTime']
  return obj_json


def sort_lists(obj):
  for key, val in obj.iteritems():
    if type(val) is list:
      obj[key] = sorted(val)
  return obj


def make_questionnaire_response_json(participant_id, questionnaire_id, code_answers=None,
                                string_answers=None, date_answers=None, uri_answers=None):
  results = []
  if code_answers:
    for answer in code_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueCoding": {
                           "code": answer[1].code,
                           "system": answer[1].system
                         }
                       }]
                    })
  if string_answers:
    for answer in string_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueString": answer[1] }
                       ]
                    })
  if date_answers:
    for answer in date_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueDate": "%s" % answer[1].isoformat() }
                        ]
                    })
  if uri_answers:
    for answer in uri_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueUri": answer[1] }
                        ]
                    })
  return {"resourceType": "QuestionnaireResponse",
          "status": "completed",
          "subject": { "reference": "Patient/{}".format(participant_id) },
          "questionnaire": { "reference": "Questionnaire/{}".format(questionnaire_id) },
          "group": {
            "question": results
          }
      }

def questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id

def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

@contextlib.contextmanager
def random_ids(ids):
  with patch('random.randint', side_effect=ids):
    yield

def run_deferred_tasks(test):
  tasks = test.taskqueue.get_filtered_tasks()
  test.taskqueue.FlushQueue("default")
  while tasks:
    for task in tasks:
      if task.url == '/_ah/queue/deferred':
        deferred.run(task.payload)
    tasks = test.taskqueue.get_filtered_tasks()
    for task in tasks:
      # As soon as we hit a non-deferred task, bail out of here.
      if task.url != '/_ah/queue/deferred':
        return
    test.taskqueue.FlushQueue("default")
