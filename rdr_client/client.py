import argparse
import copy
import httplib
import httplib2
import json
import logging
import pprint

from oauth2client.service_account import ServiceAccountCredentials


SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
DEFAULT_INSTANCE = 'https://pmi-drc-api-test.appspot.com'
_DEFAULT_BASE_PATH = 'rdr/v1'
POST_HEADERS = {
    'Content-Type': 'application/json; charset=utf-8',
}
client_log = logging.getLogger(__name__)


class HttpException(BaseException):
  def __init__(self, url, method, response, content):
    message = '%s:%s - %s\n---%s' % (url, method, response.status, content)
    super(HttpException, self).__init__(self, message)
    self.message = message
    self.code = response.status
    self.response = response
    self.content = content


class Client(object):
  """Encapsulation for making authenticated API JSON requests.

  Command-line arg parsing for --instance and --creds_file, and implementation for making a JSON
  request.
  """
  def __init__(
      self,
      base_path=_DEFAULT_BASE_PATH,
      parse_cli=True,
      creds_file=None,
      default_instance=None,
      parser=None):
    default_instance = default_instance or DEFAULT_INSTANCE
    parser = parser or argparse.ArgumentParser()
    if parse_cli:
      self.args = self._parse_args(default_instance, parser)
      if base_path == 'offline':
        # Adjust the instance to be https://offline-dot-<PROJECT>.appspot.com
        # for offline requests
        self.instance = 'https://offline-dot-%s' % self.args.instance[8:]
      else:
        self.instance = self.args.instance

      if self.args.creds_file:
        creds_file = self.args.creds_file
    else:
      self.instance = default_instance
    self.base_path = base_path
    if not creds_file and 'localhost' not in self.instance:
      raise ValueError('Client requires credentials for non-local instance %r.' % self.instance)
    self.creds_file = creds_file
    self._http = self._get_authorized_http()
    self.last_etag = None

  def _parse_args(self, default_instance, parser):
    parser.add_argument(
        '--instance',
        help='The instance to hit, either https://xxx.appspot.com, '
        'or http://localhost:8080',
        default=default_instance)
    parser.add_argument(
        '--project',
        help='GCP project name associated with --instance.')
    parser.add_argument(
        '--creds_file',
        help='Path to a credentials file to use when talking to the server.',
        required=False)
    return parser.parse_args()

  def _get_authorized_http(self):
    if self.creds_file:
      credentials = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, [SCOPE])
      return credentials.authorize(httplib2.Http())
    else:
      return httplib2.Http()

  def request(self,
              path,
              method='GET',
              body=None,
              query_args=None,
              headers=None,
              cron=False,
              absolute_path=False,
              check_status=True,
              authenticated=True,
              pretend_date=None):
    """Sends an API request and returns a (response object, response content) tuple.

    Args:
      path: Relative URL path (such as "Participant/123"), unless absolute_path=True.
      pretend_date: A datetime, used by the server (if nonprod requests are allowed) for creation
          timestamps etc.
    """
    if absolute_path:
      url = path
    else:
      url = '{}/{}/{}'.format(self.instance, self.base_path, path)
    if query_args:
      args_str = '&'.join(
          '{}={}'.format(k, v) for k, v in query_args.iteritems())
      url = '{}?{}'.format(url, args_str)

    headers = copy.deepcopy(headers or {})

    if method == 'POST':
      headers.update(POST_HEADERS)
    if cron:
      # Provide the header the dev_appserver uses for cron calls.
      headers['X-Appengine-Cron'] = 'true'
    if pretend_date is not None:
      headers['x-pretend-date'] = pretend_date.isoformat()

    if authenticated:
      resp, content = self._http.request(url, method, headers=headers, body=body)
    else:
      # On dev_appserver, there is no way to tell if a request is authenticated or not.
      # This adds a header that we can use to reject 'unauthenticated' requests.  What this
      # is really testing is that the auth_required annotation is in all the right places.
      headers['unauthenticated'] = 'Yes'
      resp, content = httplib2.Http().request(url, method, headers=headers, body=body)

    client_log.info('%s for %s to %s', resp.status, method, url)
    details_level = (
        logging.WARNING if (check_status and resp.status != httplib.OK)
        else logging.DEBUG)
    if client_log.isEnabledFor(details_level):
      try:
        formatted_content = pprint.pformat(json.loads(content))
      except ValueError:
        formatted_content = content
      client_log.log(
          details_level,
          'Response headers: %s\nResponse content: %s', pprint.pformat(resp), formatted_content)

    if resp.status == httplib.UNAUTHORIZED:
      client_log.warn(
          'Unauthorized. If you expect this request to be allowed, try'
          'tools/install_config.sh --config config/config_dev.json --update')
    if check_status and resp.status != httplib.OK:
      raise HttpException(url, method, resp, content)
    if resp.get('etag'):
      self.last_etag = resp['etag']

    return resp, content

  def request_json(self,
                   path,
                   method='GET',
                   body=None,
                   query_args=None,
                   headers=None,
                   cron=False,
                   absolute_path=False,
                   pretend_date=None):
    json_body = None
    if body:
      json_body = json.dumps(body)
    elif method == 'POST':
      json_body = '{}'
    _, content = self.request(
        path,
        method,
        body=json_body,
        query_args=query_args,
        headers=headers,
        cron=cron,
        absolute_path=absolute_path,
        pretend_date=pretend_date)
    try:
      return json.loads(content)
    except ValueError:
      logging.error('Error decoding response content:\n%r', content)
      raise
