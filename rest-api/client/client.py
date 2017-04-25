import argparse
import copy
import httplib
import httplib2
import json

from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
DEFAULT_INSTANCE = 'https://pmi-drc-api-test.appspot.com'
POST_HEADERS = {
    'Content-Type': 'application/json; charset=utf-8',
}


class HttpException(BaseException):
  """Indicates an http error occurred."""
  def __init__(self, message, code):
    super(HttpException, self).__init__(self, message)
    self.message = message
    self.code = code


class Client(object):
  def __init__(self, base_path, parse_cli=True, creds_file=None, default_instance=None):
    default_instance = default_instance or DEFAULT_INSTANCE
    if parse_cli:
      args = self._parse_args(default_instance)
      self.instance = args.instance
      if args.creds_file:
        creds_file = args.creds_file
    else:
      self.instance = default_instance
    self.base_path = base_path
    if not creds_file and 'localhost' not in self.instance:
      raise ValueError('Client requires credentials for non-local instance %r.' % self.instance)
    self.creds_file = creds_file
    self._http = self._get_authorized_http()
    self.last_etag = None

  def _parse_args(self, default_instance):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--instance',
        type=str,
        help='The instance to hit, either https://xxx.appspot.com, '
        'or http://localhost:8080',
        default=default_instance)
    parser.add_argument(
        '--creds_file',
        type=str,
        help='Path to a credentials file to use when talking to the server.')
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

    print '{} to {}'.format(method, url)
    if authenticated:
      resp, content = self._http.request(url, method, headers=headers, body=body)
    else:
      # On dev_appserver, there is no way to tell if a request is authenticated or not.
      # This adds a header that we can use to reject 'unauthenticated' requests.  What this
      # is really testing is that the auth_required annotation is in all the right places.
      headers['unauthenticated'] = 'Yes'
      resp, content = httplib2.Http().request(url, method, headers=headers, body=body)
    print resp

    if resp.status == httplib.UNAUTHORIZED:
      print 'If you expect this request to be allowed, try'
      print 'tools/install_config.sh --config config/config_dev.json --update'
    if check_status and resp.status != httplib.OK:
      raise HttpException(
          '{}:{} - {}\n---{}'.format(url, method, resp.status, content), resp.status)
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
    return json.loads(content)
