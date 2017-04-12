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
      args = self.parse_args(default_instance)
      self.instance = args.instance
    else:
      self.instance = default_instance
    self.base_path = base_path
    if not creds_file and 'localhost' not in self.instance:
      raise ValueError('Client requires credentials for non-local instance %r.' % self.instance)
    self.creds_file = creds_file
    self._http = self._get_authorized_http()
    self.last_etag = None

  def parse_args(self, default_instance):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--instance',
        type=str,
        help='The instance to hit, either https://xxx.appspot.com, '
        'or http://localhost:8080',
        default=default_instance)
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
              test_unauthenticated=True,
              absolute_path=False):
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

    if test_unauthenticated:
      unauthenticated_headers = copy.deepcopy(headers)
      # On dev_appserver, there is no way to tell if a request is authenticated or not.
      # This adds a header that we can use to reject 'unauthenticated' requests.  What this
      # is really testing is that the auth_required annotation is in all the right places.
      unauthenticated_headers['unauthenticated'] = 'YES'
      print 'Trying unauthenticated {} to {}.'.format(method, url)
      resp, content = httplib2.Http().request(
          url, method, headers=unauthenticated_headers, body=body)
      if resp.status == httplib.INTERNAL_SERVER_ERROR:
        raise HttpException(
            'API server error ({}) for {} to {}.'.format(resp.status, method, url),
            resp.status)
      if resp.status != httplib.UNAUTHORIZED:
        raise HttpException(
            'API is allowing unauthenticated {} to {}. Status: {}'.format(method, url, resp.status),
            resp.status)
      else:
        print 'Not allowed. Good!'

    if cron:
      # Provide the header the dev_appserver uses for cron calls.
      headers['X-Appengine-Cron'] = 'true'

    print '{} to {}'.format(method, url)
    resp, content = self._http.request(
        url, method, headers=headers, body=body)

    print resp

    if resp.status == httplib.UNAUTHORIZED:
      print 'If you expect this request to be allowed, try'
      print 'tools/install_config.sh --config config/config_dev.json --update'
    if resp.status != httplib.OK:
      raise HttpException(
          '{}:{} - {}\n---{}'.format(url, method, resp.status, content), resp.status)

    for required_header, required_value in (
        ('content-disposition', 'attachment; filename="f.txt"'),
        ('content-type', 'application/json; charset=utf-8'),
        ('x-content-type-options', 'nosniff')):
      if resp[required_header] != required_value:
        raise HttpException(
            'Header %r is set to %r, expected %r.'
            % (required_header, resp[required_header], required_value),
            httplib.INTERNAL_SERVER_ERROR)
    if resp.get('etag'):
      self.last_etag = resp['etag']

    return content

  def request_json(self,
                   path,
                   method='GET',
                   body=None,
                   query_args=None,
                   headers=None,
                   cron=False,
                   test_unauthenticated=True,
                   absolute_path=False):
    json_body = None
    if body:
      json_body = json.dumps(body)
    elif method == 'POST':
      json_body = '{}'
    response = self.request(path,
                            method,
                            body=json_body,
                            query_args=query_args,
                            headers=headers,
                            cron=cron,
                            test_unauthenticated=test_unauthenticated,
                            absolute_path=absolute_path)
    return json.loads(response)
