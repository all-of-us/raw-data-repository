"""Args parsing code shared with sample clients.
"""

import argparse
import httplib2
import json
import copy

from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = '../rest-api/test/test-data/test-client-cert.json'
DEFAULT_INSTANCE = 'https://pmi-drc-api-test.appspot.com'
POST_HEADERS = {
    'Content-Type': 'application/json; charset=UTF-8',
}

class HttpException(BaseException):
  """Indicates an http error occurred."""
  def __init__(self, message, code):
    super(HttpException, self).__init__(self, message)
    self.message = message
    self.code = code


class Client(object):

  def __init__(self, base_path, parse_cli=True, creds_file=CREDS_FILE, default_instance=None):
    default_instance = default_instance or DEFAULT_INSTANCE
    if parse_cli:
      args = self.parse_args(default_instance)
      self.instance = args.instance
    else:
      self.instance = default_instance
    self.base_path = base_path
    self.creds_file = creds_file
    self.fetcher = self._get_fetcher()
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

  def _get_fetcher(self):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        self.creds_file, [SCOPE])
    return credentials.authorize(httplib2.Http())

  def request(self,
              path,
              method='GET',
              body=None,
              query_args=None,
              headers=None,
              dev_appserver_admin=False,
              test_unauthenticated=True):
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
      if resp.status != 401:
        raise HttpException(
            'API is allowing unauthenticated {} to {}. Status: {}'.format(method, url, resp.status),
            resp.status)
      else:
        print 'Not allowed. Good!'

    if dev_appserver_admin:
      headers['Cookie:'] = 'dev_appserver_login="test@example.com:True:0"'
    print '{} to {}'.format(method, url)
    resp, content = self.fetcher.request(
        url, method, headers=headers, body=body)

    print resp

    if resp.status != 200:
      raise HttpException(
          '{}:{} - {}\n---{}'.format(url, method, resp.status, content), resp.status)

    if resp['content-disposition'] != 'attachment':
      raise HttpException(
          'content-disposition header is set to {}'.format(resp['content-disposition']))
    if resp['x-content-type-options'] != 'nosniff':
      raise HttpException(
          'x-content-type-options header is set to {}'.format(resp['x-content-type-options']))
    if resp['content-type'] != 'application/json':
      raise HttpException(
          'content-type header is set to {}'.format(resp['content-type']))
    if resp.get('etag'):
      self.last_etag = resp['etag']

    return content

  def request_json(self,
                   path,
                   method='GET',
                   body=None,
                   query_args=None,
                   headers=None,
                   dev_appserver_admin=False,
                   test_unauthenticated=True):
    json_body = None
    if body:
      json_body = json.dumps(body)
    response = self.request(path,
                            method,
                            body=json_body,
                            query_args=query_args,
                            headers=headers,
                            dev_appserver_admin=dev_appserver_admin,
                            test_unauthenticated=test_unauthenticated)

    return json.loads(response)
