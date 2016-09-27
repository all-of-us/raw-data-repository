"""Args parsing code shared with sample clients.
"""

import argparse
import httplib2
import json

from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = '../rest-api/test/test-data/test-client-cert.json'

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
  def __init__(self, base_path):
    args = self.parse_args()
    self.instance = args.instance
    self.base_path = base_path
    self.fetcher = self._get_fetcher()

  def parse_args(self):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--instance',
        type=str,
        help='The instance to hit, either https://xxx.appspot.com, '
        'or http://localhost:8080',
        default='https://pmi-rdr-api-test.appspot.com')
    return parser.parse_args()

  def _get_fetcher(self):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE,
                                                                   [SCOPE])
    return credentials.authorize(httplib2.Http())

  def request(self, path, method='GET', body=None, query_args=None):
    url = '{}/{}/{}'.format(self.instance, self.base_path, path)
    if query_args:
      args_str = '&'.join(
          '{}={}'.format(k,v) for k, v in query_args.iteritems())
      url = '{}?{}'.format(url, args_str)

    headers = {}
    if method == 'POST':
      headers = POST_HEADERS
    print '{} to {}'.format(method, url)
    resp, content = self.fetcher.request(
        url, method, headers=headers, body=body)
    print 'Response: {}'.format(resp.status)
    if resp.status != 200:
      print resp
      raise HttpException(
          '{}:{}\n---{}'.format(url, method, content), resp.status)

    return content

  def request_json(self, path, method='GET', body=None, query_args=None):
    json_body = None
    if body:
      json_body = json.dumps(body)
    response = self.request(path, method, body=json_body, query_args=query_args)
    print response
    return json.loads(response)
