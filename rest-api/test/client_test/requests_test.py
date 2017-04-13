import httplib
import unittest

from base import BaseClientTest


class RequestsTest(BaseClientTest):
  """Tests basic mechanics of requests: authorization and headers."""
  def test_unauthenticated(self):
    response, _ = self.client.request(
        'Participant',
        method='POST',
        body='{}',
        unauthenticated=True,
        check_status=False)
    self.assertEquals(response.status, httplib.UNAUTHORIZED)

  def test_header_values(self):
    response, _ = self.client.request('Participant', method='POST', body='{}')
    for required_header, required_value in (
        ('content-disposition', 'attachment; filename="f.txt"'),
        ('content-type', 'application/json; charset=utf-8'),
        ('x-content-type-options', 'nosniff')):
      self.assertEquals(response.get(required_header), required_value,
            'Response header %r was set to %r, expected %r.'
            % (required_header, response.get(required_header), required_value))


if __name__ == '__main__':
  unittest.main()
