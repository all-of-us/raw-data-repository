import http.client
import json

from rdr_service.services.flask import app, API_PREFIX
from rdr_service.clock import FakeClock


class InProcessClient(object):
    """A request sender that invokes API endpoints on the server running in the same process.

  Used for creating fake data.
  """
    def __init__(self, headers=None):
        self._headers = headers

    def request_json(self, local_path, method="GET", body=None, headers=None, pretend_date=None):
        """
        Makes a JSON API call against the server and returns its response data.
        :param local_path: The API endpoint's URL (excluding main.PREFIX).
        :param method: HTTP method, as a string.
        :param body: Parsed JSON payload for the request.
        :param headers: the headers for the request.
        :param pretend_date: the time at which the request should appear to occur.
        """

        merged_headers = {**self._headers, **headers} \
            if self._headers and headers else self._headers \
            if self._headers else headers

        with FakeClock(pretend_date):
            with app.app_context():
                with app.test_request_context(
                    API_PREFIX + local_path, method=method, headers=merged_headers, data=json.dumps(body)
                ):
                    try:
                        rv = app.preprocess_request()
                        if rv is None:
                            # Main Dispatch
                            rv = app.dispatch_request()
                    # pylint: disable=broad-except
                    except Exception as e:
                        rv = app.handle_user_exception(e)

                    response = app.make_response(rv)
                    response = app.process_response(response)
        if response.status_code != http.client.OK:
            raise RuntimeError("Request failed: %s, %s, response = %s" % (local_path, body, response))
        return json.loads(response.data)
