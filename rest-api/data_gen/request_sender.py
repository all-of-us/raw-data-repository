import httplib
import json
import main

from clock import FakeClock


class InProcessRequestSender(object):
  """A request sender that invokes API endpoints on the server running in the same process.

  Used for creating fake data.
  """

  def send_request(self, request_time, method, local_path, request_data=None, headers=None):
    """Makes a JSON API call against the server and returns its response data.

    Args:
      request_time: the time at which the request should appear to occur
      method: HTTP method, as a string.
      local_path: The API endpoint's URL (excluding main.PREFIX).
      request_data: Parsed JSON payload for the request.
      headers: the headers for the request.
    """
    app = main.app
    with FakeClock(request_time):
      with app.app_context():
        with app.test_request_context(main.PREFIX + local_path,
                                      method=method,
                                      headers=headers,
                                      data=json.dumps(request_data)):
          try:
            rv = app.preprocess_request()
            if rv is None:
              # Main Dispatch
              rv = app.dispatch_request()
          #pylint: disable=broad-except
          except Exception as e:
            rv = app.handle_user_exception(e)

          response = app.make_response(rv)
          response = app.process_response(response)
    if response.status_code != httplib.OK:
      raise RuntimeError("Request failed: %s, %s, response = %s" % (local_path, request_data,
                                                                    response))
    return json.loads(response.data)
