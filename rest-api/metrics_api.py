"""The API definition for the metrics API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import config
import endpoints
import metrics

from protorpc import message_types
from protorpc import messages
from protorpc import protojson
from protorpc import remote

# Note that auth_level is missing.  This makes sure that the user is
# authenticated before the endpoint is called.  This is unnecessary as this
# check is insufficient, and we we are doing a string account whitelisting in
# check_auth().  Ideally we would turn it on anyway, but at the moment, it
# causes errors saying that this API is not enabled for the service account's
# project... And there is no way to enable the API. This API enabling should
# work fine once we upgrade to Cloud Endpoints 2.0.
metrics_api = endpoints.api(
    name='metrics',
    version='v1',
    allowed_client_ids=config.getSettingList(config.ALLOWED_CLIENT_ID),
    scopes=[endpoints.EMAIL_SCOPE])
@metrics_api
class MetricsApi(remote.Service):
  @endpoints.method(
      metrics.MetricsRequest,
      metrics.MetricsResponse,
      path='metrics',
      http_method='POST',
      name='metrics.calculate')
  def get_metric(self, request):
    api_util.check_auth()
    return metrics.SERVICE.get_metrics(request)
