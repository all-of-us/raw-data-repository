"""Implementation of the metrics API"""

import participant

from protorpc import message_types
from protorpc import messages

class InvalidMetricException(BaseException):
  """Exception thrown when a invalid metric is specified."""


class Metrics(messages.Enum):
  """Predefined metric types"""
  NONE = 0
  PARTICIPANT_TOTAL = 1
  PARTICIPANT_MEMBERSHIP_TIER = 2

class MetricsResponseBucket(messages.Message):
  name = messages.StringField(1)
  value = messages.FloatField(2)

class MetricsResponse(messages.Message):
  bucket = messages.MessageField(MetricsResponseBucket, 1, repeated=True)

class MetricsRequest(messages.Message):
  metric = messages.EnumField(Metrics, 1, default='NONE')

_metric_map = {
    Metrics.PARTICIPANT_TOTAL: {
        'model': participant.Participant,
    },
    Metrics.PARTICIPANT_MEMBERSHIP_TIER: {
        'model': participant.Participant,
        'column': participant.Participant.membership_tier,
        'enum': participant.MembershipTier,
    },
}

class MetricService(object):

  def get_metrics(self, request):
    if request.metric not in _metric_map:
      raise InvalidMetricException(
          '{} is not a valid metric.'.format(request.metric))

    metric_config = _metric_map[request.metric]
    model = metric_config['model']
    column = metric_config['column'] if 'column' in metric_config else None
    enum = metric_config['enum'] if 'enum' in metric_config else None

    response = MetricsResponse()
    if enum:
      for name, val in enum.to_dict().iteritems():
        query = model.query(column == enum(val))
        bucket = MetricsResponseBucket()
        bucket.value = float(query.count())
        bucket.name = str(name)
        response.bucket.append(bucket)
    else:
      query = model.query()
      bucket = MetricsResponseBucket()
      bucket.value = float(query.count())
      bucket.name = 'total'
      response.bucket.append(bucket)

    return response


def _convert_name(result, enum):
  if result is None:
    return 'NULL'
  else:
    return str(enum(result))


SERVICE = MetricService()
