"""Implementation of the metrics API"""

import db
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
        'table': 'participant'
    },
    Metrics.PARTICIPANT_MEMBERSHIP_TIER: {
        'column': 'membership_tier',
        'enum': participant.MembershipTier,
        'table': 'participant',
    },
}

class MetricService(object):

  @db.connection
  def get_metrics(self, connection, request):
    if request.metric not in _metric_map:
      raise InvalidMetricException(
          '{} is not a valid metric.'.format(request.metric))

    metric_config = _metric_map[request.metric]
    table = metric_config['table']
    column = metric_config['column'] if 'column' in metric_config else None
    enum = metric_config['enum'] if 'enum' in metric_config else None

    group_by = None
    select = ['count(*)']
    if 'column' in metric_config:
      group_by = 'group by {}'.format(column)
      select.append(column)

    query = 'select {} from {} {}'.format(','.join(select), table, group_by)
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    response = MetricsResponse()
    for result in results:
      bucket = MetricsResponseBucket()
      bucket.value = float(result[0])
      bucket.name = 'total' if not enum else _convert_name(result[1], enum)
      response.bucket.append(bucket)

    return response


def _convert_name(result, enum):
  if result is None:
    return 'NULL'
  else:
    return str(enum(result))


SERVICE = MetricService()
