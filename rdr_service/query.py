"""A query to run against a DAO (abstracted from the persistent level)."""
from protorpc import messages
from sqlalchemy import func, not_, or_

class Operator(messages.Enum):
    EQUALS = 0  # Case insensitive comparison for strings, exact comparison otherwise
    LESS_THAN = 1
    GREATER_THAN = 2
    LESS_THAN_OR_EQUALS = 3
    GREATER_THAN_OR_EQUALS = 4
    NOT_EQUALS = 5
    EQUALS_OR_NONE = 6
    # Note: we don't support contains or exact string comparison at this stage


class PropertyType(messages.Enum):
    STRING = 0
    DATE = 1
    DATETIME = 2
    ENUM = 3
    INTEGER = 4
    CODE = 5


class FieldJsonContainsFilter(object):
    """
  Filter json field using JSON_CONTAINS
  """

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def add_to_sqlalchemy_query(self, query, field):
        if self.value is None:
            return query.filter(field.is_(None))
        if self.operator == Operator.NOT_EQUALS:
            return query.filter(func.json_contains(field, self.value, "$") == 0)
        else:
            return query.filter(func.json_contains(field, self.value, "$") == 1)


class FieldLikeFilter(object):
    """
  Handle SQL Like filters
  """

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def add_to_sqlalchemy_query(self, query, field):
        if self.value is None:
            return query.filter(field.is_(None))
        if self.operator == Operator.NOT_EQUALS:
            return query.filter(not_(field.like("%{0}%".format(self.value))))
        else:
            return query.filter(field.like("%{0}%".format(self.value)))


class FieldFilter(object):
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value

    def add_to_sqlalchemy_query(self, query, field):
        if self.value is None:
            return query.filter(field.is_(None))
        query = {
            Operator.EQUALS: query.filter(field == self.value),
            Operator.LESS_THAN: query.filter(field < self.value),
            Operator.GREATER_THAN: query.filter(field > self.value),
            Operator.LESS_THAN_OR_EQUALS: query.filter(field <= self.value),
            Operator.GREATER_THAN_OR_EQUALS: query.filter(field >= self.value),
            Operator.NOT_EQUALS: query.filter(field != self.value),
            Operator.EQUALS_OR_NONE: query.filter(or_(field == self.value, field == None)),
        }.get(self.operator)
        if not query:
            raise ValueError("Invalid operator: %r." % self.operator)
        return query


class OrderBy(object):
    def __init__(self, field_name, ascending):
        self.field_name = field_name
        self.ascending = ascending


class Query(object):
    def __init__(
        self,
        field_filters,
        order_by,
        max_results,
        pagination_token,
        a_id=None,
        always_return_token=False,
        include_total=False,
        offset=False,
        options=None,
        invalid_filters=None,
        attributes=None
    ):
        self.field_filters = field_filters
        self.order_by = order_by
        self.offset = offset
        self.max_results = max_results
        self.pagination_token = pagination_token
        self.ancestor_id = a_id
        self.always_return_token = always_return_token
        self.include_total = include_total
        self.options = options
        self.invalid_filters = invalid_filters
        self.attributes = attributes


class Results(object):
    def __init__(self, items, pagination_token=None, more_available=False, total=None):
        self.items = items
        self.pagination_token = pagination_token
        self.more_available = more_available
        self.total = total
