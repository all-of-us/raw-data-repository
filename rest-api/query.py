"""A query to run against a DAO (abstracted from the persistent level)."""
from protorpc import messages

class Operator(messages.Enum):
  EQUALS = 0 # Case insensitive comparison for strings, exact comparison otherwise
  LESS_THAN = 1
  GREATER_THAN = 2
  LESS_THAN_OR_EQUALS = 3
  GREATER_THAN_OR_EQUALS = 4
  # Note: we don't support contains, not equals, or exact string comparison at this stage

class PropertyType(messages.Enum):
  STRING = 0
  DATE = 1
  ENUM = 2

class FieldFilter:
  def __init__(self, field_name, operator, value):
    self.field_name = field_name
    self.operator = operator
    self.value = value

class OrderBy:
  def __init__(self, field_name, ascending):
    self.field_name = field_name
    self.ascending = ascending

class Query:
  def __init__(self, field_filters, order_by, max_results, pagination_token):
    self.field_filters = field_filters
    self.order_by = order_by
    self.max_results = max_results
    self.pagination_token = pagination_token

class Results:
  def __init__(self, items, pagination_token):
    self.items = items
    self.pagination_token = pagination_token


