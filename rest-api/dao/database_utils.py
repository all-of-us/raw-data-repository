"""Helpers for querying the SQL database."""


def get_sql_and_params_for_array(arr, name_prefix):
  """Returns an SQL expression and associated params dict for an array of values.

  SQLAlchemy can't format array parameters. Work around it by building the :param style expression
  and creating a dictionary of individual params for that.
  """
  array_values = {}
  for i, v in enumerate(arr):
    array_values['%s%d' % (name_prefix, i)] = v
  sql_expr = '(%s)' % ','.join([':' + param_name for param_name in array_values])
  return sql_expr, array_values
