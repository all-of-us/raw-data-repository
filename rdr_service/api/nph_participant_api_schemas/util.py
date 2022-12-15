from dataclasses import dataclass, field
from typing import Optional, Dict
from graphene import List

from sqlalchemy.orm import Query, aliased


@dataclass
class SortContext:
    query: Query
    order_expression: Optional = None
    filter_expressions: List = field(default_factory=list)
    references: Dict = field(default_factory=dict)
    join_expressions: List = field(default_factory=list)
    sort_table = None
    table = None

    def set_table(self, value):
        self.table = value

    def set_sort_table(self, reference):
        self.sort_table = self.references[reference]

    def add_filter(self, expr):
        self.filter_expressions.append(expr)

    def add_ref(self, table, ref_name):
        self.references[ref_name] = aliased(table)
        return self

    def add_join(self, joined_table, join_expr):
        self.join_expressions.append((joined_table, join_expr))
        return self

    def set_order_expression(self, expr):
        self.order_expression = expr

    def get_resulting_query(self):
        resulting_query = self.query

        for table, expr in self.join_expressions:
            resulting_query = resulting_query.join(table, expr)
        for expr in self.filter_expressions:
            resulting_query = resulting_query.filter(expr)

        return resulting_query.order_by(self.order_expression)


def validation_error_message(errors):

    return {"error": [error.formatted for error in errors]}


def error_message(message):

    return {"errors": message}
