from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.orm.query import Query as SQLAlchemyQuery
from werkzeug.exceptions import BadRequest

from rdr_service.code_constants import UNSET
from rdr_service.model.utils import to_client_participant_id
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.awardee_insite import AwardeeInSite
from rdr_service.dao.base_dao import UpsertableDao
from rdr_service.query import Results, Query, FieldFilter, Operator


class AwardeeInSiteDao(UpsertableDao):
    def __init__(self):
        super().__init__(AwardeeInSite)
        self.total_result = None

    def get_id(self, obj: AwardeeInSite) -> int | None:
        """
        Return id if a participant already exists in the table,
        otherwise return None. This function is used for updating an
        existing record in MySQL table when streaming from BQ.
        """
        with self.session() as session:
            query = session.query(AwardeeInSite.id).filter_by(
                participantId=obj.participantId
            )
            return query.first()

    def make_query_filter(self, field_name: str, value: str) -> FieldFilter:
        """
        Return the filter object that will be used as argument to create a Query object.
        Called from _make_query in the API.
        """
        if field_name == "awardee":
            return FieldFilter("HPO.name", Operator.EQUALS, value)
        if field_name == "updatedSince":
            return FieldFilter(
                "AwardeeInSite.modified",
                Operator.GREATER_THAN_OR_EQUALS,
                datetime.strptime(value, "%Y-%m-%d"),
            )

    @staticmethod
    def add_awardee_col(query: SQLAlchemyQuery) -> SQLAlchemyQuery:
        """
        Add awardee (HPO name) to the table, so we can filter on it.
        This will be utilized to ensure that we only return participants
        associated with an awardee when that awardee calls the API.
        """
        query = (
            query.add_columns(HPO.name)
            .outerjoin(
                Organization, AwardeeInSite.organization == Organization.externalId
            )
            .join(HPO, HPO.hpoId == Organization.hpoId)
        )  # filter out obsolete HPOs?
        return query

    def _set_filters(
        self, query: SQLAlchemyQuery, filter_list: list[FieldFilter]
    ) -> SQLAlchemyQuery:
        """Add filters to SQLAlchemy query and then return that query"""
        str_to_model_map = {"HPO": HPO, "AwardeeInSite": AwardeeInSite}
        for field_filter in filter_list:
            model, col = field_filter.field_name.split(".")
            try:
                filter_attribute = getattr(str_to_model_map[model], col)
            except AttributeError:
                raise BadRequest(
                    f"No field named {field_filter.field_name} found on {model}."
                )
            query = self._add_filter(query, field_filter, filter_attribute)
        return query

    def _make_query(
        self, session: Session, query_definition: Query
    ) -> tuple[SQLAlchemyQuery, list]:
        """
        Return a SQLAlchemy query from a Query object passed in as a parameter.
        Also returns the name of the columns that pagination will use to create token.
        Adds pagination filter where it decodes the pagination token to grab only the
        records for the next page.
        """
        query: SQLAlchemyQuery = super()._initialize_query(session, query_definition)
        query = AwardeeInSiteDao.add_awardee_col(query)
        query = self._set_filters(query, query_definition.field_filters)
        order_by_field_names, order_by_fields, first_descending = (
            ["id"],
            [AwardeeInSite.id],
            False,
        )

        if query_definition.include_total:
            self.total_result = query.count()

        if query_definition.pagination_token:
            query = self._add_pagination_filter(
                query, query_definition, order_by_fields, first_descending
            )
        query.limit(query_definition.max_results + 1)
        return query, order_by_field_names

    def query(self, query_definition: Query) -> Results:
        with self.session() as session:
            # query_definition param passed from API file
            query, order_by_field_names = self._make_query(session, query_definition)
            items = query.all()
            if not items:
                return Results([])

        if len(items) > query_definition.max_results:
            page = items[0 : query_definition.max_results]
            pagination_token = self._make_pagination_token(
                items[query_definition.max_results - 1][0].asdict(),
                order_by_field_names,
            )
            return Results(
                page, pagination_token, more_available=True, total=self.total_result
            )

        else:
            pagination_token = (
                (
                    self._make_pagination_token(
                        items[-1][0].asdict(), order_by_field_names
                    )
                )
                if query_definition.always_return_token
                else None
            )
            return Results(
                items, pagination_token, more_available=False, total=self.total_result
            )

    def to_client_json(self, model: AwardeeInSite) -> dict:
        """
        Returns a response dict containing required values for a given row.
        If a column is null, it sets it to 'UNSET'.
        """
        result = model.asdict()

        for field in AwardeeInSite.internal_fields:
            del result[field]

        result["participantId"] = to_client_participant_id(result["participantId"])

        final_result = {}
        for key, value in result.items():
            if value == UNSET.lower():
                value = UNSET
            final_result[key] = value or UNSET
        return final_result
