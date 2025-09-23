from flask import request
from werkzeug.exceptions import BadRequest, InternalServerError

from rdr_service.query import Query, Results
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.app_util import auth_required, get_validated_user_info
from rdr_service.api_util import AWARDEE, RDR, PPSC
from rdr_service.dao.awardee_insite_dao import AwardeeInSiteDao


AWARDEE_INSITE_PAGINATION_MAX_RESULTS = 1000


class AwardeeInSiteApi(BaseApi):
    def __init__(self):
        super().__init__(AwardeeInSiteDao())
        self.awardee = None

    @auth_required([RDR] + [AWARDEE] + [PPSC])
    def get(self, id_=None, participant_id=None):
        log_api_request(log=request.log_record)

        _, user_info = get_validated_user_info()

        # Get the "awardee" linked to the user_email from the config
        if AWARDEE in user_info["roles"]:
            try:
                self.awardee = user_info["awardee"]
            except KeyError:
                raise InternalServerError("Config error for awardee")

        # RDR & PPSC can pass an awardee query param with an awardee name to get the data
        if RDR in user_info["roles"] or PPSC in user_info["roles"]:
            self.awardee = request.args.get("awardee")
            if not self.awardee:
                raise BadRequest(
                    "Awardee not found. Please pass an awardee to the query"
                )

        return self._query()

    def _make_query(self, check_invalid: bool = True) -> Query:
        """
        Returns a Query object, setting properties like the max_results to be returned
        in a page and field filters (awardee name, last modified).
        """
        query_definition = super()._make_query(check_invalid)

        field_filters = []
        if self.awardee:
            field_filters.append(self.dao.make_query_filter("awardee", self.awardee))

        if len(request.args) > 0:
            for key, value in request.args.items(multi=True):
                if key == "updatedSince":
                    field_filters.append(self.dao.make_query_filter(key, value))

        query_definition.field_filters = field_filters
        query_definition.max_results = AWARDEE_INSITE_PAGINATION_MAX_RESULTS

        return query_definition

    def _query(self) -> dict:
        """
        Called in GET function. Creates a Query object and then runs that
        query and return the payload.
        """
        query_definition: Query = self._make_query()
        results: Results = self.dao.query(query_definition)
        payload: dict = self._make_bundle(results)
        return payload

    def _make_bundle(self, results: Results) -> dict:
        """
        Return response in a dict. If pagination token exists (meaning there is a next page), it creates
        a URL so that the client can call that URL to retrieve the next page. The URL to get the
        next page, and the participants results are added into the dictionary to be sent to the client.

        :param results: Result object containing the results of the query in
            the item attribute and pagination token in the pagination_token attribute.
        :return: Payload that will be sent in the GET request.
        """

        from rdr_service import main

        bundle_dict = {"resourceType": "Bundle", "type": "searchset"}
        if results.pagination_token:
            query_params = request.args.copy()
            query_params["_token"] = results.pagination_token
            next_url = main.api.url_for(
                self.__class__, _external=True, **query_params.to_dict(flat=False)
            )
            bundle_dict["link"] = [{"relation": "next", "url": next_url}]

        entries = []
        for item in results.items:
            resource = self._make_response(item[0])  # item = [awardee_model, HPO.name]
            entries.append({"resource": resource})

        bundle_dict["entry"] = entries
        if results.total is not None:
            bundle_dict["total"] = results.total
        return bundle_dict
