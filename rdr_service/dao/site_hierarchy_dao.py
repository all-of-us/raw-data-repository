from typing import Dict

from flask import jsonify, Response
from sqlalchemy import or_
from sqlalchemy.orm import Session, query
from werkzeug.exceptions import BadRequest

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.site import Site
from rdr_service.model.organization import Organization
from rdr_service.model.hpo import HPO
from rdr_service.model.site_enums import ObsoleteStatus


class SiteHierarchyDao(BaseDao):
    _QUERY_PARAM_TO_COLUMN_MAPPING = {
        "awardee_id": HPO.name,
        "organization_id": Organization.externalId,
        "google_group": Site.googleGroup,
    }

    def __init__(self):
        super().__init__(Site)

    def get_id(self, obj):
        pass

    def handle_list_queries(self, **kwargs: Dict[str, str]) -> Response:
        """
        Return information on site hierarchy based on query parameters.

        :param kwargs: Contains the query params extracted from the URL.
        :return: A dictionary representing the response to be sent to the client.
        :raises BadRequest: If a client provides an incorrect query param.

        **Note:**
        - Currently handles 3 query params: awardee_id, organization_id, google_group
            Any query parameters not listed above will be ignored.
        - The function returns a list of site hierarchy models that match the specified query parameters.
        """
        self._validate_query_params(**kwargs)

        filters = [
            self._QUERY_PARAM_TO_COLUMN_MAPPING[key] == value
            for key, value in kwargs.items()
            if key in self._QUERY_PARAM_TO_COLUMN_MAPPING
        ]
        with self.session() as session:
            query_ = self.get_site_hierarchy_query(session)
            if filters:
                query_ = query_.filter(*filters)
            result = query_.all()

        response = {"data": [model._asdict() for model in result]}
        return jsonify(response)

    def _validate_query_params(self, **kwargs: Dict[str, str]):
        """Check if client uses valid query params listed in _QUERY_PARAM_MAPPING."""
        for key in kwargs:
            if key not in self._QUERY_PARAM_TO_COLUMN_MAPPING:
                raise BadRequest(
                    f"Invalid query parameter(s). "
                    f"Supported parameters are: {', '.join(self._QUERY_PARAM_TO_COLUMN_MAPPING.keys())}"
                )

    def get_site_hierarchy_query(self, session: Session) -> query.Query:
        """Initialize and return a SQLAlchemy query for active sites, their organizations, and HPOs.
        The query is designed to retrieve data from the 'Site', 'Organization', and 'HPO' tables and filter for active
        sites. It returns information labeled as 'awardee_id' (HPO name), 'organization_id' (Organization external ID),
        'google_group' (Site's Google Group), and 'site_name' (Site name).
        """
        return (
            session.query(
                HPO.name.label("awardee_id"),
                Organization.externalId.label("organization_id"),
                Site.googleGroup.label("google_group"),
                Site.siteName.label("site_name"),
            )
            .join(Organization, Organization.organizationId == Site.organizationId)
            .join(HPO, HPO.hpoId == Organization.hpoId)
            .filter(
                or_(Site.isObsolete == None, Site.isObsolete == ObsoleteStatus.ACTIVE)
            )
        )
