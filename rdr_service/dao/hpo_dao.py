
from sqlalchemy.orm import subqueryload

from rdr_service.code_constants import UNSET
from rdr_service.dao.base_dao import FhirMixin, FhirProperty
from rdr_service.dao.cache_all_dao import CacheAllDao
from rdr_service.dao.organization_dao import OrganizationDao, _FhirOrganization
from rdr_service.lib_fhir.fhirclient_1_0_6.models.domainresource import DomainResource
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.singletons import HPO_CACHE_INDEX


class _FhirAwardee(FhirMixin, DomainResource):
    """FHIR client definition of the expected JSON structure for an Awardee resource."""

    resource_name = "Awardee"
    _PROPERTIES = [
        FhirProperty("display_name", str, required=True),
        FhirProperty("type", str, required=True),
        FhirProperty("organizations", _FhirOrganization, is_list=True),
    ]


# Sort order for HPOs.
_ORDER_BY_ENDING = "name"


class HPODao(CacheAllDao):
    def __init__(self):
        super(HPODao, self).__init__(
            HPO,
            cache_index=HPO_CACHE_INDEX,
            cache_ttl_seconds=600,
            index_field_keys=["name"],
            order_by_ending=_ORDER_BY_ENDING,
        )

    def _validate_update(self, session, obj, existing_obj):
        # HPOs aren't versioned; suppress the normal check here.
        pass

    def get_id(self, obj):
        return obj.hpoId

    def get_by_name(self, name):
        return self._get_cache().index_maps["name"].get(name)

    def get_by_resource_id(self, resource_id):
        with self.session() as session:
            query = session.query(HPO).filter(HPO.resourceId == resource_id)
            return query.first()

    def get_with_children_in_session(self, session, obj_id):
        return session.query(HPO).options(subqueryload(HPO.organizations).subqueryload(Organization.sites)).get(obj_id)

    def get_with_children(self, obj_id):
        with self.session() as session:
            return self.get_with_children_in_session(session, obj_id)

    def _make_query(self, session, query_def):  # pylint: disable=unused-argument
        # For now, no filtering, ordering, or pagination is supported; fetch child organizations and
        # sites.
        return (
            session.query(HPO)
            .options(subqueryload(HPO.organizations).subqueryload(Organization.sites))
            .order_by(HPO.name),
            _ORDER_BY_ENDING,
        )

    def to_client_json(self, model, inactive_sites):
        return HPODao._to_json(model, inactive_sites)

    @staticmethod
    def _to_json(model, inactive_sites=False):
        resource = _FhirAwardee()
        resource.id = model.name
        resource.display_name = model.displayName
        if model.organizationType:
            resource.type = str(model.organizationType)
        else:
            resource.type = UNSET
        resource.organizations = [
            OrganizationDao._to_json(organization, inactive_sites) for organization in model.organizations
        ]
        json = resource.as_json()
        del json["resourceType"]
        return json
