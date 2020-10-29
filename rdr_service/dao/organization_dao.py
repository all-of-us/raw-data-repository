from rdr_service import clock
from rdr_service.dao.base_dao import FhirMixin, FhirProperty
from rdr_service.dao.cache_all_dao import CacheAllDao
from rdr_service.dao.site_dao import SiteDao, _FhirSite
from rdr_service.lib_fhir.fhirclient_1_0_6.models.backboneelement import BackboneElement
from rdr_service.model.organization import Organization
from rdr_service.participant_enums import make_primary_provider_link_for_id
from rdr_service.singletons import ORGANIZATION_CACHE_INDEX


class _FhirOrganization(FhirMixin, BackboneElement):
    """FHIR client definition of the expected JSON structure for an Organization resource"""

    resource_name = "Organization"
    _PROPERTIES = [FhirProperty("display_name", str, required=True), FhirProperty("sites", _FhirSite, is_list=True)]


class OrganizationDao(CacheAllDao):
    def __init__(self):
        super(OrganizationDao, self).__init__(
            Organization, cache_index=ORGANIZATION_CACHE_INDEX, cache_ttl_seconds=600, index_field_keys=["externalId"]
        )

    def _validate_update(self, session, obj, existing_obj):
        # Organizations aren't versioned; suppress the normal check here.
        pass

    def _do_update(self, session, obj, existing_obj):
        update_participants = False
        if obj.hpoId != existing_obj.hpoId:
            update_participants = True
            new_hpo_id = obj.hpoId
        super(OrganizationDao, self)._do_update(session, obj, existing_obj)
        if update_participants:
            provider_link = make_primary_provider_link_for_id(new_hpo_id)

            participant_sql = """
            UPDATE participant
            SET hpo_id = :hpo_id,
                provider_link = :provider_link
            WHERE organization_id = :org_id;
            """

            participant_summary_sql = """
            UPDATE participant_summary
            SET hpo_id = :hpo_id,
                last_modified = :now
            WHERE organization_id = :org_id;
            """

            participant_history_sql = """
            UPDATE participant_history
            SET hpo_id = :hpo_id,
                provider_link = :provider_link
            WHERE organization_id = :org_id;
            """
            params = {
                "hpo_id": new_hpo_id,
                "provider_link": provider_link,
                "org_id": existing_obj.organizationId,
                "now": clock.CLOCK.now(),
            }

            session.execute(participant_sql, params)
            session.execute(participant_summary_sql, params)
            session.execute(participant_history_sql, params)

    def get_id(self, obj):
        return obj.organizationId

    def get_by_external_id(self, external_id):
        return self._get_cache().index_maps["externalId"].get(external_id)

    def get_by_resource_id(self, resource_id):
        with self.session() as session:
            query = session.query(Organization).filter(Organization.resourceId == resource_id)
            return query.first()

    @staticmethod
    def _to_json(model, inactive_sites, obsolete_filters):
        resource = _FhirOrganization()
        resource.id = model.externalId
        resource.display_name = model.displayName
        if inactive_sites:
            resource.sites = [
                SiteDao._to_json(site) for site in model.sites
                if site.isObsolete in obsolete_filters
            ]
        else:
            resource.sites = [
                SiteDao._to_json(site)
                for site in model.sites
                if site.siteStatus and site.siteStatus == site.siteStatus.ACTIVE
                and site.isObsolete in obsolete_filters
            ]
        return resource
