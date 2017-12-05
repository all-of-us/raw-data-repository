from dao.cache_all_dao import CacheAllDao
from model.site import Site
from singletons import SITE_CACHE_INDEX
from dao.base_dao import FhirMixin, FhirProperty
from fhirclient.models.backboneelement import BackboneElement

class _FhirSite(FhirMixin, BackboneElement):
  """FHIR client definition of the expected JSON structure for a Site resource"""
  resource_name = 'Site'
  _PROPERTIES = [
    FhirProperty('display_name', str, required=True),
  ]

class SiteDao(CacheAllDao):
  def __init__(self):
    super(SiteDao, self).__init__(Site, cache_index=SITE_CACHE_INDEX,
                                  cache_ttl_seconds=600, index_field_keys=['googleGroup'])

  def _validate_update(self, session, obj, existing_obj):
    # Sites aren't versioned; suppress the normal check here.
    pass

  def get_id(self, obj):
    return obj.siteId

  def get_by_google_group(self, google_group):
    return self._get_cache().index_maps['googleGroup'].get(google_group)

  @staticmethod
  def _to_json(model):
    resource = _FhirSite()
    resource.id = model.googleGroup
    resource.display_name = model.siteName
    return resource