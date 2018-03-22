from dao.cache_all_dao import CacheAllDao
from model.site import Site
from singletons import SITE_CACHE_INDEX
from dao.base_dao import FhirMixin, FhirProperty
from fhirclient.models.address import Address
from fhirclient.models.backboneelement import BackboneElement
from fhirclient.models import fhirdate


def _to_fhir_date(dt):
  if not dt:
    return None
  return fhirdate.FHIRDate.with_json(dt.isoformat())

class _FhirSite(FhirMixin, BackboneElement):
  """FHIR client definition of the expected JSON structure for a Site resource"""
  resource_name = 'Site'
  _PROPERTIES = [
    FhirProperty('display_name', str, required=True),
    FhirProperty('mayolink_client_number', long),
    FhirProperty('site_status', str, required=True),
    FhirProperty('enrolling_status', str),
    FhirProperty('launch_date', fhirdate.FHIRDate),
    FhirProperty('notes', str),
    FhirProperty('latitude', float),
    FhirProperty('longitude', float),
    FhirProperty('time_zone_id', str),
    FhirProperty('directions', str),
    FhirProperty('physical_location_name', str),
    FhirProperty('address', Address),
    FhirProperty('phone_number', str),
    FhirProperty('admin_emails', str, is_list=True),
    FhirProperty('link', str)
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
    if model.mayolinkClientNumber:
      resource.mayolink_client_number = long(model.mayolinkClientNumber)
    resource.site_status = str(model.siteStatus)
    if model.enrollingStatus:
      resource.enrolling_status = str(model.enrollingStatus)
    resource.launch_date = _to_fhir_date(model.launchDate)
    resource.notes = model.notes
    resource.latitude = model.latitude
    resource.longitude = model.longitude
    resource.time_zone_id = model.timeZoneId
    resource.directions = model.directions
    resource.physical_location_name = model.physicalLocationName
    address = Address()
    resource.address = address
    address.line = []
    if model.address1:
      address.line.append(model.address1)
    if model.address2:
      address.line.append(model.address2)
    address.city = model.city
    address.state = model.state
    address.postalCode = model.zipCode
    resource.phone_number = model.phoneNumber
    resource.admin_emails = ([email.strip() for email in model.adminEmails.split(',')]
                             if model.adminEmails  else [])
    resource.link = model.link
    return resource
