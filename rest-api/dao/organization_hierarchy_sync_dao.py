import os
import logging
import googlemaps
import lib_fhir.fhirclient_3_0_0.models.organization

from lib_fhir.fhirclient_3_0_0.models.fhirabstractbase import FHIRValidationError
from werkzeug.exceptions import BadRequest
from dao.base_dao import BaseDao
from model.hpo import HPO
from model.organization import Organization
from model.site import Site
from model.site_enums import ObsoleteStatus, SiteStatus, EnrollingStatus, DigitalSchedulingStatus
from dao.hpo_dao import HPODao
from participant_enums import OrganizationType
from dao.organization_dao import OrganizationDao
from dao.site_dao import SiteDao
from dateutil.parser import parse

_FHIR_SYSTEM_PREFIX = 'http://all-of-us.org/fhir/sites/'


class OrganizationHierarchySyncDao(BaseDao):
  def __init__(self):
    super(OrganizationHierarchySyncDao, self).__init__(HPO)
    self.hpo_dao = HPODao()
    self.organization_dao = OrganizationDao()
    self.site_dao = SiteDao()

  def from_client_json(self, resource_json, id_=None, expected_version=None, client_id=None):  # pylint: disable=unused-argument
    try:
      fhir_org = lib_fhir.fhirclient_3_0_0.models.organization.Organization(resource_json)
    except FHIRValidationError:
      raise BadRequest('Invalid FHIR format in payload data.')

    if not fhir_org.meta or not fhir_org.meta.versionId:
      raise BadRequest('No versionId info found in payload data.')
    try:
      fhir_org.version = int(fhir_org.meta.versionId)
    except ValueError:
      raise BadRequest('Invalid versionId in payload data.')

    return fhir_org

  def to_client_json(self, hierarchy_org_obj):
    return hierarchy_org_obj.as_json()

  def get_etag(self, id_, pid):  # pylint: disable=unused-argument
    return None

  def update(self, hierarchy_org_obj):
    obj_type = self._get_type(hierarchy_org_obj)

    operation_funcs = {
      'AWARDEE': self._update_awardee,
      'ORGANIZATION': self._update_organization,
      'SITE': self._update_site
    }

    if obj_type not in operation_funcs:
      raise BadRequest('No awardee-type info found in payload data.')

    operation_funcs[obj_type](hierarchy_org_obj)

  def _update_awardee(self, hierarchy_org_obj):
    awardee_id = self._get_value_from_identifier(hierarchy_org_obj,
                                                 _FHIR_SYSTEM_PREFIX + 'organization-identifier')
    if awardee_id is None:
      raise BadRequest('No organization-identifier info found in payload data.')
    is_obsolete = ObsoleteStatus('OBSOLETE') if not hierarchy_org_obj.active else None
    awardee_type = 'DV'  # TODO waiting for PTSC's document

    try:
      organization_type = OrganizationType(awardee_type)
      if organization_type == OrganizationType.UNSET:
        organization_type = None
    except TypeError:
      raise BadRequest('Invalid organization type {} for awardee {}'
                       .format(awardee_type, awardee_id))

    entity = HPO(name=awardee_id.upper(),
                 displayName=hierarchy_org_obj.name,
                 organizationType=organization_type,
                 isObsolete=is_obsolete)

    existing_map = {entity.name: entity for entity in self.hpo_dao.get_all()}
    existing_entity = existing_map.get(entity.name)

    with self.hpo_dao.session() as session:
      if existing_entity:
        new_dict = entity.asdict()
        new_dict['hpoId'] = None
        existing_dict = existing_entity.asdict()
        existing_dict['hpoId'] = None
        if existing_dict == new_dict:
          logging.info('Not updating {}.'.format(new_dict['name']))
        else:
          existing_entity.displayName = entity.displayName
          existing_entity.organizationType = entity.organizationType
          existing_entity.isObsolete = entity.isObsolete
          self.hpo_dao.update_with_session(session, existing_entity)
      else:
        entity.hpoId = len(existing_map)
        self.hpo_dao.insert_with_session(session, entity)

  def _update_organization(self, hierarchy_org_obj):
    organization_id = self._get_value_from_identifier(hierarchy_org_obj,
                                                      _FHIR_SYSTEM_PREFIX +
                                                      'organization-identifier')
    if organization_id is None:
      raise BadRequest('No organization-identifier info found in payload data.')
    is_obsolete = ObsoleteStatus('OBSOLETE') if not hierarchy_org_obj.active else None
    awardee_id = hierarchy_org_obj.partOf.identifier.value

    hpo = self.hpo_dao.get_by_name(awardee_id.upper())
    if hpo is None:
      raise BadRequest('Invalid awardee ID {} importing organization {}'
                       .format(awardee_id, organization_id))

    entity = Organization(externalId=organization_id.upper(),
                          displayName=hierarchy_org_obj.name,
                          hpoId=hpo.hpoId,
                          isObsolete=is_obsolete)
    existing_map = {entity.externalId: entity for entity in self.organization_dao.get_all()}
    existing_entity = existing_map.get(entity.externalId)
    with self.organization_dao.session() as session:
      if existing_entity:
        new_dict = entity.asdict()
        new_dict['organizationId'] = None
        existing_dict = existing_entity.asdict()
        existing_dict['organizationId'] = None
        if existing_dict == new_dict:
          logging.info('Not updating {}.'.format(new_dict['externalId']))
        else:
          existing_entity.displayName = entity.displayName
          existing_entity.hpoId = entity.hpoId
          existing_entity.isObsolete = entity.isObsolete
          self.organization_dao.update_with_session(session, existing_entity)
      else:
        self.organization_dao.insert_with_session(session, entity)

  def _update_site(self, hierarchy_org_obj):
    google_group = self._get_value_from_identifier(hierarchy_org_obj,
                                                   _FHIR_SYSTEM_PREFIX + 'organization-identifier')
    if google_group is None:
      raise BadRequest('No organization-identifier info found in payload data.')
    google_group = google_group.lower()
    is_obsolete = ObsoleteStatus('OBSOLETE') if not hierarchy_org_obj.active else None
    organization_id = hierarchy_org_obj.partOf.identifier.value

    organization = self.organization_dao.get_by_external_id(organization_id.upper())
    if organization is None:
      raise BadRequest('Invalid organization ID {} importing site {}'
                       .format(organization_id, google_group))

    launch_date = None
    launch_date_str = self._get_value_from_extention(hierarchy_org_obj,
                                                     _FHIR_SYSTEM_PREFIX + 'anticipatedLaunchDate',
                                                     'valueDate')
    if launch_date_str:
      try:
        launch_date = parse(launch_date_str).date()
      except ValueError:
        raise BadRequest('Invalid launch date {} for site {}'.format(launch_date_str, google_group))

    name = hierarchy_org_obj.name
    mayolink_client_number = None
    mayolink_client_number_str = self._get_value_from_identifier(hierarchy_org_obj,
                                                                 _FHIR_SYSTEM_PREFIX +
                                                                 'mayo-link-identifier')
    if mayolink_client_number_str:
      try:
        mayolink_client_number = int(mayolink_client_number_str)
      except ValueError:
        raise BadRequest('Invalid Mayolink Client # {} for site {}'.format(
          mayolink_client_number_str, google_group))

    notes = self._get_value_from_extention(hierarchy_org_obj, _FHIR_SYSTEM_PREFIX + 'notes')

    site_status_bool = self._get_value_from_extention(hierarchy_org_obj,
                                                      _FHIR_SYSTEM_PREFIX +
                                                      'schedulingStatusActive',
                                                      'valueBoolean')
    try:
      site_status = SiteStatus('ACTIVE' if site_status_bool else 'INACTIVE')
    except TypeError:
      raise BadRequest('Invalid site status {} for site {}'.format(site_status, google_group))

    enrolling_status_bool = self._get_value_from_extention(hierarchy_org_obj,
                                                           _FHIR_SYSTEM_PREFIX +
                                                           'enrollmentStatusActive',
                                                           'valueBoolean')
    try:
      enrolling_status = EnrollingStatus('ACTIVE' if enrolling_status_bool else 'INACTIVE')
    except TypeError:
      raise BadRequest('Invalid enrollment site status {} for site {}'
                       .format(enrolling_status_bool, google_group))

    digital_scheduling_bool = self._get_value_from_extention(hierarchy_org_obj,
                                                             _FHIR_SYSTEM_PREFIX +
                                                             'digitalSchedulingStatusActive',
                                                             'valueBoolean')
    try:
      digital_scheduling_status = DigitalSchedulingStatus('ACTIVE' if digital_scheduling_bool
                                                          else 'INACTIVE')
    except TypeError:
      raise BadRequest('Invalid digital scheduling status {} for site {}'
                       .format(digital_scheduling_bool, google_group))

    directions = self._get_value_from_extention(hierarchy_org_obj,
                                                _FHIR_SYSTEM_PREFIX + 'directions')
    physical_location_name = self._get_value_from_extention(hierarchy_org_obj,
                                                            _FHIR_SYSTEM_PREFIX + 'locationName')
    address_1, address_2, city, state, zip_code = self._get_address(hierarchy_org_obj)

    phone = self._get_contact_point(hierarchy_org_obj, 'phone')
    admin_email_addresses = self._get_contact_point(hierarchy_org_obj, 'email')
    link = self._get_contact_point(hierarchy_org_obj, 'url')

    schedule_instructions = self._get_value_from_extention(hierarchy_org_obj,
                                                           _FHIR_SYSTEM_PREFIX + 'schedulingInstructions')

    entity = Site(siteName=name,
                  googleGroup=google_group,
                  mayolinkClientNumber=mayolink_client_number,
                  organizationId=organization.organizationId,
                  hpoId=organization.hpoId,
                  siteStatus=site_status,
                  enrollingStatus=enrolling_status,
                  digitalSchedulingStatus=digital_scheduling_status,
                  scheduleInstructions=schedule_instructions,
                  scheduleInstructions_ES='',
                  launchDate=launch_date,
                  notes=notes,
                  notes_ES='',
                  directions=directions,
                  physicalLocationName=physical_location_name,
                  address1=address_1,
                  address2=address_2,
                  city=city,
                  state=state,
                  zipCode=zip_code,
                  phoneNumber=phone,
                  adminEmails=admin_email_addresses,
                  link=link,
                  isObsolete=is_obsolete)

    existing_map = {entity.googleGroup: entity for entity in self.site_dao.get_all()}

    existing_entity = existing_map.get(entity.googleGroup)
    with self.site_dao.session() as session:
      if existing_entity:
        self._populate_lat_lng_and_time_zone(entity, existing_entity)
        if entity.siteStatus == SiteStatus.ACTIVE and \
          (entity.latitude is None or entity.longitude is None):
          raise BadRequest('Active site without geocoding: {}'.format(entity.googleGroup))

        new_dict = entity.asdict()
        new_dict['siteId'] = None
        existing_dict = existing_entity.asdict()
        existing_dict['siteId'] = None
        if existing_dict == new_dict:
          logging.info('Not updating {}.'.format(new_dict['googleGroup']))
        else:
          for k, v in entity.asdict().iteritems():
            if k != 'siteId' and k != 'googleGroup':
              setattr(existing_entity, k, v)
          self.site_dao.update_with_session(session, existing_entity)
      else:
        self._populate_lat_lng_and_time_zone(entity, None)
        if entity.siteStatus == SiteStatus.ACTIVE and \
          (entity.latitude is None or entity.longitude is None):
          raise BadRequest('Active site without geocoding: {}'.format(entity.googleGroup))
        self.site_dao.insert_with_session(session, entity)

  def _get_type(self, hierarchy_org_obj):
    obj_type = None
    type_arr = hierarchy_org_obj.type
    for type_item in type_arr:
      code_arr = type_item.coding
      for code_item in code_arr:
        if code_item.system == _FHIR_SYSTEM_PREFIX + 'awardee-type':
          obj_type = code_item.code
          break

    return obj_type

  def _get_value_from_identifier(self, hierarchy_org_obj, system):
    identifier_arr = hierarchy_org_obj.identifier
    for identifier in identifier_arr:
      if identifier.system == system:
        return identifier.value
    else:
      return None

  def _get_value_from_extention(self, hierarchy_org_obj, url, value_key='valueString'):
    extension_arr = hierarchy_org_obj.extension
    for extension in extension_arr:
      if extension.url == url:
        ext_json = extension.as_json()
        return ext_json[value_key]
    else:
      return None

  def _get_contact_point(self, hierarchy_org_obj, code):
    contact_arr = hierarchy_org_obj.contact
    for contact in contact_arr:
      telecom_arr = contact.telecom
      for telecom in telecom_arr:
        if telecom.system == code:
          return telecom.value
    else:
      return None

  def _get_address(self, hierarchy_org_obj):
    address = hierarchy_org_obj.address[0]
    address_1 = address.line[0] if len(address.line) > 0 else ''
    address_2 = address.line[1] if len(address.line) > 1 else ''
    city = address.city
    state = address.state
    postal_code = address.postalCode

    return address_1, address_2, city, state, postal_code

  def _populate_lat_lng_and_time_zone(self, site, existing_site):
    if site.address1 and site.city and site.state:
      if existing_site:
        if (existing_site.address1 == site.address1 and existing_site.city == site.city and
            existing_site.state == site.state and existing_site.latitude is not None and
            existing_site.longitude is not None and existing_site.timeZoneId is not None):
            # Address didn't change, use the existing lat/lng and time zone.
          site.latitude = existing_site.latitude
          site.longitude = existing_site.longitude
          site.timeZoneId = existing_site.timeZoneId
          return
      latitude, longitude = self._get_lat_long_for_site(site.address1, site.city, site.state)
      site.latitude = latitude
      site.longitude = longitude
      if latitude and longitude:
        site.timeZoneId = self._get_time_zone(latitude, longitude)
    else:
      if site.googleGroup not in self.status_exception_list:
        if site.siteStatus == self.ACTIVE:
          logging.warn('Active site must have valid address. Site: {}, Group: {}'.format(
            site.siteName, site.googleGroup))

  def _get_lat_long_for_site(self, address_1, city, state):
    self.full_address = address_1 + ' ' + city + ' ' + state
    try:
      self.api_key = os.environ.get('API_KEY')
      self.gmaps = googlemaps.Client(key=self.api_key)
      try:
        geocode_result = self.gmaps.geocode(address_1 + '' + city + ' ' + state)[0]
      except IndexError:
        logging.warn('Bad address for {}, could not geocode.'.format(self.full_address))
        return None, None
      if geocode_result:
        geometry = geocode_result.get('geometry')
        if geometry:
          location = geometry.get('location')
        if location:
          latitude = location.get('lat')
          longitude = location.get('lng')
          return latitude, longitude
        else:
          logging.warn('Can not find lat/long for %s', self.full_address)
          return None, None
      else:
        logging.warn('Geocode results failed for %s.', self.full_address)
        return None, None
    except ValueError as e:
      logging.exception('Invalid geocode key: %s. ERROR: %s', self.api_key, e)
      return None, None
    except IndexError as e:
      logging.exception('Geocoding failure Check that address is correct. ERROR: %s', e)
      return None, None

  def _get_time_zone(self, latitude, longitude):
    time_zone = self.gmaps.timezone(location=(latitude, longitude))
    if time_zone['status'] == 'OK':
      time_zone_id = time_zone['timeZoneId']
      return time_zone_id
    else:
      logging.info('can not retrieve time zone from %s', self.full_address)
      return None
