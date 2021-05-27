import logging
import time

import googlemaps
import rdr_service.lib_fhir.fhirclient_3_0_0.models.organization

from rdr_service.lib_fhir.fhirclient_3_0_0.models.fhirabstractbase import FHIRValidationError
from werkzeug.exceptions import BadRequest
from flask import request
from rdr_service import config
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.model.site import Site
from rdr_service.model.site_enums import ObsoleteStatus, SiteStatus, EnrollingStatus, DigitalSchedulingStatus
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.participant_enums import OrganizationType
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.bq_hpo_dao import bq_hpo_update_by_id
from rdr_service.dao.bq_organization_dao import bq_organization_update_by_id
from rdr_service.dao.bq_site_dao import bq_site_update_by_id
from rdr_service.dao.code_dao import CodeDao
from rdr_service.code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from dateutil.parser import parse
from rdr_service.api_util import HIERARCHY_CONTENT_SYSTEM_PREFIX
from rdr_service.tools.import_participants import _setup_questionnaires, import_participant
from rdr_service.data_gen.in_process_client import InProcessClient


class OrganizationHierarchySyncDao(BaseDao):
    def __init__(self):
        super(OrganizationHierarchySyncDao, self).__init__(HPO)
        self.hpo_dao = HPODao()
        self.organization_dao = OrganizationDao()
        self.site_dao = SiteDao()
        self.code_dao = CodeDao()

    def from_client_json(self, resource_json, id_=None, expected_version=None, client_id=None):  # pylint: disable=unused-argument
        try:
            fhir_org = rdr_service.lib_fhir.fhirclient_3_0_0.models.organization.Organization(resource_json)
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
        if hierarchy_org_obj.id is None:
            raise BadRequest('No id found in payload data.')
        awardee_id = self._get_value_from_identifier(hierarchy_org_obj,
                                                     HIERARCHY_CONTENT_SYSTEM_PREFIX + 'awardee-id')
        if awardee_id is None:
            raise BadRequest('No organization-identifier info found in payload data.')
        is_obsolete = ObsoleteStatus('OBSOLETE') if not hierarchy_org_obj.active else None
        awardee_type = self._get_value_from_extention(hierarchy_org_obj, HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                      'awardee-type')

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
                     isObsolete=is_obsolete,
                     resourceId=hierarchy_org_obj.id)

        existing_map = {entity.name: entity for entity in self.hpo_dao.get_all(refresh_cache=True)}
        existing_entity = existing_map.get(entity.name)

        with self.hpo_dao.session() as session:
            if existing_entity:
                hpo_id = existing_entity.hpoId
                new_dict = entity.asdict()
                new_dict['hpoId'] = None
                existing_dict = existing_entity.asdict()
                existing_dict['hpoId'] = None
                if existing_dict == new_dict:
                    logging.warning(f'No change found for {new_dict["name"]}, skip updating.')
                else:
                    existing_entity.displayName = entity.displayName
                    existing_entity.organizationType = entity.organizationType
                    existing_entity.isObsolete = entity.isObsolete
                    existing_entity.resourceId = entity.resourceId
                    self.hpo_dao.update_with_session(session, existing_entity)
            else:
                hpo_id_list = [item.hpoId for item in self.hpo_dao.get_all(refresh_cache=True)]
                entity.hpoId = max(hpo_id_list) + 1 if len(hpo_id_list) > 0 else 0
                hpo_id = entity.hpoId
                self.hpo_dao.insert_with_session(session, entity)
        bq_hpo_update_by_id(hpo_id)

    def _update_organization(self, hierarchy_org_obj):
        if hierarchy_org_obj.id is None:
            raise BadRequest('No id found in payload data.')
        organization_id = self._get_value_from_identifier(hierarchy_org_obj,
                                                          HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                          'organization-id')
        if organization_id is None:
            raise BadRequest('No organization-identifier info found in payload data.')
        is_obsolete = ObsoleteStatus('OBSOLETE') if not hierarchy_org_obj.active else None
        resource_id = self._get_reference(hierarchy_org_obj)

        hpo = self.hpo_dao.get_by_resource_id(resource_id)
        if hpo is None:
            raise BadRequest('Invalid partOf reference {} importing organization {}'
                             .format(resource_id, organization_id))

        entity = Organization(externalId=organization_id.upper(),
                              displayName=hierarchy_org_obj.name,
                              hpoId=hpo.hpoId,
                              isObsolete=is_obsolete,
                              resourceId=hierarchy_org_obj.id)
        existing_map = {entity.externalId: entity for entity in self.organization_dao.get_all(refresh_cache=True)}
        existing_entity = existing_map.get(entity.externalId)
        with self.organization_dao.session() as session:
            if existing_entity:
                new_dict = entity.asdict()
                new_dict['organizationId'] = None
                existing_dict = existing_entity.asdict()
                existing_dict['organizationId'] = None
                if existing_dict == new_dict:
                    logging.warning(f'No change found for {new_dict["externalId"]}, skip updating.')
                else:
                    existing_entity.displayName = entity.displayName
                    existing_entity.hpoId = entity.hpoId
                    existing_entity.isObsolete = entity.isObsolete
                    existing_entity.resourceId = entity.resourceId
                    self.organization_dao.update_with_session(session, existing_entity)
            else:
                self.organization_dao.insert_with_session(session, entity)
        org_id = self.organization_dao.get_by_external_id(organization_id.upper()).organizationId
        bq_organization_update_by_id(org_id)

    def _update_site(self, hierarchy_org_obj):
        if hierarchy_org_obj.id is None:
            raise BadRequest('No id found in payload data.')
        google_group = self._get_value_from_identifier(hierarchy_org_obj,
                                                       HIERARCHY_CONTENT_SYSTEM_PREFIX + 'site-id')
        if google_group is None:
            raise BadRequest('No organization-identifier info found in payload data.')
        google_group = google_group.lower()
        is_obsolete = ObsoleteStatus('OBSOLETE') if not hierarchy_org_obj.active else None
        resource_id = self._get_reference(hierarchy_org_obj)

        organization = self.organization_dao.get_by_resource_id(resource_id)
        if organization is None:
            raise BadRequest('Invalid partOf reference {} importing site {}'
                             .format(resource_id, google_group))

        site_type = self._get_value_from_extention(hierarchy_org_obj, HIERARCHY_CONTENT_SYSTEM_PREFIX + 'site-type',
                                                   'valueString')
        launch_date = None
        launch_date_str = self._get_value_from_extention(hierarchy_org_obj,
                                                         HIERARCHY_CONTENT_SYSTEM_PREFIX + 'anticipated-launch-date',
                                                         'valueString')
        if launch_date_str:
            try:
                launch_date = parse(launch_date_str).date()
            except ValueError:
                try:
                    # Sometime they send a human readable date, sometimes they send epoch time.
                    launch_date = time.strftime('%Y-%m-%d', time.localtime(int(launch_date_str)))
                except ValueError:
                    raise BadRequest('Invalid launch date {} for site {}'.format(launch_date_str, google_group))

        name = hierarchy_org_obj.name
        mayolink_client_number = None
        mayolink_client_number_str = self._get_value_from_extention(hierarchy_org_obj,
                                                                    HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                                    'mayolink-client-#')
        if mayolink_client_number_str:
            try:
                mayolink_client_number = int(mayolink_client_number_str)
            except ValueError:
                raise BadRequest('Invalid Mayolink Client # {} for site {}'.format(
                    mayolink_client_number_str, google_group))

        notes = self._get_value_from_extention(hierarchy_org_obj, HIERARCHY_CONTENT_SYSTEM_PREFIX + 'notes')

        site_status_value = self._get_value_from_extention(hierarchy_org_obj,
                                                           HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                           'ptsc-scheduling-status',
                                                           'valueString')
        # Since the data type was changed without us knowing, we can use this work around to get boolean values.
        site_status_bool = True if site_status_value == "true" else False
        try:
            site_status = SiteStatus('ACTIVE' if site_status_bool else 'INACTIVE')
        except TypeError:
            raise BadRequest('Invalid site status {} for site {}'.format(site_status, google_group))

        enrolling_status_value = self._get_value_from_extention(hierarchy_org_obj,
                                                                HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                                'enrolling-status',
                                                                'valueString')
        enrolling_status_bool = True if enrolling_status_value == "true" else False

        try:
            enrolling_status = EnrollingStatus('ACTIVE' if enrolling_status_bool else 'INACTIVE')
        except TypeError:
            raise BadRequest('Invalid enrollment site status {} for site {}'
                             .format(enrolling_status_bool, google_group))

        digital_scheduling = self._get_value_from_extention(hierarchy_org_obj,
                                                            HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                            'digital-scheduling-status',
                                                            'valueString')
        digital_scheduling_bool = True if digital_scheduling == 'true' else False
        try:
            digital_scheduling_status = DigitalSchedulingStatus('ACTIVE' if digital_scheduling_bool
                                                                else 'INACTIVE')
        except TypeError:
            raise BadRequest('Invalid digital scheduling status {} for site {}'
                             .format(digital_scheduling_bool, google_group))

        directions = self._get_value_from_extention(hierarchy_org_obj,
                                                    HIERARCHY_CONTENT_SYSTEM_PREFIX + 'directions')
        physical_location_name = self._get_value_from_extention(hierarchy_org_obj,
                                                                HIERARCHY_CONTENT_SYSTEM_PREFIX + 'location-name')
        address_1, address_2, city, state, zip_code = self._get_address(hierarchy_org_obj)

        phone = self._get_contact_point(hierarchy_org_obj, 'phone')
        admin_email_addresses = self._get_contact_point(hierarchy_org_obj, 'email')
        link = self._get_contact_point(hierarchy_org_obj, 'url')

        schedule_instructions = self._get_value_from_extention(hierarchy_org_obj,
                                                               HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                               'scheduling-instructions')
        notes_spanish = self._get_value_from_extention(hierarchy_org_obj,
                                                       HIERARCHY_CONTENT_SYSTEM_PREFIX +
                                                       'notes-spanish')

        entity = Site(siteName=name,
                      googleGroup=google_group,
                      mayolinkClientNumber=mayolink_client_number,
                      organizationId=organization.organizationId,
                      hpoId=organization.hpoId,
                      siteType=site_type,
                      siteStatus=site_status,
                      enrollingStatus=enrolling_status,
                      digitalSchedulingStatus=digital_scheduling_status,
                      scheduleInstructions=schedule_instructions,
                      scheduleInstructions_ES='',
                      launchDate=launch_date,
                      notes=notes,
                      notes_ES=notes_spanish,
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
                      isObsolete=is_obsolete,
                      resourceId=hierarchy_org_obj.id)

        existing_map = {entity.googleGroup: entity for entity in self.site_dao.get_all(refresh_cache=True)}
        existing_entity = existing_map.get(entity.googleGroup)
        new_site = None
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
                    logging.warning(f'No change found for {new_dict["googleGroup"]}, skip updating.')
                else:
                    for k, v in entity.asdict().items():
                        if k != 'siteId' and k != 'googleGroup':
                            setattr(existing_entity, k, v)
                    self.site_dao.update_with_session(session, existing_entity)
            else:
                self._populate_lat_lng_and_time_zone(entity, None)
                if entity.siteStatus == SiteStatus.ACTIVE and \
                    (entity.latitude is None or entity.longitude is None):
                    raise BadRequest('Active site without geocoding: {}'.format(entity.googleGroup))
                new_site = self.site_dao.insert_with_session(session, entity)

        if new_site is not None:
            # Generates 20 fake participants for the site if not on Prod
            # Not called during unittests since codebook breaks
            logging.info(f'New site: {new_site.googleGroup}')
            if self.code_dao.get_code(PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE):
                logging.info('Generating fake participants.')
                self._generate_fake_participants_for_site(new_site)

        site_id = self.site_dao.get_by_google_group(google_group).siteId
        bq_site_update_by_id(site_id)

    def _generate_fake_participants_for_site(self, new_site):
        if config.GAE_PROJECT in ['localhost',
                                  "all-of-us-rdr-stable",
                                  "all-of-us-rdr-sandbox"]:
            n = 20  # number of participants
            logging.info(f'Generating {n} fake participants for {new_site.googleGroup}.')
            auth_header = {'Authorization': request.headers.get('Authorization')}
            client = InProcessClient(headers=auth_header)
            questionnaire_to_questions, consent_questionnaire_id_and_version = _setup_questionnaires(client)
            consent_questions = questionnaire_to_questions[consent_questionnaire_id_and_version]
            participants = {
                "zip_code": "20001",
                "date_of_birth": "1933-3-3",
                "gender_identity": "GenderIdentity_Woman",
                "withdrawalStatus": "NOT_WITHDRAWN",
                "suspensionStatus": "NOT_SUSPENDED",
            }
            for p in range(1, n+1):
                participant = participants
                participant.update({"last_name": new_site.googleGroup.split("-")[-1]})
                participant.update({"first_name": "Participant {}".format(p)})
                participant.update({"site": new_site.googleGroup})

                import_participant(
                    participant,
                    client,
                    consent_questionnaire_id_and_version,
                    questionnaire_to_questions,
                    consent_questions,
                )

            logging.info(f"{n} participants imported.")

    def _get_type(self, hierarchy_org_obj):
        obj_type = None
        type_arr = hierarchy_org_obj.type
        for type_item in type_arr:
            code_arr = type_item.coding
            for code_item in code_arr:
                if code_item.system == HIERARCHY_CONTENT_SYSTEM_PREFIX + 'type':
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
        if not hierarchy_org_obj.contact:
            return None
        contact_arr = hierarchy_org_obj.contact
        for contact in contact_arr:
            telecom_arr = contact.telecom
            for telecom in telecom_arr:
                if telecom.system == code:
                    return telecom.value


    def _get_address(self, hierarchy_org_obj):
        if hierarchy_org_obj.address:
            try:
                address = hierarchy_org_obj.address[0]
            except IndexError:
                return None, None, None, None, None
        else:
            return None, None, None, None, None
        address_1 = address.line[0] if len(address.line) > 0 else ''
        address_2 = address.line[1] if len(address.line) > 1 else ''
        city = address.city
        state = address.state
        postal_code = address.postalCode

        return address_1, address_2, city, state, postal_code

    def _get_reference(self, hierarchy_org_obj):
        try:
            return hierarchy_org_obj.partOf.reference.split('/')[1]
        except IndexError:
            return None

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
            if site.siteStatus == SiteStatus.ACTIVE:
                logging.warning(f'Active site must have valid address. Site:{site.siteName}, Group:{site.googleGroup}')

    def _get_lat_long_for_site(self, address_1, city, state):
        self.full_address = address_1 + ' ' + city + ' ' + state
        try:
            self.api_key = config.getSetting('geocode_api_key', None)
            if not self.api_key:
                logging.error('Geocode key not set')
                return None, None

            self.gmaps = googlemaps.Client(key=self.api_key)
            try:
                geocode_result = self.gmaps.geocode(address_1 + '' + city + ' ' + state)[0]
            except IndexError:
                logging.warning(f'Bad address for {self.full_address}, could not geocode.')
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
                    logging.warning(f'Can not find lat/long for {self.full_address}')
                    return None, None
            else:
                logging.warning(f'Geocode results failed for {self.full_address}.')
                return None, None
        except ValueError as e:
            logging.exception(f'Invalid geocode key: {self.api_key}. ERROR: {e}')
            return None, None
        except IndexError as e:
            logging.exception(f'Geocoding failure Check that address is correct. ERROR: {e}')
            return None, None

    def _get_time_zone(self, latitude, longitude):
        time_zone = self.gmaps.timezone(location=(latitude, longitude))
        if time_zone['status'] == 'OK':
            time_zone_id = time_zone['timeZoneId']
            return time_zone_id
        else:
            logging.info(f'can not retrieve time zone from {self.full_address}')
            return None
