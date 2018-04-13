"""Imports awardees / HPOs, organizations, and sites into the database.

Usage:
  tools/import_organizations.sh [--account <USER>@pmi-ops.org --project <PROJECT>] \
      --awardee_file <AWARDEE CSV> --organization_file <ORGANIZATION CSV> \
      --site_file <SITE CSV> [--dry_run]

  The CSV files originate from the Google Sheets found here:

  https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/edit

  Imports are idempotent; if you run the import multiple times, subsequent imports should
  have no effect.
"""
import os
import logging
import googlemaps
from dateutil.parser import parse
from tools.csv_importer import CsvImporter
from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.site_dao import SiteDao
from model.hpo import HPO
from model.organization import Organization
from model.site import Site
from model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus
from participant_enums import OrganizationType
from main_util import get_parser, configure_logging
# Environments
ENV_LOCAL = 'localhost'
ENV_TEST = 'pmi-drc-api-test'
ENV_STAGING = 'all-of-us-rdr-staging'
ENV_STABLE = 'all-of-us-rdr-stable'
ENV_PROD = 'all-of-us-rdr-prod'
ENV_LIST = [ENV_TEST, ENV_STABLE, ENV_STAGING, ENV_PROD]
# Column headers from the Awardees sheet at:
# https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/edit#gid=1076878570
HPO_AWARDEE_ID_COLUMN = 'Awardee ID'
HPO_NAME_COLUMN = 'Name'
HPO_TYPE_COLUMN = 'Type'

# Column headers for the Organizations sheet at:
# https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/edit#gid=1098779958
ORGANIZATION_AWARDEE_ID_COLUMN = 'Awardee ID'
ORGANIZATION_ORGANIZATION_ID_COLUMN = 'Organization ID'
ORGANIZATION_NAME_COLUMN = 'Name'

# Column headers for the Sites sheet at:
# https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/edit#gid=0
SITE_ORGANIZATION_ID_COLUMN = 'Organization ID'
SITE_SITE_ID_COLUMN = 'Site ID / Google Group'
SITE_SITE_COLUMN = 'Site'
SITE_MAYOLINK_CLIENT_NUMBER_COLUMN = 'MayoLINK Client #'
SITE_NOTES_COLUMN = 'Notes'
# TODO: switch this back to 'Status' [DA-538]
SCHEDULING_INSTRUCTIONS = 'Scheduling Instructions'
SITE_LAUNCH_DATE_COLUMN = 'Anticipated Launch Date'
SITE_DIRECTIONS_COLUMN = 'Directions'
SITE_PHYSICAL_LOCATION_NAME_COLUMN = 'Physical Location Name'
SITE_ADDRESS_1_COLUMN = 'Address 1'
SITE_ADDRESS_2_COLUMN = 'Address 2'
SITE_CITY_COLUMN = 'City'
SITE_STATE_COLUMN = 'State'
SITE_ZIP_COLUMN = 'Zip'
SITE_PHONE_COLUMN = 'Phone'
SITE_ADMIN_EMAIL_ADDRESSES_COLUMN = 'Admin Email Addresses'
SITE_LINK_COLUMN = 'Link'
# values of these columns generated based on environment
SITE_STATUS_COLUMN = 'PTSC Scheduling Status'
ENROLLING_STATUS_COLUMN = 'Enrolling Status'
DIGITAL_SCHEDULING_STATUS_COLUMN = 'Digital Scheduling Status'

class HPOImporter(CsvImporter):

  def __init__(self):
    super(HPOImporter, self).__init__('awardee', HPODao(), 'hpoId', 'name',
                                      [HPO_AWARDEE_ID_COLUMN, HPO_NAME_COLUMN,
                                       HPO_TYPE_COLUMN])
    self.new_count = 0
    self.environment = None

  def _entity_from_row(self, row):
    type_str = row[HPO_TYPE_COLUMN]
    try:
      organization_type = OrganizationType(type_str)
      if organization_type == OrganizationType.UNSET:
        organization_type = None
    except TypeError:
      logging.warn('Invalid organization type %s for awardee %s', type_str,
                   row[HPO_AWARDEE_ID_COLUMN])
      self.errors.append('Invalid organization type {} for awardee {}'.format(type_str,
                         row[HPO_AWARDEE_ID_COLUMN]))
      return None
    return HPO(name=row[HPO_AWARDEE_ID_COLUMN].upper(),
               displayName=row[HPO_NAME_COLUMN],
               organizationType=organization_type)

  def _insert_entity(self, entity, existing_map, session, dry_run):
    # HPO IDs are not autoincremented by the database; manually set it here.
    entity.hpoId = len(existing_map) + self.new_count
    self.new_count += 1
    super(HPOImporter, self)._insert_entity(entity, existing_map, session, dry_run)


class OrganizationImporter(CsvImporter):

  def __init__(self):
    super(OrganizationImporter, self).__init__('organization', OrganizationDao(),
                                               'organizationId', 'externalId',
                                               [ORGANIZATION_AWARDEE_ID_COLUMN,
                                                ORGANIZATION_ORGANIZATION_ID_COLUMN,
                                                ORGANIZATION_NAME_COLUMN])
    self.hpo_dao = HPODao()
    self.environment = None

  def _entity_from_row(self, row):
    hpo = self.hpo_dao.get_by_name(row[ORGANIZATION_AWARDEE_ID_COLUMN].upper())
    if hpo is None:
      logging.warn('Invalid awardee ID %s importing organization %s',
                   row[ORGANIZATION_AWARDEE_ID_COLUMN],
                   row[ORGANIZATION_ORGANIZATION_ID_COLUMN])
      self.errors.append('Invalid awardee ID {} importing organization {}'.format(
                        row[ORGANIZATION_AWARDEE_ID_COLUMN],
                        row[ORGANIZATION_ORGANIZATION_ID_COLUMN]))
      return None
    return Organization(externalId=row[ORGANIZATION_ORGANIZATION_ID_COLUMN].upper(),
                        displayName=row[ORGANIZATION_NAME_COLUMN],
                        hpoId=hpo.hpoId)

class SiteImporter(CsvImporter):

  def __init__(self):
    args = parser.parse_args()
    self.organization_dao = OrganizationDao()
    self.geocode_flag = args.geocode_flag
    self.ACTIVE = SiteStatus.ACTIVE
    self.status_exception_list = ['hpo-site-walgreensphoenix']
    self.project = args.project
    self.instance = args.instance
    self.creds_file = args.creds_file

    if self.project in ENV_LIST:
      self.environment = ' ' + self.project.split('-')[-1].upper()
    elif self.project == ENV_LOCAL:
      self.environment = ' ' + ENV_STABLE.split('-')[-1].upper()

    super(SiteImporter, self).__init__('site', SiteDao(), 'siteId', 'googleGroup',
                                       [SITE_ORGANIZATION_ID_COLUMN, SITE_SITE_ID_COLUMN,
                                        SITE_SITE_COLUMN, SITE_STATUS_COLUMN + self.environment,
                                        ENROLLING_STATUS_COLUMN + self.environment,
                                        DIGITAL_SCHEDULING_STATUS_COLUMN + self.environment])

  def _entity_from_row(self, row):
    google_group = row[SITE_SITE_ID_COLUMN].lower()
    organization = self.organization_dao.get_by_external_id(
                                        row[SITE_ORGANIZATION_ID_COLUMN].upper())
    if organization is None:
      logging.warn('Invalid organization ID %s importing site %s',
                   row[SITE_ORGANIZATION_ID_COLUMN].upper(),
                   google_group)
      self.errors.append('Invalid organization ID {} importing site {}'.format(
                         row[SITE_ORGANIZATION_ID_COLUMN].upper(), google_group))
      return None

    launch_date = None
    launch_date_str = row.get(SITE_LAUNCH_DATE_COLUMN)
    if launch_date_str:
      try:
        launch_date = parse(launch_date_str).date()
      except ValueError:
        logging.warn('Invalid launch date %s for site %s', launch_date_str, google_group)
        self.errors.append('Invalid launch date {} for site {}'.format(
                          launch_date_str, google_group))
        return None
    name = row[SITE_SITE_COLUMN]
    mayolink_client_number = None
    mayolink_client_number_str = row.get(SITE_MAYOLINK_CLIENT_NUMBER_COLUMN)
    if mayolink_client_number_str:
      try:
        mayolink_client_number = int(mayolink_client_number_str)
      except ValueError:
        logging.warn('Invalid Mayolink Client # %s for site %s', mayolink_client_number_str,
                     google_group)
        self.errors.append('Invalid Mayolink Client # {} for site {}'.format(
                          mayolink_client_number_str, google_group))
        return None
    notes = row.get(SITE_NOTES_COLUMN)
    try:
      site_status = SiteStatus(row[SITE_STATUS_COLUMN + self.environment].upper())
    except TypeError:
      logging.warn('Invalid site status %s for site %s', row[SITE_STATUS_COLUMN + self.environment],
                                                                                      google_group)
      self.errors.append('Invalid site status {} for site {}'.format(row[SITE_STATUS_COLUMN +
                                                                  self.environment], google_group))
      return None
    try:
      enrolling_status = EnrollingStatus(row[ENROLLING_STATUS_COLUMN + self.environment].upper())
    except TypeError:
      logging.warn('Invalid enrollment site status %s for site %s', row[ENROLLING_STATUS_COLUMN +
                                                                  self.environment], google_group)
      self.errors.append('Invalid enrollment site status {} for site {}'.format(
                         row[ENROLLING_STATUS_COLUMN + self.environment], google_group))

    directions = row.get(SITE_DIRECTIONS_COLUMN)
    physical_location_name = row.get(SITE_PHYSICAL_LOCATION_NAME_COLUMN)
    address_1 = row.get(SITE_ADDRESS_1_COLUMN)
    address_2 = row.get(SITE_ADDRESS_2_COLUMN)
    city = row.get(SITE_CITY_COLUMN)
    state = row.get(SITE_STATE_COLUMN)
    zip_code = row.get(SITE_ZIP_COLUMN)
    phone = row.get(SITE_PHONE_COLUMN)
    admin_email_addresses = row.get(SITE_ADMIN_EMAIL_ADDRESSES_COLUMN)
    link = row.get(SITE_LINK_COLUMN)
    digital_scheduling_status = DigitalSchedulingStatus(row[DIGITAL_SCHEDULING_STATUS_COLUMN +
                                                            self.environment].upper())
    schedule_instructions = row.get(SCHEDULING_INSTRUCTIONS)
    return Site(siteName=name,
                googleGroup=google_group,
                mayolinkClientNumber=mayolink_client_number,
                organizationId=organization.organizationId,
                hpoId=organization.hpoId,
                siteStatus=site_status,
                enrollingStatus=enrolling_status,
                digitalSchedulingStatus=digital_scheduling_status,
                scheduleInstructions=schedule_instructions,
                launchDate=launch_date,
                notes=notes,
                directions=directions,
                physicalLocationName=physical_location_name,
                address1=address_1,
                address2=address_2,
                city=city,
                state=state,
                zipCode=zip_code,
                phoneNumber=phone,
                adminEmails=admin_email_addresses,
                link=link)

  def _update_entity(self, entity, existing_entity, session, dry_run):
    self._populate_lat_lng_and_time_zone(entity, existing_entity)
    if entity.siteStatus == self.ACTIVE and (entity.latitude == None or entity.longitude == None):
      self.errors.append('Skipped active site without geocoding: {}'.format(entity.googleGroup))
      return None, True
    return super(SiteImporter, self)._update_entity(entity, existing_entity, session, dry_run)

  def _insert_entity(self, entity, existing_map, session, dry_run):
    self._populate_lat_lng_and_time_zone(entity, None)
    if entity.siteStatus == self.ACTIVE and (entity.latitude == None or entity.longitude == None):
      self.errors.append('Skipped active site without geocoding: {}'.format(entity.googleGroup))
      return False
    self.new_site_list.append(entity)
    super(SiteImporter, self)._insert_entity(entity, existing_map, session, dry_run)

  def _populate_lat_lng_and_time_zone(self, site, existing_site):
    if site.address1 and site.city and site.state:
      if existing_site:
        if (existing_site.address1 == site.address1 and existing_site.city == site.city
            and existing_site.state == site.state and existing_site.latitude is not None
            and existing_site.longitude is not None and existing_site.timeZoneId is not None):
            # Address didn't change, use the existing lat/lng and time zone.
          site.latitude = existing_site.latitude
          site.longitude = existing_site.longitude
          site.timeZoneId = existing_site.timeZoneId
          return
      if self.geocode_flag:
        latitude, longitude = self._get_lat_long_for_site(site.address1, site.city, site.state)
        site.latitude = latitude
        site.longitude = longitude
        if latitude and longitude:
          site.timeZoneId = self._get_time_zone(latitude, longitude)
    else:
      if site.googleGroup not in self.status_exception_list:
        if site.siteStatus == self.ACTIVE:
          self.errors.append('Active site must have valid address. Site: {}, Group: {}'.format(
                            site.siteName, site.googleGroup))

  def _get_lat_long_for_site(self, address_1, city, state):
    self.full_address = address_1 + ' ' + city + ' ' + state
    try:
      self.api_key = os.environ.get('API_KEY')
      self.gmaps = googlemaps.Client(key=self.api_key)
      try:
        geocode_result = self.gmaps.geocode(address_1 + '' +  city + ' ' +  state)[0]
      except IndexError:
        self.errors.append('Bad address for {}, could not geocode.'.format(self.full_address))
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
          self.errors.append('Can not find lat/long for {}'.format(self.full_address))
          return None, None
      else:
        logging.warn('Geocode results failed for %s.', self.full_address)
        self.errors.append('Geocode results failed for {}'.format(self.full_address))
        return None, None
    except ValueError as e:
      logging.exception('Invalid geocode key: %s. ERROR: %s', self.api_key, e)
      self.errors.append('Invalid geocode key: {}. ERROR: {}'.format(self.api_key, e))
      return None, None
    except IndexError as e:
      logging.exception('Geocoding failure Check that address is correct. ERROR: %s', e)
      self.errors.append('Geocoding failured Check that address is correct. ERROR: {}'.format(
                        self.api_key, e))
      return None, None

  def _get_time_zone(self, latitude, longitude):
    time_zone = self.gmaps.timezone(location=(latitude, longitude))
    if time_zone['status'] == 'OK':
      time_zone_id = time_zone['timeZoneId']
      return time_zone_id
    else:
      logging.info('can not retrieve time zone from %s', self.full_address)
      self.errors.append('Can not retrieve time zone from {}'.format(self.full_address))
      return None

def main(args):
  HPOImporter().run(args.awardee_file, args.dry_run)
  OrganizationImporter().run(args.organization_file, args.dry_run)
  SiteImporter().run(args.site_file, args.dry_run)


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--awardee_file', help='Filename containing awardee CSV to import',
                      required=True)
  parser.add_argument('--organization_file', help='Filename containing organization CSV to import',
                      required=True)
  parser.add_argument('--site_file', help='Filename containing site CSV to import',
                      required=True)
  parser.add_argument('--dry_run', help='Read CSV and check for diffs against database.',
                      action='store_true')
  parser.add_argument('--geocode_flag', help='If --account passed into import_organizations.sh, '
                      'geocoding is performed.', action='store_true')
  parser.add_argument('--project', help='Project is used to determine enviroment for specific '
                      'settings', required=True)

  parser.add_argument('--instance',
                      type=str,
                      help='The instance to hit, defaults to http://localhost:8080',
                      default='http://localhost:8080')
  parser.add_argument('--creds_file',
                      type=str,
                      help='Path to credentials JSON file.')
  main(parser.parse_args())
