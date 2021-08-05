"""Utilities used by the API definition, and authentication/authorization/roles."""
import datetime

from dateutil.parser import parse
from werkzeug.exceptions import BadRequest
from rdr_service.storage import get_storage_provider
from rdr_service.code_constants import UNMAPPED, UNSET

# Role constants
PTC = "ptc"
HEALTHPRO = "healthpro"
RDR = "rdr"
CURATION = "curation"
AWARDEE = "awardee_sa"
STOREFRONT = "storefront"
EXPORTER = "exporter"
WORKBENCH = "workbench"
REDCAP = 'redcap'
RESOURCE = 'resource'
DEV_MAIL = "example@example.com"
GEM = "gem"
BIOBANK = 'biobank'
RDR_AND_PTC = [RDR, PTC]
PTC_AND_GEM = [PTC, GEM]
WORKBENCH_AND_REDCAP = [WORKBENCH, REDCAP]
STOREFRONT_AND_REDCAP = [STOREFRONT, REDCAP]
PTC_AND_HEALTHPRO = [PTC, HEALTHPRO]
PTC_HEALTHPRO_AWARDEE = [PTC, HEALTHPRO, AWARDEE]
PTC_HEALTHPRO_AWARDEE_CURATION = [PTC, HEALTHPRO, AWARDEE, CURATION]
ALL_ROLES = [PTC, HEALTHPRO, STOREFRONT, EXPORTER, WORKBENCH, GEM, REDCAP, BIOBANK, RDR]
DV_FHIR_URL = "http://joinallofus.org/fhir/"
DV_BARCODE_URL = DV_FHIR_URL + "barcode"
DV_ORDER_URL = DV_FHIR_URL + "order-type"
DV_FULFILLMENT_URL = DV_FHIR_URL + "fulfillment-status"
HIERARCHY_CONTENT_SYSTEM_PREFIX = 'http://all-of-us.org/fhir/sites/'


def parse_date(date_str, date_format=None, date_only=False):
    """Parses JSON dates.

  Args:
    date_format: If specified, use this date format, otherwise uses the proto
      converter's date handling logic.
   date_only: If specified, and true, will raise an exception if the parsed
     timestamp isn't midnight.
  """
    if date_format:
        return datetime.datetime.strptime(date_str, date_format)
    else:
        date_obj = parse(date_str)
        if date_obj.utcoffset():
            date_obj = date_obj.replace(tzinfo=None) - date_obj.utcoffset()
        else:
            date_obj = date_obj.replace(tzinfo=None)
        if date_only:
            if date_obj != datetime.datetime.combine(date_obj.date(), datetime.datetime.min.time()):
                raise BadRequest("Date contains non zero time fields")
        return date_obj


def convert_to_datetime(date):
    return datetime.datetime.combine(date, datetime.datetime.min.time())


def format_json_date(obj, field_name, date_format=None):
    """Converts a field of a dictionary from a datetime to a string."""
    if field_name in obj:
        if obj[field_name] is None:
            del obj[field_name]
        else:
            if date_format:
                obj[field_name] = obj[field_name].strftime(date_format)
            else:
                obj[field_name] = obj[field_name].isoformat()


def format_json_code(obj, code_dao, field_name, unset_value=UNSET):
    field_without_id = field_name[0 : len(field_name) - 2]
    value = obj.get(field_name)
    if value:
        code = code_dao.get(value)
        if code.mapped:
            obj[field_without_id] = code.value
        else:
            obj[field_without_id] = UNMAPPED
        del obj[field_name]
    else:
        obj[field_without_id] = unset_value


def format_json_hpo(obj, hpo_dao, field_name):
    if obj[field_name]:
        obj[field_name] = hpo_dao.get(obj[field_name]).name
    else:
        obj[field_name] = UNSET


def format_json_org(obj, organization_dao, field_name):
    if obj[field_name]:
        obj[field_name] = organization_dao.get(obj[field_name]).externalId
    else:
        obj[field_name] = UNSET


def format_json_site(obj, site_dao, field_name):
    site_id = obj.get(field_name + "Id")
    if site_id is not None:
        obj[field_name] = site_dao.get(site_id).googleGroup
        del obj[field_name + "Id"]
    else:
        obj[field_name] = UNSET


def parse_json_enum(obj, field_name, enum_cls):
    """Converts a field of a dictionary from a string to an enum."""
    if field_name in obj and obj[field_name] is not None:
        obj[field_name] = enum_cls(obj[field_name])


def format_json_enum(obj, field_name):
    """Converts a field of a dictionary from a enum to an string."""
    if field_name in obj and obj[field_name] is not None:
        obj[field_name] = str(obj[field_name])
    else:
        obj[field_name] = UNSET


def get_site_id_from_google_group(obj, site_dao):
    if "site" in obj:
        site = site_dao.get_by_google_group(obj["site"])
        if site is not None:
            return site.siteId
    return None


def get_site_id_by_site_value(obj):
    if "site" in obj:
        from rdr_service.dao.site_dao import SiteDao

        site_dao = SiteDao()
        site = site_dao.get_by_google_group(obj["site"]["value"])
        if site is not None:
            return site.siteId
    return None


def get_awardee_id_from_name(obj, hpo_dao):
    if "awardee" in obj:
        awardee = hpo_dao.get_by_name(obj["awardee"])
        if awardee is not None:
            return awardee.hpoId
    return None


def get_organization_id_from_external_id(obj, organization_dao):
    if "organization" in obj:
        organization = organization_dao.get_by_external_id(obj["organization"])
        if organization is not None:
            return organization.organizationId
    return None


def get_code_id(obj, code_dao, field, prefix):
    """ Gets a code id based on the value in ppi questionnaire
  i.e. prefix = State_ and field = state and obj[state] = VA
  will return the code_id for State_VA"""
    system = "http://terminology.pmi-ops.org/CodeSystem/ppi"
    field_with_id = field + "Id"
    if field_with_id is not None:
        code_value = prefix + obj[field]
        code = code_dao.get_code(system, code_value)
        if code:
            return code.codeId
    else:
        return UNSET


def open_cloud_file(cloud_file_path, mode=None):
    provider = get_storage_provider()
    return provider.open(cloud_file_path, mode or 'rt')


def lookup_bucket(bucket_name):
    provider = get_storage_provider()
    return provider.lookup(bucket_name)


def list_blobs(bucket_name, prefix=None):
    provider = get_storage_provider()
    return provider.list(bucket_name, prefix)


def get_blob(bucket_name, blob_name):
    provider = get_storage_provider()
    return provider.get_blob(bucket_name, blob_name)


def upload_from_file(source_file_name, cloud_file_path):
    provider = get_storage_provider()
    return provider.upload_from_file(source_file_name, cloud_file_path)


def upload_from_string(contents, cloud_file_path):
    provider = get_storage_provider()
    return provider.upload_from_string(contents, cloud_file_path)


def delete_cloud_file(path):
    provider = get_storage_provider()
    return provider.delete(path)


def copy_cloud_file(source_path, destination_path):
    provider = get_storage_provider()
    return provider.copy_blob(source_path, destination_path)


def download_cloud_file(source_path, destination_path):
    provider = get_storage_provider()
    return provider.download_blob(source_path, destination_path)
