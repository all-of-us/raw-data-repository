#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from enum import Enum

from marshmallow import validate

from rdr_service.resource import Schema, fields
from rdr_service.resource.constants import SchemaID

# TODO: Use the Enums in participant_enums.py
class SiteStatusEnum(Enum):
    """ The active scheduling status of a site. """
    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class EnrollingStatusEnum(Enum):
    """ The actively enrolling status of a site. """
    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class DigitalSchedulingStatusEnum(Enum):
    """ The status of a sites digital scheduling capability. """
    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class ObsoleteStatusEnum(Enum):
    """ If an organization is obsolete but referenced in other tables. """
    ACTIVE = 0
    OBSOLETE = 1

class InPersonOperationsStatus(Enum):
    """ The in-person operations status of a site """
    # Based on a drop-down list of values from PMT
    UNSET = 0
    ONBOARDING = 1
    APPROVED_TO_OPEN = 2
    OPEN_ENGAGEMENT_RECRUITMENT_ENROLLMENT = 3
    OPEN_ENGAGEMENT_ONLY = 4
    PAUSED = 5
    CLOSED_TEMPORARILY = 6
    CLOSED_PERMANENTLY = 7
    ERROR_NEVER_ACTIVATED = 8
    NOT_APPLICABLE_VIRTUAL_SITE = 9

class SiteSchema(Schema):
    hpo_id = fields.Int32()
    organization_id = fields.Int32()

    site_id = fields.Int32(required=False)
    site_name = fields.String(validate=validate.Length(max=255))
    site_type = fields.String(validate=validate.Length(max=255))
    # The Google group for the site; this is a unique key used externally.
    google_group = fields.String(validate=validate.Length(max=255))
    mayolink_client_number = fields.Int32(required=False)

    site_status = fields.EnumString(enum=SiteStatusEnum)
    site_status_id = fields.EnumInteger(enum=SiteStatusEnum)
    enrolling_status = fields.EnumString(enum=EnrollingStatusEnum)
    enrolling_status_id = fields.EnumInteger(enum=EnrollingStatusEnum)
    digital_scheduling_status = fields.EnumString(enum=DigitalSchedulingStatusEnum)
    digital_scheduling_status_id = fields.EnumInteger(enum=DigitalSchedulingStatusEnum)

    schedule_instructions = fields.String(validate=validate.Length(max=4096))
    schedule_instructions_es = fields.String(validate=validate.Length(max=4096))
    launch_date = fields.Date()
    notes = fields.Text()
    notes_es = fields.Text()
    latitude = fields.Float()
    longitude = fields.Float()
    time_zone_id = fields.String(validate=validate.Length(max=1024))
    directions = fields.Text()
    physical_location_name = fields.String(validate=validate.Length(max=1024))
    address_1 = fields.String(validate=validate.Length(max=1024))
    address_2 = fields.String(validate=validate.Length(max=1024))
    city = fields.String(validate=validate.Length(max=255))
    state = fields.String(validate=validate.Length(max=2))
    zip_code = fields.String(validate=validate.Length(max=10))
    phone_number = fields.String(validate=validate.Length(max=80))
    admin_emails = fields.String(validate=validate.Length(max=4096))
    link = fields.String(validate=validate.Length(max=255))
    is_obsolete = fields.EnumString(enum=ObsoleteStatusEnum)
    is_obsolete_id = fields.EnumInteger(enum=ObsoleteStatusEnum)
    in_person_operations_status = fields.EnumString(enum=InPersonOperationsStatus)
    in_person_operations_status_id = fields.EnumInteger(enum=InPersonOperationsStatus)

    class Meta:
        schema_id = SchemaID.site
        resource_uri = 'Site'
        resource_pk_field = 'site_id'
        pii_fields = ()  # List fields that contain PII data.
        pii_filter = {}  # dict(field: lambda function).
