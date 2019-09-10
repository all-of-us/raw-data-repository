from enum import Enum

from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQField, BQFieldTypeEnum, BQFieldModeEnum


# TODO: Revert to using site_enums.py when they have been updated to Python 3.7 Enum classes.
class BQSiteStatus(Enum):
    """ The active scheduling status of a site. """
    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class BQEnrollingStatus(Enum):
    """ The actively enrolling status of a site. """
    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class BQDigitalSchedulingStatus(Enum):
    """ The status of a sites digital scheduling capability. """
    UNSET = 0
    ACTIVE = 1
    INACTIVE = 2


class BQObsoleteStatusEnum(Enum):
    """ If an organization is obsolete but referenced in other tables. """
    ACTIVE = 0
    OBSOLETE = 1


class BQSiteSchema(BQSchema):
    id = BQField('id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    created = BQField('created', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)
    modified = BQField('modified', BQFieldTypeEnum.DATETIME, BQFieldModeEnum.REQUIRED)

    hpo_id = BQField('hpo_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)
    organization_id = BQField('organization_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    site_id = BQField('site_id', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.REQUIRED)
    site_name = BQField('site_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    # The Google group for the site; this is a unique key used externally.
    google_group = BQField('google_group', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    mayolink_client_number = BQField('mayolink_client_number', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE)

    site_status = BQField('site_status', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                          fld_enum=BQSiteStatus)
    enrolling_status = BQField('enrolling_status', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                               fld_enum=BQEnrollingStatus)
    digital_scheduling_status = BQField('digital_scheduling_status', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                                        fld_enum=BQDigitalSchedulingStatus)

    schedule_instructions = BQField('schedule_instructions', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    schedule_instructions_es = BQField('schedule_instructions_es', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    launch_date = BQField('launch_date', BQFieldTypeEnum.DATE, BQFieldModeEnum.NULLABLE)
    notes = BQField('notes', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    notes_es = BQField('notes_es', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    latitude = BQField('latitude', BQFieldTypeEnum.FLOAT, BQFieldModeEnum.NULLABLE)
    longitude = BQField('longitude', BQFieldTypeEnum.FLOAT, BQFieldModeEnum.NULLABLE)
    time_zone_id = BQField('time_zone_id', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    directions = BQField('directions', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    physical_location_name = BQField('physical_location_name', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    address_1 = BQField('address_1', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    address_2 = BQField('address_2', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    city = BQField('city', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    state = BQField('state', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    zip_code = BQField('zip_code', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    phone_number = BQField('phone_number', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    admin_emails = BQField('admin_emails', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    link = BQField('link', BQFieldTypeEnum.STRING, BQFieldModeEnum.NULLABLE)
    is_obsolete = BQField('is_obsolete', BQFieldTypeEnum.INTEGER, BQFieldModeEnum.NULLABLE,
                          fld_enum=BQObsoleteStatusEnum)


class BQSite(BQTable):
    """ Organization Site BigQuery Table """
    __tablename__ = 'site'
    __schema__ = BQSiteSchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQSiteView(BQView):
    __viewname__ = 'v_site'
    __viewdescr__ = 'Site View'
    __table__ = BQSite
