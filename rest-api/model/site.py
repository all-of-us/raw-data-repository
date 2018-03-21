from model.base import Base
from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from site_enums import SiteStatus, EnrollingStatus
from model.utils import Enum

class Site(Base):
  __tablename__ = 'site'
  siteId = Column('site_id', Integer, primary_key=True)
  siteName = Column('site_name', String(255), nullable=False)
  # The Google group for the site; this is a unique key used externally.
  googleGroup = Column('google_group', String(255), nullable=False, unique=True)
  mayolinkClientNumber = Column('mayolink_client_number', Integer)
  organizationId = Column('organization_id', Integer,
                          ForeignKey('organization.organization_id'))
  # Deprecated; this is being replaced by organizationId.
  hpoId = Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'))

  siteStatus = Column('site_status', Enum(SiteStatus))
  enrollingStatus = Column('enrolling_status', Enum(EnrollingStatus))
  launchDate = Column('launch_date', Date)
  notes = Column('notes', String(1024))
  latitude = Column('latitude', Float)
  longitude = Column('longitude', Float)
  timeZoneId = Column('time_zone_id', String(1024))
  directions = Column('directions', String(1024))
  physicalLocationName = Column('physical_location_name', String(1024))
  address1 = Column('address_1', String(1024))
  address2 = Column('address_2', String(1024))
  city = Column('city', String(255))
  state = Column('state', String(2))
  zipCode = Column('zip_code', String(10))
  phoneNumber = Column('phone_number', String(80))
  adminEmails = Column('admin_emails', String(4096))
  link = Column('link', String(255))
