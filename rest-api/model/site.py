from model.base import Base
from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from site_enums import SiteStatus

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
  launchDate = Column('launch_date', Date)
  notes = Column('notes', String(1024))
  latitude = Column('latitude', Float)
  longitude = Column('latitude', Float)
  directions = Column('directions', String(1024))
  physicalLocationName = Column('physical_location_name', String(1024))
  address1
