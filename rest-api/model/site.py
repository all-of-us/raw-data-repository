from model.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey

class Site(Base):
  __tablename__ = 'site'
  siteId = Column('site_id', Integer, primary_key=True)
  siteName = Column('site_name', String(255), nullable=False)
  # The e-mail address of the Google group for the site; this is a unique key used externally.
  googleGroup = Column('google_group', String(255), nullable=False, unique=True)
  consortiumName = Column('consortium_name', String(255), nullable=False)
  mayolinkClientNumber = Column('mayolink_client_number', Integer)
  hpoId = Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)
