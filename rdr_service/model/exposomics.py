from sqlalchemy import (
    Column, Integer,
    String, event
)

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime
from sqlalchemy.dialects.mysql import JSON


class ExposomicsBase:

    id = Column('id', Integer,
                primary_key=True,
                autoincrement=True,
                nullable=False)
    created = Column("created", UTCDateTime, nullable=True)
    modified = Column("modified", UTCDateTime, nullable=True)


class ExposomicsManifestBase(ExposomicsBase):

    file_path = Column(String(255), nullable=False, index=True)
    file_data = Column(JSON, nullable=False)
    file_name = Column(String(128), nullable=False)
    bucket_name = Column(String(128), nullable=False, index=True)


class ExposomicsM0(Base, ExposomicsManifestBase):

    __tablename__ = 'exposomics_m0'


event.listen(ExposomicsM0, 'before_insert', model_insert_listener)
event.listen(ExposomicsM0, 'before_update', model_update_listener)
