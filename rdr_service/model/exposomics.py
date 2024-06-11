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


class ExposomicsSamples(Base, ExposomicsBase):

    __tablename__ = 'exposomics_samples'

    biobank_id = Column(String(255), nullable=False, index=True)
    sample_id = Column(String(255), nullable=False, index=True)
    collection_tube_id = Column(String(255), nullable=False, index=True)
    exposomics_set = Column(Integer, nullable=False, default=0)


event.listen(ExposomicsSamples, 'before_insert', model_insert_listener)
event.listen(ExposomicsSamples, 'before_update', model_update_listener)


class ExposomicsM0(Base, ExposomicsManifestBase):

    __tablename__ = 'exposomics_m0'

    exposomics_set = Column(Integer, nullable=False, default=0)


event.listen(ExposomicsM0, 'before_insert', model_insert_listener)
event.listen(ExposomicsM0, 'before_update', model_update_listener)


class ExposomicsM1(Base, ExposomicsManifestBase):

    __tablename__ = 'exposomics_m1'

    copied_path = Column(String(255), nullable=False, index=True)


event.listen(ExposomicsM1, 'before_insert', model_insert_listener)
event.listen(ExposomicsM1, 'before_update', model_update_listener)
