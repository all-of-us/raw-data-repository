from sqlalchemy import Column, event, ForeignKey, Integer, String

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.utils import UTCDateTime


class HProConsentFile(Base):
    __tablename__ = 'hpro_consent_files'

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    consent_file_id = Column(Integer, ForeignKey(ConsentFile.id))
    file_upload_time = Column(UTCDateTime, nullable=True)
    file_path = Column(String(250), nullable=True)


event.listen(HProConsentFile, "before_insert", model_insert_listener)
event.listen(HProConsentFile, "before_update", model_update_listener)
