from sqlalchemy import Column, event, Index, Integer, String

from rdr_service.model.base import Base, model_insert_listener
from rdr_service.model.utils import UTCDateTime


class ApiUser(Base):
    __tablename__ = "api_user"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime)
    system = Column("system", String(80), nullable=False)
    username = Column('username', String(255), nullable=False)


Index('api_username_system', ApiUser.system, ApiUser.username)

event.listen(ApiUser, "before_insert", model_insert_listener)
