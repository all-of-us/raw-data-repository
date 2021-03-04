from sqlalchemy import Column, DateTime, Integer, String, event, Boolean
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener


class RequestsLog(Base):
    """
  Capture and log all raw requests
  """

    __tablename__ = "requests_log"
    __rdr_internal_table__ = True

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True, index=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)
    # endpoint
    endpoint = Column("endpoint", String(80), nullable=False)
    # Api version information
    version = Column("version", Integer, nullable=False)
    # API request method, IE: GET
    method = Column("method", String(10), nullable=False)
    # request url, including GET arguments
    url = Column("url", String(2049), nullable=False)
    # request body data
    resource = Column("resource", JSON, nullable=True)
    # Participant ID
    participantId = Column("participant_id", Integer, nullable=True, index=True)
    # Foreign primary key value. Primary key value from table where this request was used.
    fpk_id = Column("fpk_id", Integer, nullable=True)
    fpk_alt_id = Column("fpk_alt_id", String(80), nullable=True)
    # Foreign table name. Table name where this request was used.
    fpk_table = Column("fpk_table", String(65), nullable=True)
    # Foreign column name. Primary key column name where this request was used.
    fpk_column = Column("fpk_column", String(65), nullable=True)
    # Authenticated User ID
    user = Column('user', String(255), nullable=True)
    # request completed successfully
    complete = Column('complete', Boolean, nullable=True, default=0)



event.listen(RequestsLog, "before_insert", model_insert_listener)
event.listen(RequestsLog, "before_update", model_update_listener)
