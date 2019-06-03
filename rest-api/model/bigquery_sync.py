from model.base import Base, model_insert_listener, model_update_listener
from sqlalchemy import Column, DateTime, Integer, String, Index, event, ForeignKey
from sqlalchemy.dialects.mysql import JSON


class BigQuerySync(Base):
  """
  BigQuery synchronization table
  """
  __tablename__ = 'bigquery_sync'

  # Primary Key
  id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
  # have mysql set the creation data for each new order
  created = Column('created', DateTime, nullable=True, index=True)
  # have mysql always update the modified data when the record is changed
  modified = Column('modified', DateTime, nullable=True, index=True)
  # BigQuery dataset name
  dataSet = Column('dataset', String(80), nullable=False)
  # BigQuery table name
  table = Column('table', String(80), nullable=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)
  # data resource hold the data to be transferred to BigQuery
  resource = Column('resource', JSON, nullable=False)

Index('ix_participant_ds_table', BigQuerySync.participantId, BigQuerySync.dataSet, BigQuerySync.table)

event.listen(BigQuerySync, 'before_insert', model_insert_listener)
event.listen(BigQuerySync, 'before_update', model_update_listener)
