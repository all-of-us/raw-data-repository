from sqlalchemy import Column, DateTime, Integer, String, Index, event
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener


class BigQuerySync(Base):
    """
    BigQuery synchronization table
    """
    __tablename__ = 'bigquery_sync'
    __rdr_internal_table__ = True

    # Primary Key
    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column('created', DateTime, nullable=True, index=True)
    # have mysql always update the modified data when the record is changed
    modified = Column('modified', DateTime, nullable=True, index=True)
    projectId = Column('project_id', String(80), nullable=True)
    # BigQuery dataset name
    datasetId = Column('dataset_id', String(80), nullable=False)
    # BigQuery table name
    tableId = Column('table_id', String(80), nullable=False)
    # primary key from data source table.
    pk_id = Column('pk_id', Integer, nullable=False)
    # data resource hold the data to be transferred to BigQuery
    resource = Column('resource', JSON, nullable=False)


Index('ix_participant_ds_table', BigQuerySync.pk_id, BigQuerySync.projectId, BigQuerySync.datasetId,
      BigQuerySync.tableId)

event.listen(BigQuerySync, 'before_insert', model_insert_listener)
event.listen(BigQuerySync, 'before_update', model_update_listener)
