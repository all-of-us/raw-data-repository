from sqlalchemy import func
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import make_transient
from typing import List

from rdr_service.config import get_db_config
from rdr_service.dao import database_factory
from rdr_service.model.database import Database
from rdr_service.model.requests_log import RequestsLog


class RequestsLogMigrator:
    def __init__(self, target_instance_name, batch_size=500):
        self.target_instance_name = target_instance_name
        self.source_table_name = RequestsLog.__tablename__
        self.target_table_name = f'{self.source_table_name}_cron'
        self.batch_size = batch_size

    def migrate_latest_requests_logs(self):
        migration_target_connection = self._get_database_connection()

        with migration_target_connection.session() as target_session:
            # Find the last request log id that was migrated
            RequestsLog.__table__.name = self.target_table_name
            last_migrated_log_id = target_session.query(func.max(RequestsLog.id)).scalar()

            # Get all the new request logs that need to be migrated
            database_connection = database_factory.get_database()
            with database_connection.session() as source_session:
                while True:  # Migrate batches until caught up
                    requests_logs = self._load_request_logs(
                        last_migrated_id=last_migrated_log_id,
                        session=source_session
                    )
                    self._migrate_request_logs(requests_logs, session=target_session)

                    if len(requests_logs) < self.batch_size:
                        # End the loop if we didn't retrieve a full batch.
                        # Note: There's a chance that we could get stuck in an infinite loop of checking for
                        #       more if we only end when there's nothing
                        break
                    else:
                        last_migrated_log_id = requests_logs[-1].id

    def _load_request_logs(self, last_migrated_id, session):
        RequestsLog.__table__.name = self.source_table_name

        query = session.query(RequestsLog).order_by(RequestsLog.id)
        if last_migrated_id is not None:
            query = query.filter(RequestsLog.id > last_migrated_id)

        return query.limit(self.batch_size).all()

    def _migrate_request_logs(self, request_logs: List[RequestsLog], session):
        for log in request_logs:
            # Get the request_logs objects ready to upload to the new database
            make_transient(log)

        RequestsLog.__table__.name = self.target_table_name
        session.bulk_save_objects(request_logs)

    def _get_database_connection(self):
        db_config = get_db_config()
        connection_string = db_config['db_connection_pattern'].format(
            driver='mysql+mysqldb' if 'mysql' in self.target_instance_name else 'postgres',
            user='rdr',
            password=db_config[f'{self.target_instance_name}_password'],
            db_instance_name=self.target_instance_name,
            database_name='rdr'
        )
        return Database(make_url(connection_string))
