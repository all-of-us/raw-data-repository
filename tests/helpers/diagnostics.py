import logging


class LoggingDatabaseActivity:
    def __enter__(self):
        self._sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
        self._original_log_level = self._sqlalchemy_logger.level  # Store the level so it can be restored later
        self._sqlalchemy_logger.setLevel(logging.INFO)  # INFO will print all SQL run against the database

    def __exit__(self, *_):
        self._sqlalchemy_logger.setLevel(self._original_log_level)
