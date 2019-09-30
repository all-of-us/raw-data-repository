#
# Celery configuration
#
# https://docs.celeryproject.org/en/latest/userguide/configuration.html
#
# https://medium.com/@taylorhughes/three-quick-tips-from-two-years-with-celery-c05ff9d7f9eb
# https://flask.palletsprojects.com/en/1.1.x/patterns/celery/
# https://stackoverflow.com/questions/56179319/running-celery-as-a-flask-app-with-gunicorn
# https://stackoverflow.com/questions/31999269/how-to-setup-sqlalchemy-session-in-celery-tasks-with-no-global-variable
#
# AMQP version issue: https://github.com/celery/py-amqp/issues/242
#
from celery import Celery

# Every python module that has a Celery task must be included in this list.
_celery_includes = [
    'rdr_service.dao.bq_code_dao',
    'rdr_service.dao.bq_questionnaire_dao',
    'rdr_service.dao.bq_participant_summary_dao',
    'rdr_service.api.data_gen_api',
    'rdr_service.offline.sync_consent_files',
    'rdr_service.celery_test',
    'rdr_service.offline.bigquery_sync'
]

def configure_celery(flask_app):
    """
    Allow background tasks to have Flask context support.
    # https://flask.palletsprojects.com/en/1.1.x/patterns/celery/
    :param flask_app: Flask application object
    """
    celery = Celery(
        flask_app.import_name,
        broker=flask_app.config['CELERY_BROKER_URL'],
        backend=flask_app.config['RESULT_BACKEND'],
        task_serializer='json',
        include=_celery_includes,
    )
    # Configure additional celery settings here.
    celery.conf.update(
        flask_app.config,  # Merge flask app configuration settings
        # https://docs.celeryproject.org/en/latest/userguide/configuration.html#database-engine-options
        database_engine_options={'echo': False},  # Set to True to see Sqlalchemy output in stdout.
        # https://docs.celeryproject.org/en/latest/userguide/configuration.html#task-soft-time-limit
        task_soft_time_limit=60*58,  # 58 minutes
        # https://docs.celeryproject.org/en/latest/userguide/configuration.html#task-time-limit
        task_time_limit=60*60,  # 60 minutes
    )

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

