from rdr_service.services.flask import celery

@celery.task()
def add(x, y):
    return x + y