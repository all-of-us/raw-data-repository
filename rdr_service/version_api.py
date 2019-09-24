"""Version API used for prober and release management.

No auth is required for this endpoint because it serves nothing sensitive.
"""
from time import sleep

from rdr_service.config import GAE_VERSION_ID
from rdr_service.celery_test import add

from flask_restful import Resource


class VersionApi(Resource):
    """Api handler for retrieving version info."""

    def get(self):

        task = add.delay(2, 40)
        count = 30

        while not task.ready() and count > 0:
            count =- 1
            sleep(1.0)

        if not task.ready():
            print('****** Celery Add Task Failed ********')
        else:
            print('****** Celery Add Task Succeeded, The Answer Is: {0} ********'.format(task.get()))


        return {"version_id": GAE_VERSION_ID}
