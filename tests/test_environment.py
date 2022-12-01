import os
import sys

from rdr_service.dao.hpo_dao import HPODao
from rdr_service.model.hpo import HPO
from tests.helpers.unittest_base import BaseTestCase


class TestEnvironment(BaseTestCase):
    def test_python_version(self):
        """ Make sure we are using Python 3.7 or higher """
        self.assertEqual(sys.version_info[0], 3)
        self.assertGreaterEqual(sys.version_info[1], 7)

    def test_project_structure(self):
        """ Test that the project structure looks correct """
        cwd = os.path.curdir
        self.assertTrue(os.path.exists(os.path.join(cwd, 'rdr_service')))
        self.assertTrue(os.path.exists(os.path.join(cwd, 'rdr_service/tools')))
        self.assertTrue(os.path.exists(os.path.join(cwd, 'rdr_service/main.py')))

    def test_flask_app(self):
        """
        Test that we can import the flask app object and get the version id.
        https://realpython.com/python-testing/#how-to-use-unittest-and-flask
        """
        from rdr_service.main import app
        self.assertTrue(isinstance(app, object))
        # Put flask in testing mode
        app.testing = True

        client = app.test_client()
        resp = client.get('/')
        self.assertEqual(resp.json['version_id'], 'develop')

    def test_basic_db_query(self):
        """
        Test that we are connected to the database and can complete a query.
        """
        with HPODao().session() as session:
            count = session.query(HPO).count()
            self.assertGreater(count, 0)
