import json
import os
import sys
import unittest

from tests.helpers.mysql_helper import start_mysql_instance


class TestEnvironment(unittest.TestCase):
    """
    Test our python environment
    """

    def setUp(self) -> None:
        # start_mysql_instance()
        pass

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

        app.testing = True
        client = app.test_client()

        resp = client.get('/')
        self.assertEqual(resp.json['version_id'], 'develop')

