from datetime import datetime

from rdr_service.tools.tool_libs.app_engine_manager import DeployAppClass
from tests.helpers.unittest_base import BaseTestCase


class AppEngineManagerTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def test_deploy_date_calculation(self):
        expected_release_date = 'Jan 21, 2021'

        run_date = datetime(2021, 1, 14, 15, 49)  # 2021-1-14 3:49pm
        self.assertEqual(expected_release_date, DeployAppClass.find_prod_release_date(run_date),
                         'Deploying on a Thursday should give a release date for the next Thursday')

        run_date = datetime(2021, 1, 15, 17, 16)  # 2021-1-15 5:16pm
        self.assertEqual(expected_release_date, DeployAppClass.find_prod_release_date(run_date),
                         'Deploying on a Friday should also give a release date for the next Thursday')

        run_date = datetime(2021, 1, 18, 9, 43)  # 2021-1-18 9:43am
        self.assertEqual(expected_release_date, DeployAppClass.find_prod_release_date(run_date),
                         'A late release on a Monday morning should give a release date for the upcoming Thursday')

        run_date = datetime(2021, 1, 13, 18, 9)  # 2021-1-13 6:09pm
        self.assertEqual(expected_release_date, DeployAppClass.find_prod_release_date(run_date),
                         'Cutting a release early on Wednesday should set the deploy time for next week')
