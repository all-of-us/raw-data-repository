from datetime import datetime
import mock

from rdr_service.tools.tool_libs.app_engine_manager import DeployAppClass, CronSettingsAggregator
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


class CronSettingsTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super(CronSettingsTest, self).setUp(*args, **kwargs)

        open_patcher = mock.patch('rdr_service.tools.tool_libs.app_engine_manager.open')
        self.open_mock = open_patcher.start()
        self.file_mock = self.open_mock.return_value.__enter__.return_value
        self.addCleanup(self.open_mock.stop)

        # Make it appear as if any files exist
        os_patcher = mock.patch('rdr_service.tools.tool_libs.app_engine_manager.os')
        os_mock = os_patcher.start()
        os_mock.path.exists.return_value = True
        self.addCleanup(os_patcher.stop)

    def test_basic_cron_file_generation(self):
        """Make sure separate files can be put together"""
        first_file_path = 'first'
        second_file_path = 'second'

        def return_file():
            path = self.open_mock.call_args.args[0]  # Retrieve the path of the file that was opened
            if path == first_file_path:
                return """
                    {
                        "basic job configuration": {
                            "url": "/offline/test",
                            "schedule": "every day 12:00",
                            "timezone": "Central",
                            "target": "offline"
                        }
                    }
                """
            elif path == second_file_path:
                return """
                    {
                        "another job": {
                            "url": "/offline/other",
                            "schedule": "jan 1 12:00",
                            "timezone": "Central",
                            "target": "resource"
                        }
                    }
                """
        self.file_mock.read.side_effect = return_file

        generator = CronSettingsAggregator()
        generator.extend_with_file(first_file_path)
        generator.extend_with_file(second_file_path)

        expected = self.clean_multiline_str("""
            cron:
            - description: basic job configuration
              schedule: every day 12:00
              target: offline
              timezone: Central
              url: /offline/test
            - description: another job
              schedule: jan 1 12:00
              target: resource
              timezone: Central
              url: /offline/other
        """)
        self.assertEqual(expected, generator.get_config_file_contents().strip())

    def test_overriding_values(self):
        """Test that later files override entries from the first file"""
        default_path = 'default'
        env_path = 'env'

        def return_file():
            path = self.open_mock.call_args.args[0]  # Retrieve the path of the file that was opened
            if path == default_path:
                return """
                    {
                        "test job to override": {
                            "url": "/offline/test",
                            "schedule": "every day 12:00"
                        }
                    }
                """
            elif path == env_path:
                return """
                    {
                        "test job to override": {
                            "url": "/offline/other",
                            "schedule": "every Sunday at 5"
                        }
                    }
                """
        self.file_mock.read.side_effect = return_file

        generator = CronSettingsAggregator()
        generator.extend_with_file(default_path)
        generator.extend_with_file(env_path)

        expected = self.clean_multiline_str("""
            cron:
            - description: test job to override
              schedule: every Sunday at 5
              url: /offline/other
        """)  # Order of elements is arbitrary here
        self.assertEqual(expected, generator.get_config_file_contents().strip())

    def test_removing_entries(self):
        """Test that later files can remove entries from the list"""
        default_path = 'default'
        env_path = 'env'

        def return_file():
            path = self.open_mock.call_args.args[0]  # Retrieve the path of the file that was opened
            if path == default_path:
                return """
                    {
                        "test job": {
                            "url": "/offline/test",
                            "schedule": "every day 12:00"
                        },
                        "entry to remove": {
                            "url": "/offline/test",
                            "schedule": "every monday at 8"
                        }
                    }
                """
            elif path == env_path:
                return """
                    {
                        "another filler job": {
                            "url": "/offline/another",
                            "schedule": "Sunday at 5"
                        },
                        "entry to remove": {}
                    }
                """
        self.file_mock.read.side_effect = return_file

        generator = CronSettingsAggregator()
        generator.extend_with_file(default_path)
        generator.extend_with_file(env_path)

        expected = self.clean_multiline_str("""
            cron:
            - description: test job
              schedule: every day 12:00
              url: /offline/test
            - description: another filler job
              schedule: Sunday at 5
              url: /offline/another
        """)  # Order of elements is arbitrary here
        self.assertEqual(expected, generator.get_config_file_contents().strip())
