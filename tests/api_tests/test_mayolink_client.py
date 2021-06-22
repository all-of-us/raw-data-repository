import mock

from rdr_service.api.mayolink_api import MayoLinkApi
from tests.helpers.unittest_base import BaseTestCase


class MayolinkClientTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(MayolinkClientTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(MayolinkClientTest, self).setUp(*args, **kwargs)

        open_cloud_file_patch = mock.patch('rdr_service.api.mayolink_api.open_cloud_file')
        self.open_cloud_file_mock = open_cloud_file_patch.start()
        self.addCleanup(open_cloud_file_patch.stop)

        self.open_cloud_file_mock.return_value.__enter__.return_value.read.return_value = """
            {
                "default": {
                    "username": "test_user",
                    "password": "1234",
                    "account": 1122
                },
                "version_two": {
                    "username": "v2_user",
                    "password": "9876",
                    "account": 8765
                }
            }
        """

    def test_default_credentials(self):
        """Test that the client uses the default account by default"""
        mayolink_client = MayoLinkApi()
        self.assertEqual('test_user', mayolink_client.username)
        self.assertEqual('1234', mayolink_client.pw)
        self.assertEqual(1122, mayolink_client.account)

    def test_specific_account_credentials(self):
        """Test that the client switches to the new credentials when specified"""
        mayolink_client = MayoLinkApi(credentials_key='version_two')
        self.assertEqual('v2_user', mayolink_client.username)
        self.assertEqual('9876', mayolink_client.pw)
        self.assertEqual(8765, mayolink_client.account)

    def test_new_code_with_old_file(self):
        """
        Test that the new code can work with the previous file structure.
        This way the code can deploy, and we can take our time updating the file structure.
        """
        self.open_cloud_file_mock.return_value.__enter__.return_value.read.return_value = """
            {
                "username": "legacy_user",
                "password": "9283",
                "account": 7676
            }
        """

        mayolink_client = MayoLinkApi()
        self.assertEqual('legacy_user', mayolink_client.username)
        self.assertEqual('9283', mayolink_client.pw)
        self.assertEqual(7676, mayolink_client.account)

    def test_empty_field_in_xml(self):
        """Making sure an empty field gets sent in the xml"""
        client = MayoLinkApi()
        xml_output = client.__dict_to_mayo_xml__({
            'order': {
                'blank': None
            }
        })
        self.assertEqual(
            b'<orders xmlns="http://orders.mayomedicallaboratories.com">'
            b'<order>'
            b'<blank />'
            b'<account>1122</account>'
            b'</order>'
            b'</orders>',
            xml_output
        )
