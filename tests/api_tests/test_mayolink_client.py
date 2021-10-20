import mock

from rdr_service.api.mayolink_api import MayoLinkApi, MayoLinkOrder, MayolinkQuestion, MayoLinkTest, \
    MayolinkTestPassthroughFields
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

    @mock.patch('rdr_service.api.mayolink_api.httplib2')
    def test_order_xml_structure(self, http_mock):
        """Make sure the resulting xml lines up with the order object sent using the client interface"""
        order = self._get_default_order()

        client = MayoLinkApi()
        request_mock = http_mock.Http.return_value.request
        request_mock.return_value = ({'status': '201'}, b'<result></result>')
        with mock.patch('rdr_service.api.mayolink_api.check_auth'):
            client.post(order)

        sent_xml = request_mock.call_args.kwargs['body']
        self.assertEqual(
            b'<orders xmlns="http://orders.mayomedicallaboratories.com"><order>'
            b'<collected>2021-05-01</collected>'
            b'<account>1122</account><number>12345</number>'
            b'<patient>'
            b'<medical_record_number>Z6789</medical_record_number>'
            b'<first_name>*</first_name><last_name>Smith</last_name><middle_name />'
            b'<birth_date>3/3/1933</birth_date><gender>U</gender>'
            b'<address1>1234 Main</address1><address2>Apt C</address2>'
            b'<city>Test</city><state>TN</state><postal_code>11223</postal_code>'
            b'<phone>442-123-4567</phone>'
            b'<account_number /><race>NA</race><ethnic_group />'
            b'</patient>'
            b'<physician><name>None</name><phone /><npi /></physician>'
            b'<report_notes>testing notes</report_notes>'
            b'<tests /><comments />'
            b'</order></orders>',
            sent_xml
        )

    @mock.patch('rdr_service.api.mayolink_api.httplib2')
    def test_order_test_collection_data(self, http_mock):
        """Test the data structure with a test object provided (following the process used for mailkit orders)"""
        order = self._get_default_order()
        order.report_notes = ''
        order.tests = [
            MayoLinkTest(
                code='1SAL',
                name='Unittest',
                comments='Test object for testing'
            ),
            MayoLinkTest(
                code='1ED04',
                name='blood test',
                comments='Another object for testing'
            )
        ]

        client = MayoLinkApi()
        request_mock = http_mock.Http.return_value.request
        request_mock.return_value = ({'status': '201'}, b'<result></result>')
        with mock.patch('rdr_service.api.mayolink_api.check_auth'):
            client.post(order)

        sent_xml = request_mock.call_args.kwargs['body']
        self.assertEqual(
            b'<orders xmlns="http://orders.mayomedicallaboratories.com"><order>'
            b'<collected>2021-05-01</collected>'
            b'<account>1122</account><number>12345</number>'
            b'<patient>'
            b'<medical_record_number>Z6789</medical_record_number>'
            b'<first_name>*</first_name><last_name>Smith</last_name><middle_name />'
            b'<birth_date>3/3/1933</birth_date><gender>U</gender>'
            b'<address1>1234 Main</address1><address2>Apt C</address2>'
            b'<city>Test</city><state>TN</state><postal_code>11223</postal_code>'
            b'<phone>442-123-4567</phone>'
            b'<account_number /><race>NA</race><ethnic_group />'
            b'</patient>'
            b'<physician><name>None</name><phone /><npi /></physician>'
            b'<report_notes />'
            b'<tests>'
            b'<test><code>1SAL</code><name>Unittest</name><comments>Test object for testing</comments></test>'
            b'<test><code>1ED04</code><name>blood test</name><comments>Another object for testing</comments></test>'
            b'</tests>'
            b'<comments />'
            b'</order></orders>',
            sent_xml
        )

    @mock.patch('rdr_service.api.mayolink_api.httplib2')
    def test_passthrough_fields(self, http_mock):
        """Test the data structure with passthrough fields added in"""
        order = MayoLinkOrder(
            collected='2021-05-01',
            number='12345',
            medical_record_number='Z6789',
            last_name='Smith',
            sex='U',
            address1='1234 Main',
            address2='Apt C',
            city='Test',
            state='TN',
            postal_code='11223',
            phone='442-123-4567',
            race='NA',
            tests=[MayoLinkTest(
                code='1SAL',
                name='Unittest',
                comments='Test object for testing',
                passthrough_fields=MayolinkTestPassthroughFields(
                    field3='testing third pass-through field'
                )
            )]
        )

        client = MayoLinkApi()
        request_mock = http_mock.Http.return_value.request
        request_mock.return_value = ({'status': '201'}, b'<result></result>')
        with mock.patch('rdr_service.api.mayolink_api.check_auth'):
            client.post(order)

        sent_xml = request_mock.call_args.kwargs['body']
        self.assertEqual(
            b'<orders xmlns="http://orders.mayomedicallaboratories.com"><order>'
            b'<collected>2021-05-01</collected>'
            b'<account>1122</account><number>12345</number>'
            b'<patient>'
            b'<medical_record_number>Z6789</medical_record_number>'
            b'<first_name>*</first_name><last_name>Smith</last_name><middle_name />'
            b'<birth_date>3/3/1933</birth_date><gender>U</gender>'
            b'<address1>1234 Main</address1><address2>Apt C</address2>'
            b'<city>Test</city><state>TN</state><postal_code>11223</postal_code>'
            b'<phone>442-123-4567</phone>'
            b'<account_number /><race>NA</race><ethnic_group />'
            b'</patient>'
            b'<physician><name>None</name><phone /><npi /></physician>'
            b'<report_notes />'
            b'<tests><test>'
            b'<code>1SAL</code><name>Unittest</name><comments>Test object for testing</comments>'
            b'<client_passthrough_fields>'
            b'<field1 />'
            b'<field2 />'
            b'<field3>testing third pass-through field</field3>'
            b'<field4 />'
            b'</client_passthrough_fields>'
            b'</test></tests>'
            b'<comments />'
            b'</order></orders>',
            sent_xml
        )

    @mock.patch('rdr_service.api.mayolink_api.httplib2')
    def test_question_fields(self, http_mock):
        """Test the data structure with questions fields added in"""
        order = self._get_default_order()
        order.tests = [MayoLinkTest(
            code='1SAL',
            name='Unittest',
            comments='Test object for testing',
            questions=[
                MayolinkQuestion(code='Q1', prompt='Question 1', answer='Answer 1'),
                MayolinkQuestion(code='Q2', prompt='Question 2', answer='Answer 2')
            ]
        )]

        client = MayoLinkApi()
        request_mock = http_mock.Http.return_value.request
        request_mock.return_value = ({'status': '201'}, b'<result></result>')
        with mock.patch('rdr_service.api.mayolink_api.check_auth'):
            client.post(order)

        sent_xml = request_mock.call_args.kwargs['body']
        self.assertEqual(
            b'<orders xmlns="http://orders.mayomedicallaboratories.com"><order>'
            b'<collected>2021-05-01</collected>'
            b'<account>1122</account><number>12345</number>'
            b'<patient>'
            b'<medical_record_number>Z6789</medical_record_number>'
            b'<first_name>*</first_name><last_name>Smith</last_name><middle_name />'
            b'<birth_date>3/3/1933</birth_date><gender>U</gender>'
            b'<address1>1234 Main</address1><address2>Apt C</address2>'
            b'<city>Test</city><state>TN</state><postal_code>11223</postal_code>'
            b'<phone>442-123-4567</phone>'
            b'<account_number /><race>NA</race><ethnic_group />'
            b'</patient>'
            b'<physician><name>None</name><phone /><npi /></physician>'
            b'<report_notes />'
            b'<tests><test>'
            b'<code>1SAL</code><name>Unittest</name><comments>Test object for testing</comments>'
            b'<questions>'
            b'<question><code>Q1</code><prompt>Question 1</prompt><answer>Answer 1</answer></question>'
            b'<question><code>Q2</code><prompt>Question 2</prompt><answer>Answer 2</answer></question>'
            b'</questions>'
            b'</test></tests>'
            b'<comments />'
            b'</order></orders>',
            sent_xml
        )

    def _get_default_order(self):
        return MayoLinkOrder(
            collected='2021-05-01',
            number='12345',
            medical_record_number='Z6789',
            last_name='Smith',
            sex='U',
            address1='1234 Main',
            address2='Apt C',
            city='Test',
            state='TN',
            postal_code='11223',
            phone='442-123-4567',
            race='NA',
            comments='test data',
            report_notes='testing notes'
        )
