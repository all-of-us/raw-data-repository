import mock

from tests.helpers.unittest_base import BaseTestCase

class ConsentValidationEndpointTest(BaseTestCase):
    def setUp(self):
        super(ConsentValidationEndpointTest, self).setUp()

        from rdr_service.offline.main import app, OFFLINE_PREFIX
        self.offline_test_client = app.test_client()
        self.url_prefix = OFFLINE_PREFIX

    def test_manual_validation_endpoint(self):
        with mock.patch('rdr_service.offline.main.ConsentValidationController') as controller_class_mock:
            self.send_post(
                'ManuallyValidateFiles',
                test_client=self.offline_test_client,
                prefix=self.url_prefix,
                request_data={
                    'ids': [
                        1234,
                        5678,
                        1212
                    ]
                }
            )

            controller_mock = controller_class_mock.return_value
            controller_mock.validate_all_for_participant.assert_has_calls([
                mock.call(participant_id=1234, output_strategy=mock.ANY),
                mock.call(participant_id=5678, output_strategy=mock.ANY),
                mock.call(participant_id=1212, output_strategy=mock.ANY)
            ])
