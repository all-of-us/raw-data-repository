import http.client
import random
from copy import deepcopy

from rdr_service import config
from rdr_service.api_util import PPSC, RDR, HEALTHPRO
from tests.helpers.unittest_base import BaseTestCase


class PPSCSiteAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def overwrite_test_user_roles(self, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def test_ppsc_role_validation(self):

        accepted_roles = [PPSC, RDR]

        self.overwrite_test_user_roles(
            [random.choice(accepted_roles)]
        )

        payload = {
            'participantId': 'P22'
        }

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_post('Site', request_data=payload, expected_status=http.client.FORBIDDEN)
        self.assertTrue(response.status_code == 403)

    def test_site_data_inserts(self):
        ...

    def test_site_data_updates(self):
        ...

    def test_site_data_deletes(self):
        ...

    def test_site_data_insert_event_deps(self):
        ...

    def test_extra_keys_removed(self):
        ...

    def test_site_action_updates_rdr_schema_tables(self):
        ...

    def tearDown(self):
        super().tearDown()
