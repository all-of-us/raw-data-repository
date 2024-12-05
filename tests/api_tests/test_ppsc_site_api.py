import http.client
import random
from copy import deepcopy

from rdr_service import config
from rdr_service.api_util import PPSC, RDR, HEALTHPRO
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.ppsc_dao import SiteDao, PPSCDefaultBaseDao
from rdr_service.dao.site_dao import SiteDao as LegacySiteDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc import PartnerEventActivity
from rdr_service.model.site_enums import EnrollingStatus, DigitalSchedulingStatus
from rdr_service.participant_enums import OrganizationType
from tests.helpers.unittest_base import BaseTestCase


class PPSCSiteAPITest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.site_dao = SiteDao()
        self.partner_event_activity_dao = PPSCDefaultBaseDao(model_type=PartnerEventActivity)

        self.hpo_dao = HPODao()
        self.organization_dao = OrganizationDao()
        self.legacy_site_dao = LegacySiteDao()

        activities = ['Site Update']
        for activity in activities:
            self.ppsc_data_gen.create_database_partner_activity(
                name=activity
            )

        self.base_payload = {
            "city": "Minneapolis",
            "phone": None,
            "state": "MN",
            "active": 1,
            "org_id": "Core_Platform_Test_Org_1",
            "site_name": "Core Platform Test Site 1-Red",
            "site_type": "Clinic Site",
            "awardee_id": "Core_Platform_Test_Award",
            "postal_code": "55415",
            "address_line": "713 Washington Ave S Ste 101",
            "awardee_name": "Core Platform Test Award",
            "awardee_type": "HPO",
            "mayo_link_id": "7044418",
            "location_name": None,
            "site_identifier": "hpo-site-monroeville",
            "organization_name": "Core Platform Test Org 1",
            "anticipated_launch_date": None,
            "enrollment_status_active": True,
            "scheduling_status_active": None,
            "digital_scheduling_status_active": True
        }

    def overwrite_test_user_roles(self, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def test_ppsc_role_validation(self):

        accepted_roles = [PPSC, RDR]

        self.overwrite_test_user_roles(
            [random.choice(accepted_roles)]
        )

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_post('Site', request_data=self.base_payload, expected_status=http.client.FORBIDDEN)
        self.assertTrue(response.status_code == 403)

    def test_ppsc_site_req_keys_validation(self):

        missing_site_type_payload = {
            "awardee_type": "HPO",
            "awardee_id": "PITT",
            "awardee_name": "Pittsburgh awardee",
            "org_id": "PITT_UPMC",
            "organization_name": "Pittsburgh Health",
            "site_name": "UPMC Urgent Care Monroeville",
            "site_identifier": "hpo-site-monroeville",
            "enrollment_status_active": True,
            "digital_scheduling_status_active": True,
            "scheduling_status_active": True,
            "anticipated_launch_date": "2024-03-26",
            "location_name": "Thompson Building",
            "mayo_link_id": "123456",
            "active": True,
            "address_line": "1234 Fake St.",
            "city": "Springfield",
            "state": "VA",
            "postal_code": "22150",
            "phone": "7031234567",
        }

        response = self.send_post('Site', request_data=missing_site_type_payload,
                                  expected_status=http.client.BAD_REQUEST)
        self.assertTrue(response.status_code == 400)
        self.assertTrue(response.json.get(
            'message') == 'Payload for Site is invalid: Required keys - awardee_id, awardee_type, '
                          'awardee_name, org_id, organization_name, site_name, '
                          'site_identifier, site_type, enrollment_status_active, mayo_link_id, active, state')

    def test_site_data_inserts(self):

        response = self.send_post('Site', request_data=self.base_payload)

        self.assertTrue(response is not None)
        self.assertEqual(response, 'Site hpo-site-monroeville was created successfully')

        current_site_data = [obj for obj in self.site_dao.get_all()
                                if obj.site_identifier == self.base_payload.get('site_identifier')]

        self.assertEqual(len(current_site_data), 1)

        current_site = current_site_data[0].asdict()

        for k, v in self.base_payload.items():
            if v is None:
                self.assertTrue(current_site.get(k) == 0 or current_site.get(k) is None)
            else:
                self.assertTrue(current_site.get(k) == v)

    def test_bad_data_throws_exception(self):

        self.base_payload['bad_key'] = 'bad_value'

        response = self.send_post('Site', request_data=self.base_payload, expected_status=http.client.BAD_REQUEST)

        self.assertTrue(response is not None)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Error when creating/updating site record')

        current_site_data = [obj for obj in self.site_dao.get_all()
                             if obj.site_identifier == self.base_payload.get('site_identifier')]

        self.assertEqual(len(current_site_data), 0)

    def test_site_data_deactivates(self):

        # create site record
        response = self.send_post('Site', request_data=self.base_payload)

        self.assertTrue(response is not None)
        self.assertEqual(response, 'Site hpo-site-monroeville was created successfully')

        current_site_data = [obj for obj in self.site_dao.get_all()
                             if obj.site_identifier == self.base_payload.get('site_identifier')]

        self.assertEqual(len(current_site_data), 1)

        current_legacy_site = [obj for obj in self.legacy_site_dao.get_all() if obj.googleGroup ==
                               self.base_payload.get(
            'site_identifier')]

        self.assertEqual(len(current_legacy_site), 1)
        self.assertEqual(current_legacy_site[0].googleGroup, self.base_payload.get('site_identifier'))
        self.assertEqual(current_legacy_site[0].enrollingStatus, EnrollingStatus('ACTIVE'))
        self.assertEqual(current_legacy_site[0].digitalSchedulingStatus, DigitalSchedulingStatus('ACTIVE'))

        deactivate_payload = {
            "awardee_id": "PITT",
            "org_id": "PITT_UPMC",
            "site_name": "UPMC Urgent Care Monroeville",
            "site_identifier": "hpo-site-monroeville"
        }

        response = self.send_delete('Site', request_data=deactivate_payload)

        self.assertTrue(response is not None)
        self.assertEqual(response, 'Site hpo-site-monroeville was deactivated successfully')

        current_site_data = [obj for obj in self.site_dao.get_all()
                             if obj.site_identifier == self.base_payload.get('site_identifier')]

        self.assertEqual(len(current_site_data), 1)
        self.assertEqual(current_site_data[0].active, 0)

        current_legacy_site = [obj for obj in self.legacy_site_dao.get_all() if obj.googleGroup == self.base_payload.get(
            'site_identifier')]

        self.assertEqual(len(current_legacy_site), 1)
        self.assertEqual(current_legacy_site[0].googleGroup, self.base_payload.get('site_identifier'))
        self.assertEqual(current_legacy_site[0].enrollingStatus, EnrollingStatus('INACTIVE'))
        self.assertEqual(current_legacy_site[0].digitalSchedulingStatus, DigitalSchedulingStatus('INACTIVE'))

    def test_site_data_insert_event_deps(self):

        # creating site
        self.send_post('Site', request_data=self.base_payload)

        current_site_data = [obj for obj in self.site_dao.get_all()
                             if obj.site_identifier == self.base_payload.get('site_identifier')]

        # should be 1
        self.assertEqual(len(current_site_data), 1)

        # should be 1
        current_partner_activities = self.partner_event_activity_dao.get_all()
        self.assertEqual(len(current_partner_activities), 1)

    def test_awardee_insert_sync_rdr_schema(self):

        update_payload = {
            'awardee_id': 'PITTS'
        }
        self.base_payload.update(update_payload)

        response = self.send_post('Site', request_data=self.base_payload)
        self.assertTrue(response is not None)

        current_hpo = [obj for obj in self.hpo_dao.get_all() if obj.name == self.base_payload.get('awardee_id')]

        self.assertEqual(len(current_hpo), 1)
        self.assertEqual(current_hpo[0].name, self.base_payload.get('awardee_id'))
        self.assertEqual(current_hpo[0].displayName, self.base_payload.get('awardee_name'))
        self.assertTrue(current_hpo[0].isObsolete is None)
        self.assertEqual(current_hpo[0].organizationType, OrganizationType.HPO)

    def test_awardee_update_sync_rdr_schema(self):

        update_payload = {
            'awardee_id': 'PITTS',
            'awardee_name': 'Pittsburgh Health Place'
        }
        self.base_payload.update(update_payload)

        response = self.send_post('Site', request_data=self.base_payload)
        self.assertTrue(response is not None)

        current_hpo = [obj for obj in self.hpo_dao.get_all() if obj.name == self.base_payload.get('awardee_id')]

        self.assertEqual(len(current_hpo), 1)
        self.assertEqual(current_hpo[0].displayName, update_payload.get('awardee_name'))

    def test_org_insert_sync_rdr_schema(self):

        response = self.send_post('Site', request_data=self.base_payload)
        self.assertTrue(response is not None)

        current_org = [obj for obj in
                       self.organization_dao.get_all() if obj.externalId == self.base_payload.get('org_id').upper()]

        self.assertEqual(len(current_org), 1)
        self.assertEqual(current_org[0].displayName, self.base_payload.get('organization_name'))
        self.assertEqual(current_org[0].externalId, self.base_payload.get('org_id').upper())
        self.assertTrue(current_org[0].isObsolete is None)

        current_hpos = [obj for obj in self.hpo_dao.get_all() if obj.name == self.base_payload.get('awardee_id').upper()]
        self.assertEqual(len(current_hpos), 1)

        self.assertEqual(current_org[0].hpoId, current_hpos[0].hpoId)

    def test_org_update_sync_rdr_schema(self):

        current_hpo = self.data_generator.create_database_hpo()
        current_org = self.data_generator.create_database_organization(
            hpoId=current_hpo.hpoId,
            externalId=self.base_payload.get('org_id')
        )

        self.base_payload['org_id'] = current_org.externalId

        response = self.send_post('Site', request_data=self.base_payload)
        self.assertTrue(response is not None)

        current_org = [obj for obj in
                       self.organization_dao.get_all() if obj.externalId == self.base_payload.get('org_id').upper()]

        self.assertEqual(len(current_org), 1)
        self.assertEqual(current_org[0].displayName, self.base_payload.get('organization_name'))
        self.assertEqual(current_org[0].externalId, self.base_payload.get('org_id').upper())
        self.assertTrue(current_org[0].isObsolete is None)

        current_hpos = [obj for obj in self.hpo_dao.get_all() if obj.name == self.base_payload.get('awardee_id').upper()
                        ]
        self.assertEqual(len(current_hpos), 1)

        self.assertEqual(current_org[0].hpoId, current_hpos[0].hpoId)

    def test_site_insert_sync_rdr_schema(self):

        self.base_payload['site_identifier'] = 'hpo-site-pittsville'

        response = self.send_post('Site', request_data=self.base_payload)
        self.assertTrue(response is not None)

        current_site = [obj for obj in
                        self.legacy_site_dao.get_all() if obj.googleGroup == self.base_payload.get('site_identifier')]
        self.assertEqual(len(current_site), 1)

        self.assertEqual(current_site[0].googleGroup, self.base_payload.get('site_identifier'))
        self.assertEqual(current_site[0].siteName, self.base_payload.get('site_name'))

        current_orgs = [obj for obj in self.organization_dao.get_all() if obj.externalId
                        == self.base_payload.get('org_id').upper()]

        self.assertEqual(len(current_orgs), 1)

        self.assertEqual(current_site[0].organizationId, current_orgs[0].organizationId)

    def test_site_update_sync_rdr_schema(self):

        self.base_payload['postal_code'] = '11111'

        response = self.send_post('Site', request_data=self.base_payload)
        self.assertTrue(response is not None)

        current_site = [obj for obj in
                        self.legacy_site_dao.get_all() if obj.googleGroup == self.base_payload.get('site_identifier')]
        self.assertEqual(len(current_site), 1)

        self.assertEqual(current_site[0].googleGroup, self.base_payload.get('site_identifier'))
        self.assertEqual(current_site[0].zipCode, self.base_payload.get('postal_code'))

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.partner_activity")
        self.clear_table_after_test("ppsc.site")
        self.clear_table_after_test("ppsc.partner_event_activity")
