import datetime

from rdr_service.clock import FakeClock
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import UNSET_HPO_ID
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.mysql_helper_data import AZ_HPO_ID, PITT_HPO_ID


class OrganizationDaoTest(BaseTestCase):
    def setUp(self):
        super(OrganizationDaoTest, self).setUp()
        self.organization_dao = OrganizationDao()
        self.participant_dao = ParticipantDao()
        self.ps_dao = ParticipantSummaryDao()
        self.ps_history = ParticipantHistoryDao()

    def test_insert(self):
        organization = Organization(externalId="myorg", displayName="myorg_display", hpoId=PITT_HPO_ID, isObsolete=1)
        created_organization = self.organization_dao.insert(organization)
        new_organization = self.organization_dao.get(created_organization.organizationId)
        organization.organizationId = created_organization.organizationId
        organization.isObsolete = new_organization.isObsolete
        self.assertEqual(organization.asdict(), new_organization.asdict())

    def test_participant_pairing_updates_onchange(self):
        provider_link = '[{"organization": {"reference": "Organization/AZ_TUCSON"}, "primary": true}]'
        TIME = datetime.datetime(2018, 1, 1)
        TIME2 = datetime.datetime(2018, 1, 2)
        insert_org = self.organization_dao.insert(
            Organization(externalId="tardis", displayName="bluebox", hpoId=PITT_HPO_ID)
        )

        with FakeClock(TIME):
            self.participant_dao.insert(Participant(participantId=1, biobankId=2))
            participant = self.participant_dao.get(1)
            participant.organizationId = insert_org.organizationId
            self.participant_dao.update(participant)

            self.assertEqual(participant.hpoId, insert_org.hpoId)
            participant = self.participant_dao.get(1)
            p_summary = self.ps_dao.insert(self.participant_summary(participant))

        with FakeClock(TIME2):
            insert_org.hpoId = AZ_HPO_ID
            self.organization_dao.update(insert_org)

        new_org = self.organization_dao.get_by_external_id("tardis")
        ps = self.ps_dao.get(p_summary.participantId)
        ph = self.ps_history.get([participant.participantId, 2])
        participant = self.participant_dao.get(1)

        self.assertEqual(ps.lastModified, TIME2)
        self.assertEqual(ps.hpoId, new_org.hpoId)
        self.assertEqual(ph.hpoId, insert_org.hpoId)
        self.assertEqual(ph.organizationId, insert_org.organizationId)
        self.assertEqual(new_org.hpoId, participant.hpoId)
        self.assertEqual(new_org.organizationId, participant.organizationId)
        self.assertIsNone(participant.siteId)
        self.assertEqual(participant.providerLink, provider_link)

    def test_participant_different_hpo_does_not_change(self):
        insert_org = self.organization_dao.insert(
            Organization(externalId="stark_industries", displayName="ironman", hpoId=PITT_HPO_ID)
        )

        self.participant_dao.insert(Participant(participantId=1, biobankId=2))
        participant = self.participant_dao.get(1)
        participant.hpoId = UNSET_HPO_ID
        self.participant_dao.update(participant)
        insert_org.hpoId = AZ_HPO_ID
        self.organization_dao.update(insert_org)
        new_org = self.organization_dao.get_by_external_id("stark_industries")
        participant = self.participant_dao.get(1)
        self.assertNotEqual(new_org.hpoId, participant.hpoId)
        self.assertEqual(new_org.hpoId, AZ_HPO_ID)
        self.assertEqual(participant.hpoId, UNSET_HPO_ID)
