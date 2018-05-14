from model.organization import Organization
from unit_test_util import SqlTestBase, PITT_HPO_ID, AZ_HPO_ID
from participant_enums import UNSET_HPO_ID
from dao.organization_dao import OrganizationDao
from model.participant import Participant
from dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from dao.participant_summary_dao import ParticipantSummaryDao

class OrganizationDaoTest(SqlTestBase):

  def setUp(self):
    super(OrganizationDaoTest, self).setUp()
    self.organization_dao = OrganizationDao()
    self.participant_dao = ParticipantDao()
    self.ps_dao = ParticipantSummaryDao()
    self.ps_history = ParticipantHistoryDao()

  def test_insert(self):
    organization = Organization(externalId='myorg', displayName='myorg_display', hpoId=PITT_HPO_ID)
    created_organization = self.organization_dao.insert(organization)
    new_organization = self.organization_dao.get(created_organization.organizationId)
    organization.organizationId = created_organization.organizationId
    self.assertEquals(organization.asdict(), new_organization.asdict())

  def test_participant_pairing_updates_onchange(self):
    insert_org = self.organization_dao.insert(
                  Organization(externalId='tardis', displayName='bluebox', hpoId=PITT_HPO_ID))

    self.participant_dao.insert(Participant(participantId=1, biobankId=2))
    participant = self.participant_dao.get(1)
    participant.organizationId = insert_org.organizationId
    self.participant_dao.update(participant)

    self.assertEquals(participant.hpoId, insert_org.hpoId)
    insert_org.hpoId = AZ_HPO_ID
    self.organization_dao.update(insert_org)
    new_org = self.organization_dao.get_by_external_id('tardis')
    participant = self.participant_dao.get(1)
    p_summary = self.ps_dao.insert(self.participant_summary(participant))
    ps = self.ps_dao.get(p_summary.participantId)
    ph = self.ps_history.get([participant.participantId, 2])
    provider_link = '[{"organization": {"reference": "Organization/AZ_TUCSON"}, "primary": true}]'

    self.assertEquals(ps.hpoId, new_org.hpoId)
    self.assertEquals(ph.hpoId, insert_org.hpoId)
    self.assertEquals(ph.organizationId, insert_org.organizationId)
    self.assertEquals(new_org.hpoId, participant.hpoId)
    self.assertEquals(new_org.organizationId, participant.organizationId)
    self.assertIsNone(participant.siteId)
    self.assertEquals(participant.providerLink, provider_link)

  def test_participant_different_hpo_does_not_change(self):
    insert_org = self.organization_dao.insert(
      Organization(externalId='stark_industries', displayName='ironman', hpoId=PITT_HPO_ID))

    self.participant_dao.insert(Participant(participantId=1, biobankId=2))
    participant = self.participant_dao.get(1)
    participant.hpoId = UNSET_HPO_ID
    self.participant_dao.update(participant)
    insert_org.hpoId = AZ_HPO_ID
    self.organization_dao.update(insert_org)
    new_org = self.organization_dao.get_by_external_id('stark_industries')
    participant = self.participant_dao.get(1)
    self.assertNotEqual(new_org.hpoId, participant.hpoId)
    self.assertEqual(new_org.hpoId, AZ_HPO_ID)
    self.assertEqual(participant.hpoId, UNSET_HPO_ID)
