from model.organization import Organization
from unit_test.unit_test_util import PITT_ORG_ID, AZ_ORG_ID
from unit_test_util import SqlTestBase, PITT_HPO_ID, UNSET_HPO_ID, AZ_HPO_ID
from dao.site_dao import SiteDao
from model.site import Site
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
    insert_org.hpoId = 4
    self.organization_dao.update(insert_org)
    new_org = self.organization_dao.get_by_external_id('tardis')
    participant = self.participant_dao.get(1)

    self.assertNotEquals(participant.hpoId, PITT_HPO_ID)
    self.assertEquals(new_org.hpoId, participant.hpoId)
    self.assertEquals(new_org.organizationId, participant.organizationId)
    self.assertIsNone(participant.siteId)
