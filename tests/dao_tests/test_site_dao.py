import datetime
import unittest

from rdr_service.clock import FakeClock
from rdr_service.dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.hpo import HPO
from rdr_service.model.participant import Participant
from rdr_service.model.site import Site
from tests.helpers.mysql_helper import reset_mysql_instance
from tests.helpers.mysql_helper_data import *
from tests.helpers.unittest_base import BaseTestCase


class SiteDaoTest(BaseTestCase):
    def setUp(self):

        self.site_dao = SiteDao()
        self.participant_dao = ParticipantDao()
        self.ps_dao = ParticipantSummaryDao()
        self.ps_history = ParticipantHistoryDao()

    def test_get_no_sites(self):
        self.assertIsNone(self.site_dao.get(9999))
        self.assertIsNone(self.site_dao.get_by_google_group("site@googlegroups.com"))

    def test_insert(self):
        site = Site(
            siteName="site", googleGroup="site@googlegroups.com", mayolinkClientNumber=12345, hpoId=PITT_HPO_ID
        )
        with self.site_dao.session() as session:
            x = session.query(Site.googleGroup).all()
            print(x)
        created_site = self.site_dao.insert(site)
        new_site = self.site_dao.get(created_site.siteId)
        site.siteId = created_site.siteId
        self.assertEqual(site.asdict(), new_site.asdict())
        self.assertEqual(site.asdict(), self.site_dao.get_by_google_group("site@googlegroups.com").asdict())

    def test_update(self):
        site = Site(
            siteName="site", googleGroup="site@googlegroups.com", mayolinkClientNumber=12345, hpoId=PITT_HPO_ID
        )
        created_site = self.site_dao.insert(site)
        new_site = Site(
            siteId=created_site.siteId,
            siteName="site2",
            googleGroup="site2@googlegroups.com",
            mayolinkClientNumber=123456,
            hpoId=UNSET_HPO_ID,
        )
        self.site_dao.update(new_site)
        fetched_site = self.site_dao.get(created_site.siteId)
        self.assertEqual(new_site.asdict(), fetched_site.asdict())
        self.assertEqual(new_site.asdict(), self.site_dao.get_by_google_group("site2@googlegroups.com").asdict())
        self.assertIsNone(self.site_dao.get_by_google_group("site@googlegroups.com"))

    def test_participant_pairing_updates_on_change(self):
        TIME = datetime.datetime(2018, 1, 1)
        TIME2 = datetime.datetime(2018, 1, 2)
        provider_link = '[{"organization": {"reference": "Organization/AZ_TUCSON"}, "primary": true}]'
        site = Site(
            siteName="site",
            googleGroup="site@googlegroups.com",
            mayolinkClientNumber=12345,
            hpoId=PITT_HPO_ID,
            organizationId=PITT_ORG_ID,
        )
        created_site = self.site_dao.insert(site)

        with FakeClock(TIME):
            p = Participant(participantId=1, biobankId=2, siteId=created_site.siteId)
            self.participant_dao.insert(p)
            fetch_p = self.participant_dao.get(p.participantId)
            updated_p = self.participant_dao.get(fetch_p.participantId)
            p_summary = self.ps_dao.insert(self.participant_summary(updated_p))

        with FakeClock(TIME2):
            update_site_parent = Site(
                siteId=created_site.siteId,
                siteName="site2",
                googleGroup="site2@googlegroups.com",
                mayolinkClientNumber=123456,
                hpoId=AZ_HPO_ID,
                organizationId=AZ_ORG_ID,
            )
            self.site_dao.update(update_site_parent)

        updated_p = self.participant_dao.get(fetch_p.participantId)
        ps = self.ps_dao.get(p_summary.participantId)
        ph = self.ps_history.get([updated_p.participantId, 1])

        self.assertEqual(update_site_parent.hpoId, updated_p.hpoId)
        self.assertEqual(update_site_parent.organizationId, updated_p.organizationId)
        self.assertEqual(ps.organizationId, update_site_parent.organizationId)
        self.assertEqual(ps.hpoId, update_site_parent.hpoId)
        self.assertEqual(ps.organizationId, update_site_parent.organizationId)
        self.assertEqual(ph.organizationId, update_site_parent.organizationId)
        self.assertEqual(updated_p.providerLink, provider_link)
        self.assertEqual(ps.lastModified, TIME2)
