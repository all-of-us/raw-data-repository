from typing import List
from os.path import dirname, realpath
from pathlib import Path
from csv import DictReader

from rdr_service.model.study_nph import Site
from rdr_service.dao.study_nph_dao import NphSiteDao
from rdr_service.tools.tool_libs.study_nph_ingest_org_awardee_site_data_from_csv import create_sites_from_csv

from tests.helpers.unittest_base import BaseTestCase


class StudyNphIngestOrgAwardeeDataTest(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.nph_site_dao = NphSiteDao()
        self.csv_path = Path(dirname(dirname(realpath(__file__)))) / "test-data/study_nph_awardee_data.csv"

    def _get_healthpro_site_ids_from_csv_file(self) -> List[str]:
        with open(self.csv_path, 'r', encoding="utf-8-sig") as csv_fp:
            csv_reader = DictReader(csv_fp)
            site_ids = []
            for row in csv_reader:
                site_ids.append(row["healthpro_site_id"])
            return site_ids

    def _get_total_sites_count(self):
        with self.nph_site_dao.session() as session:
            return session.query(Site).count()

    def test_ingest_nph_org_awardee_site_data(self):
        create_sites_from_csv(self.csv_path)
        new_healthpro_site_ids = self._get_healthpro_site_ids_from_csv_file()
        for external_id in new_healthpro_site_ids:
            site = self.nph_site_dao.get_site_from_external_id(external_id=external_id)
            self.assertIsNotNone(site)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.site")
