from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult
from tests.helpers.unittest_base import BaseTestCase


class GenomicDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.member_dao = GenomicSetMemberDao()

    def test_update_member_job_run_id(self):
        bad_field_passed = self.member_dao.update_member_job_run_id([], 1, 'badFieldName')
        self.assertEqual(bad_field_passed, GenomicSubProcessResult.ERROR)

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        for num in range(6):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="11111111",
                sampleId="222222222222",
                genomeType="aou_wgs",
            )

        members = self.member_dao.get_all()
        member_ids = [m.id for m in members]

        mid = len(member_ids) // 2
        first = member_ids[:mid]
        second = member_ids[mid:]

        first_set = self.member_dao.update_member_job_run_id(first, gen_job_run.id, 'gemA1ManifestJobRunId')

        self.assertEqual(first_set, GenomicSubProcessResult.SUCCESS)

        for num in first:
            member = self.member_dao.get(num)
            self.assertEqual(member.gemA1ManifestJobRunId, gen_job_run.id)

        second_set = self.member_dao.update_member_job_run_id(second, gen_job_run.id, 'aw3ManifestJobRunID')

        self.assertEqual(second_set, GenomicSubProcessResult.SUCCESS)

        for num in second:
            member = self.member_dao.get(num)
            self.assertEqual(member.aw3ManifestJobRunID, gen_job_run.id)
