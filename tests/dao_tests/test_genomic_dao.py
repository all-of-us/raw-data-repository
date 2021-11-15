from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicIncidentDao, GenomicSetMemberDao
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult, GenomicIncidentCode
from tests.helpers.unittest_base import BaseTestCase


class GenomicDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.member_dao = GenomicSetMemberDao()
        self.incident_dao = GenomicIncidentDao()

        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        self.gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

    def test_update_member_job_run_id(self):
        bad_field_passed = self.member_dao.update_member_job_run_id([], 1, 'badFieldName')
        self.assertEqual(bad_field_passed, GenomicSubProcessResult.ERROR)

        for num in range(6):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                biobankId="11111111",
                sampleId="222222222222",
                genomeType="aou_wgs",
            )

        members = self.member_dao.get_all()
        member_ids = [m.id for m in members]

        mid = len(member_ids) // 2
        first = member_ids[:mid]
        second = member_ids[mid:]

        first_set = self.member_dao.update_member_job_run_id(first, self.gen_job_run.id, 'gemA1ManifestJobRunId')

        self.assertEqual(first_set, GenomicSubProcessResult.SUCCESS)

        for num in first:
            member = self.member_dao.get(num)
            self.assertEqual(member.gemA1ManifestJobRunId, self.gen_job_run.id)

        second_set = self.member_dao.update_member_job_run_id(second, self.gen_job_run.id, 'aw3ManifestJobRunID')

        self.assertEqual(second_set, GenomicSubProcessResult.SUCCESS)

        for num in second:
            member = self.member_dao.get(num)
            self.assertEqual(member.aw3ManifestJobRunID, self.gen_job_run.id)

    def test_batch_member_field_update(self):

        for _ in range(10):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                biobankId="11111111",
                sampleId="222222222222",
                genomeType="aou_wgs",
            )

        bad_job_run = self.member_dao.batch_update_member_field(
            [],
            'unknownField',
            1,
            is_job_run=True
        )

        self.assertEqual(bad_job_run, GenomicSubProcessResult.ERROR)

        members = self.member_dao.get_all()
        member_ids = [m.id for m in members]

        mid = len(member_ids) // 2
        first = member_ids[:mid]
        second = member_ids[mid:]

        first_set_no_fk = self.member_dao.batch_update_member_field(
            first,
            'aw3ManifestFileId',
            2
        )

        self.assertEqual(first_set_no_fk, GenomicSubProcessResult.ERROR)

        test_file_path = "rdr_fake_genomic_center_a_bucket/AW3_array_manifest_00-00-09-45-00.csv"
        manifest_file = self.data_generator.create_database_genomic_manifest_file(
            manifestTypeId=2,
            filePath=test_file_path
        )

        first_set_with_fk = self.member_dao.batch_update_member_field(
            first,
            'aw3ManifestFileId',
            manifest_file.id
        )

        self.assertEqual(first_set_with_fk, GenomicSubProcessResult.SUCCESS)

        manifest_file_members = [obj for obj in members if obj.id in first]
        self.assertTrue(all(obj for obj in manifest_file_members if obj.aw3ManifestFileId == manifest_file.id))

        second_set = self.member_dao.batch_update_member_field(
            second,
            'sexAtBirth',
            'F'
        )

        self.assertEqual(second_set, GenomicSubProcessResult.SUCCESS)

        members = self.member_dao.get_all()

        second_set_members = [obj for obj in members if obj.id in second]
        self.assertTrue(all(obj for obj in second_set_members if obj.sexAtBirth == 'F'))

        job_run_set = self.member_dao.batch_update_member_field(
            first,
            'gemA1ManifestJobRunId',
            self.gen_job_run.id,
            is_job_run=True
        )

        self.assertEqual(job_run_set, GenomicSubProcessResult.SUCCESS)

        job_run_members = [obj for obj in members if obj.id in first]
        self.assertTrue(all(obj for obj in job_run_members if obj.gemA1ManifestJobRunId == self.gen_job_run.id))

    def test_batch_update_email_notifications_sent(self):
        incident_ids = []

        for _ in range(5):
            current_incident = self.data_generator.create_database_genomic_incident(
                code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
                message="File has failed validation.",
                submitted_gc_site_id='bcm'
            )
            incident_ids.append(current_incident.id)

        all_incidents = self.incident_dao.get_all()

        self.assertTrue(all(obj.email_notification_sent_date is None for obj in all_incidents))
        self.assertTrue(all(obj.email_notification_sent == 0 for obj in all_incidents))

        self.incident_dao.batch_update_validation_emails_sent(incident_ids)

        all_incidents = self.incident_dao.get_all()

        self.assertTrue(all(obj.email_notification_sent_date is not None for obj in all_incidents))
        self.assertTrue(all(obj.email_notification_sent == 1 for obj in all_incidents))

        new_incident = self.data_generator.create_database_genomic_incident(
            code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
            message="File has failed validation.",
            submitted_gc_site_id='bcm'
        )

        new_incident_obj = self.incident_dao.get(new_incident.id)

        self.assertTrue(new_incident_obj.email_notification_sent == 0)
        self.assertTrue(new_incident_obj.email_notification_sent_date is None)

        self.incident_dao.batch_update_validation_emails_sent(new_incident_obj.id)

        new_incident_obj = self.incident_dao.get(new_incident.id)

        self.assertTrue(new_incident_obj.email_notification_sent == 1)
        self.assertTrue(new_incident_obj.email_notification_sent_date is not None)
