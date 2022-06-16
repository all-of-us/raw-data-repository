from datetime import datetime, timedelta

from rdr_service import clock, code_constants
from rdr_service.dao.genomics_dao import GenomicIncidentDao, GenomicQueriesDao, GenomicSetMemberDao
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult, GenomicIncidentCode, GenomicIncidentStatus
from rdr_service.model.genomics import GenomicIncident
from rdr_service.participant_enums import QuestionnaireStatus
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

        self.incident_dao.batch_update_incident_fields(incident_ids)

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

        self.incident_dao.batch_update_incident_fields(new_incident_obj.id)

        new_incident_obj = self.incident_dao.get(new_incident.id)

        self.assertTrue(new_incident_obj.email_notification_sent == 1)

    def test_batch_update_resolved_incidents(self):

        for _ in range(5):
            self.data_generator.create_database_genomic_incident(
                code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
                message="File has failed validation.",
                submitted_gc_site_id='bcm'
            )

        all_incidents = self.incident_dao.get_all()

        self.assertTrue(all(obj.status == GenomicIncidentStatus.OPEN.name for obj in all_incidents))

        self.incident_dao.batch_update_incident_fields([obj.id for obj in all_incidents], _type='resolved')

        all_incidents = self.incident_dao.get_all()

        self.assertTrue(all(obj.status == GenomicIncidentStatus.RESOLVED.name for obj in all_incidents))

    def test_get_resolved_manifests(self):
        file_name = 'test_file_name.csv'
        bucket_name = 'test_bucket'
        sub_folder = 'test_subfolder'

        from_date = clock.CLOCK.now() - timedelta(days=1)

        with clock.FakeClock(from_date):
            for _ in range(5):
                gen_job_run = self.data_generator.create_database_genomic_job_run(
                    jobId=GenomicJob.METRICS_INGESTION,
                    startTime=clock.CLOCK.now(),
                    runResult=GenomicSubProcessResult.SUCCESS
                )

                gen_processed_file = self.data_generator.create_database_genomic_file_processed(
                    runId=gen_job_run.id,
                    startTime=clock.CLOCK.now(),
                    filePath=f"{bucket_name}/{sub_folder}/{file_name}",
                    bucketName=bucket_name,
                    fileName=file_name,
                )

                self.data_generator.create_database_genomic_incident(
                    source_job_run_id=gen_job_run.id,
                    source_file_processed_id=gen_processed_file.id,
                    code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
                    message=f"{gen_job_run.jobId}: File name {file_name} has failed validation.",
                )

        self.incident_dao.batch_update_incident_fields(
            [obj.id for obj in self.incident_dao.get_all()],
            _type='resolved'
        )

        resolved_incidents = self.incident_dao.get_daily_report_resolved_manifests(from_date)

        self.assertEqual(len(resolved_incidents), len(self.incident_dao.get_all()))
        self.assertTrue(all(obj.status == GenomicIncidentStatus.RESOLVED.name for obj in resolved_incidents))

        # clear current set member records
        with self.incident_dao.session() as session:
            session.query(GenomicIncident).delete()

        with clock.FakeClock(from_date):
            for _ in range(5):
                self.data_generator.create_database_genomic_incident(
                    source_job_run_id=gen_job_run.id,
                    source_file_processed_id=gen_processed_file.id,
                    code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
                    message=f"{gen_job_run.jobId}: File name {file_name} has failed validation.",
                )

        self.incident_dao.batch_update_incident_fields(
            [obj.id for obj in self.incident_dao.get_all()],
            _type='resolved'
        )

        current_incidents = self.incident_dao.get_all()

        for incident in current_incidents:
            incident.created = from_date - timedelta(days=4)
            incident.modified = clock.CLOCK.now()
            self.incident_dao.update(incident)

        resolved_incidents = self.incident_dao.get_daily_report_resolved_manifests(from_date)

        self.assertEqual(len(resolved_incidents), len(self.incident_dao.get_all()))
        self.assertTrue(all(obj.status == GenomicIncidentStatus.RESOLVED.name for obj in resolved_incidents))

    def test_update_member_blocklist(self):

        non_aian_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=self.gen_set.id,
            biobankId="11111111",
            sampleId="222222222222",
            genomeType="aou_wgs",
        )

        self.member_dao.update_member_blocklists(non_aian_member)
        updated_non_ai_an = self.member_dao.get(non_aian_member.id)

        self.assertEqual(updated_non_ai_an.blockResearch, 0)
        self.assertIsNone(updated_non_ai_an.blockResearchReason)

        aian_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=self.gen_set.id,
            biobankId="11111111",
            sampleId="222222222222",
            genomeType="aou_wgs",
            ai_an="Y"
        )

        self.member_dao.update_member_blocklists(aian_member)
        updated_ai_an = self.member_dao.get(aian_member.id)

        self.assertEqual(updated_ai_an.blockResearch, 1)
        self.assertIsNotNone(updated_ai_an.blockResearchReason)
        self.assertEqual(updated_ai_an.blockResearchReason, 'aian')

        blocked_aian_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=self.gen_set.id,
            biobankId="11111111",
            sampleId="222222222222",
            genomeType="aou_wgs",
            ai_an="Y",
            blockResearch=1,
            blockResearchReason='sample_swap'
        )

        self.member_dao.update_member_blocklists(blocked_aian_member)
        updated_blocked_aian_member = self.member_dao.get(blocked_aian_member.id)

        # should not change if already blocked
        self.assertEqual(updated_blocked_aian_member.blockResearch, 1)
        self.assertIsNotNone(updated_blocked_aian_member.blockResearchReason)
        self.assertEqual(updated_blocked_aian_member.blockResearchReason, 'sample_swap')

    def test_genomic_set_member_job_id(self):
        self.assertFalse(GenomicSetMemberDao._is_valid_set_member_job_field(None))
        self.assertFalse(GenomicSetMemberDao._is_valid_set_member_job_field('notARealField'))
        self.assertTrue(GenomicSetMemberDao._is_valid_set_member_job_field('aw2fManifestJobRunID'))

    def test_w1il_yes_no_yes(self):
        # Set up GROR questionnaire data
        questionnaire = self.data_generator.create_database_questionnaire_history()
        module_code = self.data_generator.create_database_code(value=code_constants.CONSENT_FOR_GENOMICS_ROR_MODULE)
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            codeId=module_code.codeId
        )
        question_code = self.data_generator.create_database_code(value=code_constants.GROR_CONSENT_QUESTION_CODE)
        question = self.data_generator.create_database_questionnaire_question(
            codeId=question_code.codeId
        )
        yes_answer_code = self.data_generator.create_database_code(value=code_constants.CONSENT_GROR_YES_CODE)
        no_answer_code = self.data_generator.create_database_code(value=code_constants.CONSENT_GROR_NO_CODE)

        # Create a participant that should not appear in results because they didn't give a No response to GROR
        self._generate_participant_data(
            questionnaire=questionnaire,
            question=question,
            is_gror_consented=True,
            w1il_run_datetime=datetime(2022, 1, 7),
            gror_responses=[
                (datetime(2021, 12, 11), yes_answer_code.codeId),
                (datetime(2022, 1, 21), yes_answer_code.codeId)
            ]
        )

        # Create a participant that should appear because they have a No and then a Yes after the W1IL
        yes_no_yes_participant_id = self._generate_participant_data(
            questionnaire=questionnaire,
            question=question,
            is_gror_consented=True,
            w1il_run_datetime=datetime(2022, 1, 7),
            gror_responses=[
                (datetime(2021, 12, 11), yes_answer_code.codeId),
                (datetime(2022, 1, 13), no_answer_code.codeId),
                (datetime(2022, 1, 21), yes_answer_code.codeId)
            ]
        )

        # Create a participant that should not show up because they never switch back to providing GROR consent
        self._generate_participant_data(
            questionnaire=questionnaire,
            question=question,
            is_gror_consented=False,
            w1il_run_datetime=datetime(2022, 1, 7),
            gror_responses=[
                (datetime(2021, 12, 11), yes_answer_code.codeId),
                (datetime(2022, 1, 13), no_answer_code.codeId)
            ]
        )

        dao = GenomicQueriesDao()
        yes_no_yes_participant_list = dao.get_w1il_yes_no_yes_participants(start_datetime=datetime(2022, 1, 9))

        self.assertEqual(1, len(yes_no_yes_participant_list))
        self.assertEqual(yes_no_yes_participant_id, yes_no_yes_participant_list[0].participantId)

    def _generate_participant_data(self, questionnaire, question, gror_responses, w1il_run_datetime, is_gror_consented):
        job_run = self.data_generator.create_database_genomic_job_run(
            startTime=w1il_run_datetime
        )

        participant_summary = self.data_generator.create_database_participant_summary(
            consentForGenomicsROR=(
                QuestionnaireStatus.SUBMITTED if is_gror_consented else QuestionnaireStatus.UNSET
            )
        )
        self.data_generator.create_database_genomic_set_member(
            cvlW1ilHdrJobRunId=job_run.id,
            genomicSetId=self.gen_set.id,
            participantId=participant_summary.participantId
        )

        for authored_datetime, answer_code_id in gror_responses:
            response = self.data_generator.create_database_questionnaire_response(
                participantId=participant_summary.participantId,
                authored=authored_datetime,
                questionnaireId=questionnaire.questionnaireId,
                questionnaireVersion=questionnaire.version
            )
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=response.questionnaireResponseId,
                valueCodeId=answer_code_id,
                questionId=question.questionnaireQuestionId
            )

        return participant_summary.participantId
