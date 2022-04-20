import datetime
import mock
import random

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.clock import FakeClock
from rdr_service.dao.genomics_dao import GenomicGcDataFileDao, GenomicGCValidationMetricsDao, GenomicIncidentDao, \
    GenomicInformingLoopDao, GenomicResultViewedDao, GenomicSetMemberDao, UserEventMetricsDao, GenomicJobRunDao
from rdr_service.dao.message_broker_dao import MessageBrokenEventDataDao
from rdr_service.genomic_enums import GenomicIncidentCode, GenomicJob, GenomicWorkflowState, GenomicSubProcessResult, \
    GenomicSubProcessStatus, GenomicManifestTypes, GenomicQcStatus
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.model.genomics import GenomicGcDataFile, GenomicIncident, GenomicSetMember, GenomicGCValidationMetrics
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicJobControllerTest(BaseTestCase):
    def setUp(self):
        super(GenomicJobControllerTest, self).setUp()
        self.data_file_dao = GenomicGcDataFileDao()
        self.event_data_dao = MessageBrokenEventDataDao()
        self.incident_dao = GenomicIncidentDao()
        self.informing_loop_dao = GenomicInformingLoopDao()
        self.result_viewed_dao = GenomicResultViewedDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.user_event_metrics_dao = UserEventMetricsDao()
        self.job_run_dao = GenomicJobRunDao()

    def test_incident_with_long_message(self):
        """Make sure the length of incident messages doesn't cause issues when recording them"""
        incident_message = "1" * (GenomicIncident.message.type.length + 20)
        mock_slack_handler = mock.MagicMock()

        job_controller = GenomicJobController(job_id=1)
        job_controller.genomic_alert_slack = mock_slack_handler
        job_controller.create_incident(message=incident_message, slack=True)

        # Double check that the incident was saved successfully, with part of the message
        incident: GenomicIncident = self.session.query(GenomicIncident).one()
        self.assertTrue(incident_message.startswith(incident.message))

        # Make sure Slack received the full message
        mock_slack_handler.send_message_to_webhook.assert_called_with(
            message_data={
                'text': incident_message
            }
        )

    def test_gvcf_files_ingestion(self):
        job_controller = GenomicJobController(job_id=38)
        bucket_name = "test_bucket"

        file_path = "Wgs_sample_raw_data/SS_VCF_research/BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz"
        file_path_md5 = "Wgs_sample_raw_data/SS_VCF_research/" \
                        "BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz.md5sum"

        full_path = f'{bucket_name}/{file_path}'
        full_path_md5 = f'{bucket_name}/{file_path_md5}'

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath='/test_file_path',
            bucketName='test_bucket',
            fileName='test_file_name',
        )

        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=gen_processed_file.id
        )

        job_controller.ingest_data_files_into_gc_metrics(file_path_md5, bucket_name)

        metrics = self.metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfMd5Received)
        self.assertIsNotNone(metrics.gvcfMd5Path)
        self.assertEqual(metrics.gvcfMd5Path, full_path_md5)
        self.assertEqual(metrics.gvcfMd5Received, 1)

        job_controller.ingest_data_files_into_gc_metrics(file_path, bucket_name)

        metrics = self.metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfReceived)
        self.assertIsNotNone(metrics.gvcfPath)
        self.assertEqual(metrics.gvcfPath, full_path)
        self.assertEqual(metrics.gvcfReceived, 1)

    def test_gvcf_files_ingestion_create_incident(self):
        bucket_name = "test_bucket"
        file_path = "Wgs_sample_raw_data/SS_VCF_research/BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz"

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="111111111",
            sampleId="222222222222",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath='/test_file_path',
            bucketName=bucket_name,
            fileName='test_file_name',
        )

        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=gen_processed_file.id
        )

        with GenomicJobController(GenomicJob.INGEST_DATA_FILES) as controller:
            controller.ingest_data_files_into_gc_metrics(file_path, bucket_name)

        incident = self.incident_dao.get(1)
        self.assertIsNotNone(incident)
        self.assertEqual(incident.code, GenomicIncidentCode.UNABLE_TO_FIND_METRIC.name)
        self.assertEqual(incident.data_file_path, file_path)
        self.assertEqual(incident.message, 'INGEST_DATA_FILES: Cannot find '
                                           'genomics metric record for sample id: '
                                           '21042005280')

    def test_accession_data_files(self):
        test_bucket_baylor = "fake-data-bucket-baylor"
        test_idat_file = "fake-data-bucket-baylor/Genotyping_sample_raw_data/204027270091_R02C01_Grn.idat"
        test_vcf_file = "fake-data-bucket-baylor/Genotyping_sample_raw_data/204027270091_R02C01.vcf.gz"

        test_cram_file = "fake-data-bucket-baylor/Wgs_sample_raw_data/" \
                         "CRAMs_CRAIs/BCM_A100134256_21063006771_SIA0017196_1.cram"

        test_files = [test_idat_file, test_vcf_file, test_cram_file]

        test_time = datetime.datetime(2021, 7, 9, 14, 1, 1)

        # run job controller method on each file
        with clock.FakeClock(test_time):

            for file_path in test_files:
                with GenomicJobController(GenomicJob.ACCESSION_DATA_FILES) as controller:
                    controller.accession_data_files(file_path, test_bucket_baylor)

        inserted_files = self.data_file_dao.get_all()

        # idat
        expected_idat = GenomicGcDataFile(
            id=1,
            created=test_time,
            modified=test_time,
            file_path=test_idat_file,
            gc_site_id='jh',
            bucket_name='fake-data-bucket-baylor',
            file_prefix='Genotyping_sample_raw_data',
            file_name='204027270091_R02C01_Grn.idat',
            file_type='Grn.idat',
            identifier_type='chipwellbarcode',
            identifier_value='204027270091_R02C01',
            ignore_flag=0,
        )

        # vcf
        expected_vcf = GenomicGcDataFile(
            id=2,
            created=test_time,
            modified=test_time,
            file_path=test_vcf_file,
            gc_site_id='jh',
            bucket_name='fake-data-bucket-baylor',
            file_prefix='Genotyping_sample_raw_data',
            file_name='204027270091_R02C01.vcf.gz',
            file_type='vcf.gz',
            identifier_type='chipwellbarcode',
            identifier_value='204027270091_R02C01',
            ignore_flag=0,
        )

        # cram
        expected_cram = GenomicGcDataFile(
            id=3,
            created=test_time,
            modified=test_time,
            file_path=test_cram_file,
            gc_site_id='bcm',
            bucket_name='fake-data-bucket-baylor',
            file_prefix='Wgs_sample_raw_data/CRAMs_CRAIs',
            file_name='BCM_A100134256_21063006771_SIA0017196_1.cram',
            file_type='cram',
            identifier_type='sample_id',
            identifier_value='21063006771',
            ignore_flag=0,
        )

        # obj mapping
        expected_objs = {
            0: expected_idat,
            1: expected_vcf,
            2: expected_cram
        }

        # verify test objects match expectations
        for i in range(3):
            self.assertEqual(expected_objs[i].bucket_name, inserted_files[i].bucket_name)
            self.assertEqual(expected_objs[i].created, inserted_files[i].created)
            self.assertEqual(expected_objs[i].file_name, inserted_files[i].file_name)
            self.assertEqual(expected_objs[i].file_path, inserted_files[i].file_path)
            self.assertEqual(expected_objs[i].file_prefix, inserted_files[i].file_prefix)
            self.assertEqual(expected_objs[i].file_type, inserted_files[i].file_type)
            self.assertEqual(expected_objs[i].gc_site_id, inserted_files[i].gc_site_id)
            self.assertEqual(expected_objs[i].id, inserted_files[i].id)
            self.assertEqual(expected_objs[i].identifier_type, inserted_files[i].identifier_type)
            self.assertEqual(expected_objs[i].identifier_value, inserted_files[i].identifier_value)
            self.assertEqual(expected_objs[i].ignore_flag, inserted_files[i].ignore_flag)
            self.assertEqual(expected_objs[i].metadata, inserted_files[i].metadata)
            self.assertEqual(expected_objs[i].modified, inserted_files[i].modified)

    def test_informing_loop_ingestion_message_broker(self):
        loop_decision = 'informing_loop_decision'
        loop_started = 'informing_loop_started'
        # https://docs.google.com/document/d/1E1tNSi1mWwhBSCs9Syprbzl5E0SH3c_9oLduG1mzlcY/edit#heading=h.2m73apfm9irj
        loop_module_types = ['gem', 'hdr', 'pgx']

        participant = self.data_generator.create_database_participant()

        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_decision,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': random.choice(loop_module_types), 'decision_value': 'yes'},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record.id,
                event_type=loop_decision
            )

        decision_genomic_record = self.informing_loop_dao.get(1)

        self.assertIsNotNone(decision_genomic_record)
        self.assertIsNotNone(decision_genomic_record.event_type)
        self.assertIsNotNone(decision_genomic_record.module_type)
        self.assertIsNotNone(decision_genomic_record.decision_value)

        self.assertEqual(decision_genomic_record.message_record_id, message_broker_record.id)
        self.assertEqual(decision_genomic_record.participant_id, message_broker_record.participantId)
        self.assertEqual(decision_genomic_record.event_type, loop_decision)
        self.assertTrue(decision_genomic_record.module_type in loop_module_types)
        self.assertEqual(decision_genomic_record.decision_value, 'yes')

        message_broker_record_two = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_started,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': 'gem'},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_two.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_two.participantId,
                messageRecordId=message_broker_record_two.id,
                eventType=message_broker_record_two.eventType,
                eventAuthoredTime=message_broker_record_two.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_two.id,
                event_type=loop_started
            )

        started_genomic_record = self.informing_loop_dao.get(2)

        self.assertIsNotNone(started_genomic_record)
        self.assertIsNotNone(started_genomic_record.event_type)
        self.assertIsNotNone(started_genomic_record.module_type)
        self.assertIsNone(started_genomic_record.decision_value)

        self.assertEqual(started_genomic_record.message_record_id, message_broker_record_two.id)
        self.assertEqual(started_genomic_record.participant_id, message_broker_record_two.participantId)
        self.assertEqual(started_genomic_record.event_type, loop_started)
        self.assertEqual(started_genomic_record.module_type, 'gem')

    def test_result_viewed_ingestion_message_broker(self):
        event_type = 'result_viewed'
        participant = self.data_generator.create_database_participant()
        # https://docs.google.com/document/d/1E1tNSi1mWwhBSCs9Syprbzl5E0SH3c_9oLduG1mzlcY/edit#heading=h.dtikttz25h22
        result_module_types = ['gem', 'hdr_v1', 'pgx_v1']

        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'result_type': random.choice(result_module_types)},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_VIEWED) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record.id,
                event_type=event_type
            )

        result_viewed_genomic_record = self.result_viewed_dao.get_all()

        self.assertIsNotNone(result_viewed_genomic_record)
        self.assertEqual(len(result_viewed_genomic_record), 1)

        result_viewed_genomic_record = result_viewed_genomic_record[0]

        self.assertIsNotNone(result_viewed_genomic_record.event_type)
        self.assertIsNotNone(result_viewed_genomic_record.module_type)

        self.assertEqual(result_viewed_genomic_record.message_record_id, message_broker_record.id)
        self.assertEqual(result_viewed_genomic_record.participant_id, message_broker_record.participantId)
        self.assertEqual(result_viewed_genomic_record.event_type, event_type)
        self.assertTrue(result_viewed_genomic_record.module_type in result_module_types)

        self.assertEqual(result_viewed_genomic_record.first_viewed, message_broker_record.eventAuthoredTime)
        self.assertEqual(result_viewed_genomic_record.last_viewed,  message_broker_record.eventAuthoredTime)

        message_broker_record_two = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now() + datetime.timedelta(days=1),
            messageOrigin='example@example.com',
            requestBody={'result_type': result_viewed_genomic_record.module_type},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_two.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_two.participantId,
                messageRecordId=message_broker_record_two.id,
                eventType=message_broker_record_two.eventType,
                eventAuthoredTime=message_broker_record_two.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_VIEWED) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_two.id,
                event_type=event_type
            )

        result_viewed_genomic_record = self.result_viewed_dao.get_all()

        self.assertIsNotNone(result_viewed_genomic_record)
        self.assertEqual(len(result_viewed_genomic_record), 1)

        result_viewed_genomic_record = result_viewed_genomic_record[0]

        self.assertEqual(result_viewed_genomic_record.first_viewed, message_broker_record.eventAuthoredTime)

        # check updated record has the last viewed time
        self.assertEqual(result_viewed_genomic_record.last_viewed, message_broker_record_two.eventAuthoredTime)
        self.assertEqual(result_viewed_genomic_record.message_record_id, message_broker_record.id)

    def test_updating_members_blocklists(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        # for just created and wf state query and MATCHES criteria
        for i in range(4):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType='test_investigation_one' if i & 2 != 0 else 'aou_wgs',
                genomicWorkflowState=GenomicWorkflowState.AW0,
                ai_an='Y' if i & 2 == 0 else 'N'
            )

        # for just created and wf state query and DOES NOT MATCH criteria
        for i in range(2):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType='aou_array',
                genomicWorkflowState=GenomicWorkflowState.AW0,
                ai_an='N'
            )

        with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
            controller.update_members_blocklists()

        # current config json in base_config.json
        created_members = self.member_dao.get_all()

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'aian'
            for obj in created_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW0)
        )

        # should NOT be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 0 and obj.blockResultsReason is None
            for obj in created_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW0)
        )

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'test_sample_swap'
            for obj in created_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        # should be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 1 and obj.blockResultsReason is not None and obj.blockResultsReason == 'test_sample_swap'
            for obj in created_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        # should NOT be RESEARCH/RESULTS blocked
        self.assertTrue(all(
            obj.blockResearch == 0 and obj.blockResearchReason is None
            for obj in created_members if obj.genomeType == 'aou_array' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        self.assertTrue(all(
            obj.blockResults == 0 and obj.blockResultsReason is None
            for obj in created_members if obj.genomeType == 'aou_array' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        # clear current set member records
        with self.member_dao.session() as session:
            session.query(GenomicSetMember).delete()

        run_result = self.job_run_dao.get(1)

        self.assertEqual(run_result.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(run_result.runResult, GenomicSubProcessResult.SUCCESS)

        # for modified data query and MATCHES criteria
        for i in range(4):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType='test_investigation_one' if i & 2 != 0 else 'aou_wgs',
                genomicWorkflowState=GenomicWorkflowState.AW1,
                ai_an='Y' if i & 2 == 0 else 'N'
            )

        with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
            controller.update_members_blocklists()

        modified_members = self.member_dao.get_all()

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'aian'
            for obj in modified_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW1)
        )

        # should NOT be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 0 and obj.blockResultsReason is None
            for obj in modified_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW1)
        )

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'test_sample_swap'
            for obj in modified_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW1)
        )

        # should be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 1 and obj.blockResultsReason is not None and obj.blockResultsReason == 'test_sample_swap'
            for obj in modified_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW1)
        )

        run_result = self.job_run_dao.get(2)

        self.assertEqual(run_result.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(run_result.runResult, GenomicSubProcessResult.SUCCESS)

    def test_ingest_user_metrics_file(self):
        test_file = 'Genomic-Metrics-File-User-Events-Test.csv'
        bucket_name = 'test_bucket'
        sub_folder = 'user_events'
        pids = []

        file_ingester = GenomicFileIngester()

        for _ in range(2):
            pid = self.data_generator.create_database_participant()
            pids.append(pid.participantId)

        test_metrics_file = create_ingestion_test_file(
            test_file,
            bucket_name,
            sub_folder)

        test_file_path = f'{bucket_name}/{sub_folder}/{test_metrics_file}'

        with open_cloud_file(test_file_path) as csv_file:
            metrics_to_ingest = file_ingester._read_data_to_ingest(csv_file)

        with GenomicJobController(GenomicJob.METRICS_FILE_INGEST) as controller:
            controller.ingest_metrics_file(
                metric_type='user_events',
                file_path=test_file_path,
            )

        job_run_id = controller.job_run.id
        metrics = self.user_event_metrics_dao.get_all()

        for pid in pids:
            file_metrics = list(filter(lambda x: int(x['participant_id'].split('P')[-1]) == pid, metrics_to_ingest[
                'rows']))
            participant_ingested_metrics = list(filter(lambda x: x.participant_id == pid, metrics))

            self.assertEqual(len(file_metrics), len(participant_ingested_metrics))
            self.assertTrue(all(obj.run_id == job_run_id for obj in participant_ingested_metrics))

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_reconcile_pdr_data(self, mock_cloud_task):

        # init new job run in __enter__
        with GenomicJobController(GenomicJob.RECONCILE_PDR_DATA) as controller:
            controller.reconcile_pdr_data()

        cloud_task_endpoint = 'rebuild_genomic_table_records_task'

        first_run = self.job_run_dao.get_all()

        self.assertEqual(mock_cloud_task.call_count, 1)
        call_args = mock_cloud_task.call_args_list

        self.assertEqual(len(call_args), 1)
        self.assertEqual(call_args[0].args[0]['table'], self.job_run_dao.model_type.__tablename__)

        self.assertTrue(type(call_args[0].args[0]['ids']) is list)
        self.assertEqual(call_args[0].args[0]['ids'], [obj.id for obj in first_run])
        self.assertEqual(call_args[0].args[1], cloud_task_endpoint)

        participant = self.data_generator.create_database_participant()

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        plus_ten = clock.CLOCK.now() + datetime.timedelta(minutes=10)
        plus_ten = plus_ten.replace(microsecond=0)
        with FakeClock(plus_ten):
            for i in range(2):
                gen_member = self.data_generator.create_database_genomic_set_member(
                    genomicSetId=gen_set.id,
                    biobankId="100153482",
                    sampleId="21042005280",
                    genomeType="aou_wgs",
                    genomicWorkflowState=GenomicWorkflowState.AW1
                )

                gen_processed_file = self.data_generator.create_database_genomic_file_processed(
                    runId=first_run[0].id,
                    startTime=clock.CLOCK.now(),
                    filePath=f'test_file_path_{i}',
                    bucketName='test_bucket',
                    fileName='test_file_name',
                )

                self.data_generator.create_database_genomic_gc_validation_metrics(
                    genomicSetMemberId=gen_member.id,
                    genomicFileProcessedId=gen_processed_file.id
                )

                manifest = self.data_generator.create_database_genomic_manifest_file(
                    manifestTypeId=2,
                    filePath=f'test_file_path_{i}'
                )

                self.data_generator.create_database_genomic_manifest_feedback(
                    inputManifestFileId=manifest.id,
                    feedbackRecordCount=2
                )

                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=participant.participantId,
                    event_name='test_event',
                    run_id=1,
                )

        # gets new records that were created with last job run from above
        with GenomicJobController(GenomicJob.RECONCILE_PDR_DATA) as controller:
            controller.reconcile_pdr_data()

        affected_tables = [
            'genomic_set',
            'genomic_set_member',
            'genomic_job_run',
            'genomic_file_processed',
            'genomic_gc_validation_metrics',
            'genomic_manifest_file',
            'genomic_manifest_feedback',
        ]

        self.assertEqual(mock_cloud_task.call_count, 8)
        call_args = mock_cloud_task.call_args_list
        self.assertEqual(len(call_args), 8)

        mock_tables = set([obj[0][0]['table'] for obj in call_args])
        mock_endpoint = [obj[0][1] for obj in call_args]

        self.assertTrue([mock_tables].sort() == affected_tables.sort())
        self.assertTrue(all(obj for obj in mock_endpoint if obj == cloud_task_endpoint))

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_retry_manifest_ingestions_if_deltas(self, mock_cloud_task):

        bucket_name = "test-bucket"
        aw1_file_name = "AW1_wgs_sample_manifests/RDR_AoU_SEQ_PKG-2104-026571.csv"
        aw1_manifest_path = f"{bucket_name}/{aw1_file_name}"

        aw2_file_name = "AW2_wgs_data_manifests/RDR_AoU_SEQ_DataManifest_04092021.csv"
        aw2_manifest_path = f"{bucket_name}/{aw2_file_name}"

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        # Create AW1 job_run
        aw1_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        # Create AW2 job_run
        aw2_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        # should have no data
        with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
            controller.retry_manifest_ingestions()

        job_run = self.job_run_dao.get(3)
        self.assertEqual(job_run.jobId, GenomicJob.RETRY_MANIFEST_INGESTIONS)
        self.assertEqual(job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(job_run.runResult, GenomicSubProcessResult.NO_FILES)

        self.assertEqual(mock_cloud_task.call_count, 0)
        self.assertFalse(mock_cloud_task.call_count)

        # Create genomic_aw1_raw record
        self.data_generator.create_database_genomic_aw1_raw(
            file_path=aw1_manifest_path,
            package_id="PKG-2104-026571",
            biobank_id="A10001",
        )

        # Create genomic_aw2_raw record
        self.data_generator.create_database_genomic_aw2_raw(
            file_path=aw2_manifest_path,
            biobank_id="A10001",
            sample_id="100001",
            biobankidsampleid="A10001_100001",
        )

        # Create AW1 genomic_manifest_file record
        aw1_manifest_file = self.data_generator.create_database_genomic_manifest_file(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            uploadDate=clock.CLOCK.now(),
            manifestTypeId=GenomicManifestTypes.AW1,
            filePath=aw1_manifest_path,
            fileName=aw1_file_name,
            bucketName=bucket_name,
            recordCount=1,
            rdrProcessingComplete=1,
            rdrProcessingCompleteDate=clock.CLOCK.now(),
        )

        # Create AW2 genomic_manifest_file record
        aw2_manifest_file = self.data_generator.create_database_genomic_manifest_file(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            uploadDate=clock.CLOCK.now(),
            manifestTypeId=GenomicManifestTypes.AW2,
            filePath=aw2_manifest_path,
            fileName=aw2_file_name,
            bucketName=bucket_name,
            recordCount=1,
            rdrProcessingComplete=1,
            rdrProcessingCompleteDate=clock.CLOCK.now(),
        )

        # Create AW1 file_processed
        aw1_file_processed = self.data_generator.create_database_genomic_file_processed(
            runId=aw1_job_run.id,
            startTime=clock.CLOCK.now(),
            genomicManifestFileId=aw1_manifest_file.id,
            filePath=f"/{aw1_manifest_path}",
            bucketName=bucket_name,
            fileName=aw1_file_name,
        )

        # Create AW2 file_processed
        aw2_file_processed = self.data_generator.create_database_genomic_file_processed(
            runId=aw2_job_run.id,
            startTime=clock.CLOCK.now(),
            genomicManifestFileId=aw2_manifest_file.id,
            filePath=f"/{aw2_manifest_path}",
            bucketName=bucket_name,
            fileName=aw2_file_name,
        )

        # genomic_set_member for AW1
        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW1,
            aw1FileProcessedId=aw1_file_processed.id
        )

        # genomic_gc_validation_metrics for AW1
        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=aw2_file_processed.id
        )

        # one AW1/AW2 with no deltas
        with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
            controller.retry_manifest_ingestions()

        job_run = self.job_run_dao.get(4)
        self.assertEqual(job_run.jobId, GenomicJob.RETRY_MANIFEST_INGESTIONS)
        self.assertEqual(job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(job_run.runResult, GenomicSubProcessResult.NO_FILES)

        self.assertEqual(mock_cloud_task.call_count, 0)
        self.assertFalse(mock_cloud_task.call_count)

        # empty tables resulting in deltas and cloud task calls
        with self.member_dao.session() as session:
            session.query(GenomicGCValidationMetrics).delete()
            session.query(GenomicSetMember).delete()

        with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
            controller.retry_manifest_ingestions()

        job_run = self.job_run_dao.get(5)
        self.assertEqual(job_run.jobId, GenomicJob.RETRY_MANIFEST_INGESTIONS)
        self.assertEqual(job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(job_run.runResult, GenomicSubProcessResult.SUCCESS)

        # one AW1/AW2 with deltas
        self.assertEqual(mock_cloud_task.call_count, 2)
        self.assertTrue(mock_cloud_task.call_count)

        call_args = mock_cloud_task.call_args_list
        self.assertEqual(len(call_args), 2)

        cloud_task_endpoint = ['ingest_aw1_manifest_task', 'ingest_aw2_manifest_task']
        mock_endpoint = [obj[0][1] for obj in call_args]
        self.assertTrue(all(obj for obj in mock_endpoint if obj == cloud_task_endpoint))

        mock_buckets = set([obj[0][0]['bucket_name'] for obj in call_args])
        self.assertTrue(len(mock_buckets), 1)
        self.assertTrue(list(mock_buckets)[0] == bucket_name)

    def test_calculate_informing_loop_ready_flags(self):
        num_participants = 4
        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(num_participants):
            plus_num = clock.CLOCK.now() + datetime.timedelta(minutes=num)
            plus_num = plus_num.replace(microsecond=0)
            with FakeClock(plus_num):
                summary = self.data_generator.create_database_participant_summary(
                    consentForStudyEnrollment=1,
                    consentForGenomicsROR=1
                )
                stored_sample = self.data_generator.create_database_biobank_stored_sample(
                    biobankId=summary.biobankId,
                    biobankOrderIdentifier=self.fake.pyint()
                )
                collection_site = self.data_generator.create_database_site(
                    siteType='Clinic'
                )
                order = self.data_generator.create_database_biobank_order(
                    collectedSiteId=collection_site.siteId,
                    participantId=summary.participantId,
                    finalizedTime=plus_num
                )
                self.data_generator.create_database_biobank_order_identifier(
                    value=stored_sample.biobankOrderIdentifier,
                    biobankOrderId=order.biobankOrderId
                )
                member = self.data_generator.create_database_genomic_set_member(
                    genomicSetId=gen_set.id,
                    participantId=summary.participantId,
                    genomeType=config.GENOME_TYPE_WGS,
                    qcStatus=GenomicQcStatus.PASS,
                    gcManifestSampleSource='Whole Blood',
                    collectionTubeId=stored_sample.biobankStoredSampleId
                )
                self.data_generator.create_database_genomic_gc_validation_metrics(
                    genomicSetMemberId=member.id,
                    sexConcordance='True',
                    drcFpConcordance='Pass',
                    drcSexConcordance='Pass',
                    processingStatus='Pass'
                )


        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), num_participants)

        current_set_members = self.member_dao.get_all()
        self.assertTrue(all(obj.informingLoopReadyFlag == 0 for obj in current_set_members))
        self.assertTrue(all(obj.informingLoopReadyFlagModified is None for obj in current_set_members))

        with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
            controller.calculate_informing_loop_ready_flags()

        # no config object, controller method should return
        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), num_participants)

        calculation_limit = 2
        config.override_setting(config.CALCULATE_READY_FLAG_LIMIT, [calculation_limit])

        with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
            controller.calculate_informing_loop_ready_flags()

        current_set_members = self.member_dao.get_all()
        self.assertTrue(any(obj.informingLoopReadyFlag == 1 for obj in current_set_members))
        self.assertTrue(any(obj.informingLoopReadyFlagModified is not None for obj in current_set_members))

        current_loops_set = [obj for obj in current_set_members if obj.informingLoopReadyFlag == 1
                             and obj.informingLoopReadyFlagModified is not None]
        self.assertEqual(len(current_loops_set), calculation_limit)

        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), num_participants // 2)

        with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
            controller.calculate_informing_loop_ready_flags()

        current_set_members = self.member_dao.get_all()
        self.assertTrue(all(obj.informingLoopReadyFlag == 1 for obj in current_set_members))
        self.assertTrue(all(obj.informingLoopReadyFlagModified is not None for obj in current_set_members))

        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), 0)

