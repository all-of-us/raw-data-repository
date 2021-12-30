from typing import List

from rdr_service.code_constants import PRIMARY_CONSENT_UPDATE_QUESTION_CODE
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.participant_enums import QuestionnaireStatus
from tests.helpers.unittest_base import BaseTestCase


class ConsentFileDaoTest(BaseTestCase):
    def setUp(self, *args, **kwargs) -> None:
        super(ConsentFileDaoTest, self).setUp(*args, **kwargs)
        self.consent_dao = ConsentDao()

        # Initialize questionnaire for primary update
        self.primary_update_questionnaire = self.data_generator.create_database_questionnaire_history()
        primary_update_question_code = self.data_generator.create_database_code(
            value=PRIMARY_CONSENT_UPDATE_QUESTION_CODE
        )
        self.primary_update_question = self.data_generator.create_database_questionnaire_question(
            codeId=primary_update_question_code.codeId,
            questionnaireId=self.primary_update_questionnaire.questionnaireId,
            questionnaireVersion=self.primary_update_questionnaire.version
        )

    def test_loading_summaries_with_consent(self):
        """Check that participant summaries with any consents in the given time range are loaded"""
        all_consent_no_files_participant = self._init_summary_with_consent_data(
            submitted_consent_types=[ConsentType.CABOR, ConsentType.EHR, ConsentType.GROR, ConsentType.PRIMARY_UPDATE],
            validated_consent_types=[]
        )
        all_consent_missing_ehr_participant = self._init_summary_with_consent_data(
            submitted_consent_types=[ConsentType.CABOR, ConsentType.EHR, ConsentType.GROR, ConsentType.PRIMARY_UPDATE],
            validated_consent_types=[ConsentType.PRIMARY, ConsentType.CABOR, ConsentType.GROR, ConsentType.PRIMARY_UPDATE]
        )
        only_needs_ehr_validated = self._init_summary_with_consent_data(
            submitted_consent_types=[ConsentType.EHR],
            validated_consent_types=[ConsentType.PRIMARY]
        )
        only_needs_update_validated = self._init_summary_with_consent_data(
            submitted_consent_types=[ConsentType.PRIMARY_UPDATE],
            validated_consent_types=[ConsentType.PRIMARY]
        )
        # Submit some more that shouldn't show up in the results
        self._init_summary_with_consent_data(
            submitted_consent_types=[ConsentType.CABOR],
            validated_consent_types=[ConsentType.PRIMARY, ConsentType.CABOR]
        )
        self._init_summary_with_consent_data(
            submitted_consent_types=[],
            validated_consent_types=[ConsentType.PRIMARY]
        )

        with self.consent_dao.session() as session:
            result_list = self.consent_dao.get_participants_with_unvalidated_files(session)
            self.assertListsMatch(
                expected_list=[
                    all_consent_no_files_participant,
                    all_consent_missing_ehr_participant,
                    only_needs_ehr_validated,
                    only_needs_update_validated
                ],
                actual_list=result_list,
                id_attribute='participantId'
            )

    def test_ignoring_participants(self):
        """Make sure test and ghost accounts are left out of consent validation"""
        self._init_summary_with_consent_data(
            submitted_consent_types=[],
            validated_consent_types=[ConsentType.PRIMARY],
            extra_summary_args={
                'participantId': self.data_generator.create_database_participant(isGhostId=True).participantId
            }
        )
        self._init_summary_with_consent_data(
            submitted_consent_types=[],
            validated_consent_types=[ConsentType.PRIMARY],
            extra_summary_args={
                'participantId': self.data_generator.create_database_participant(isTestParticipant=True).participantId
            }
        )
        self._init_summary_with_consent_data(
            submitted_consent_types=[],
            validated_consent_types=[ConsentType.PRIMARY],
            extra_summary_args={
                'email': 'one@example.com'
            }
        )

        with self.consent_dao.session() as session:
            results = self.consent_dao.get_participants_with_unvalidated_files(session)
        self.assertEqual([], results)

    def test_getting_files_to_correct(self):
        """Test that all the consent files that need correcting are loaded"""
        # Create files that are ready to sync
        self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            type=ConsentType.CABOR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        # Create files that need correcting
        not_ready_primary = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )
        not_ready_cabor = self.data_generator.create_database_consent_file(
            type=ConsentType.CABOR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )
        not_ready_ehr = self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )
        not_ready_gror = self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )

        with self.consent_dao.session() as session:
            result_list = self.consent_dao.get_files_needing_correction(session)
        self.assertListsMatch(
            expected_list=[
                not_ready_primary, not_ready_cabor, not_ready_ehr, not_ready_gror
            ],
            actual_list=result_list,
            id_attribute='id'
        )

    def test_batch_update_of_results(self):
        """Make sure that any new or existing validation result records can be updated by the dao"""

        self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            file_path='/not_ready_ehr'
        )
        with self.consent_dao.session() as session:
            to_update = self.consent_dao.get_files_needing_correction(session)
            updates_to_send = [
                ConsentFile(
                    type=ConsentType.EHR,
                    sync_status=ConsentSyncStatus.READY_FOR_SYNC,
                    file_path='/ready_ehr'
                )
            ]
            for result in to_update:
                result.sync_status = ConsentSyncStatus.OBSOLETE
                updates_to_send.append(result)

            self.consent_dao.batch_update_consent_files(updates_to_send, session)
            session.commit()
            results = self.consent_dao.get_all()
            self.assertEqual(2, len(results))
            for result in results:
                if result.file_path == '/ready_ehr':
                    self.assertEqual(ConsentSyncStatus.READY_FOR_SYNC, result.sync_status)
                elif result.file_path == '/not_ready_ehr':
                    self.assertEqual(ConsentSyncStatus.OBSOLETE, result.sync_status)
                else:
                    self.fail('Unexpected file validation result')

    def assertListsMatch(self, expected_list, actual_list, id_attribute):
        self.assertEqual(len(expected_list), len(actual_list))

        actual_id_list = [getattr(actual, id_attribute) for actual in actual_list]
        for expected_summary in expected_list:
            self.assertIn(getattr(expected_summary, id_attribute), actual_id_list)

    def _init_summary_with_consent_data(self, submitted_consent_types: List[ConsentType],
                                        validated_consent_types: List[ConsentType], extra_summary_args=None):
        # Set up participant summary fields to indicate that the consent was submitted
        # (ignoring PrimaryUpdate since that's not where it's checked)
        consent_type_to_summary_field_map = {
            ConsentType.CABOR: 'consentForCABoR',
            ConsentType.EHR: 'consentForElectronicHealthRecords',
            ConsentType.GROR: 'consentForGenomicsROR'
        }
        summary_consent_fields_to_set = {}
        consent_submissions_to_set_on_summary = [
            consent_type for consent_type in submitted_consent_types if consent_type != ConsentType.PRIMARY_UPDATE
        ]
        for consent_type in consent_submissions_to_set_on_summary:
            participant_summary_field_name = consent_type_to_summary_field_map[consent_type]
            summary_consent_fields_to_set[participant_summary_field_name] = QuestionnaireStatus.SUBMITTED

        if extra_summary_args is not None:
            summary_consent_fields_to_set.update(extra_summary_args)
        summary = self.data_generator.create_database_participant_summary(**summary_consent_fields_to_set)

        if ConsentType.PRIMARY_UPDATE in submitted_consent_types:
            # PrimaryUpdate doesn't have any fields of its own,
            # but is instead detected using questionnaire response answers
            response = self.data_generator.create_database_questionnaire_response(
                questionnaireId=self.primary_update_questionnaire.questionnaireId,
                questionnaireVersion=self.primary_update_questionnaire.version,
                participantId=summary.participantId
            )
            self.data_generator.create_database_questionnaire_response_answer(
                questionnaireResponseId=response.questionnaireResponseId,
                questionId=self.primary_update_question.questionnaireQuestionId
            )

        # Create the validation records needed and link them to the summary
        for consent_type in validated_consent_types:
            self.data_generator.create_database_consent_file(
                participant_id=summary.participantId,
                type=consent_type
            )

        return summary
