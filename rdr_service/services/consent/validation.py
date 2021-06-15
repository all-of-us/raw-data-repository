from collections import defaultdict
from datetime import date, datetime
import pytz
from typing import List

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile as ParsingResult, ConsentSyncStatus, ConsentType
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.consent import files
from rdr_service.storage import GoogleCloudStorageProvider


class ConsentValidationController:
    def __init__(self, consent_dao: ConsentDao, participant_summary_dao: ParticipantSummaryDao,
                 hpo_dao: HPODao, storage_provider: GoogleCloudStorageProvider):
        self.consent_dao = consent_dao
        self.participant_summary_dao = participant_summary_dao
        self.storage_provider = storage_provider

        self.va_hpo_id = hpo_dao.get_by_name('VA').hpoId

    def check_for_corrections(self):
        """Load all of the current consent issues and see if they have been resolved yet"""
        files_needing_correction = self.consent_dao.get_files_needing_correction()
        organized_results = self._organize_results(files_needing_correction)

        validation_updates: List[ParsingResult] = []
        for participant_id, corrections_needed in organized_results.items():
            participant_summary: ParticipantSummary = self.participant_summary_dao.get(participant_id)
            validator = self._build_validator(participant_summary)

            for consent_type, validation_results in corrections_needed.items():
                new_validation_results = []
                if consent_type == ConsentType.PRIMARY:
                    new_validation_results = validator.get_primary_validation_results()
                elif consent_type == ConsentType.CABOR:
                    new_validation_results = validator.get_cabor_validation_results()
                elif consent_type == ConsentType.EHR:
                    new_validation_results = validator.get_ehr_validation_results()
                elif consent_type == ConsentType.GROR:
                    new_validation_results = validator.get_gror_validation_results()

                file_ready_for_sync = self._find_file_ready_for_sync(new_validation_results)
                if file_ready_for_sync is not None:
                    for previous_validation_result in validation_results:
                        previous_validation_result.sync_status = ConsentSyncStatus.OBSOLETE
                        validation_updates.append(previous_validation_result)
                    validation_updates.append(file_ready_for_sync)
                else:
                    for new_result in new_validation_results:
                        matching_previous_result = self._find_matching_validation_result(
                            new_result=new_result,
                            previous_results=validation_results
                        )
                        if matching_previous_result is None:
                            validation_updates.append(new_result)

            self.consent_dao.batch_update_consent_files(validation_updates)

    def validate_recent_uploads(self, min_consent_date):
        """Find all the expected consents since the minimum date and check the files that have been uploaded"""
        validation_results = []
        validated_participant_ids = []
        for summary in self.consent_dao.get_participants_with_consents_in_range(start_date=min_consent_date):
            validated_participant_ids.append(summary.participantId)
            validator = self._build_validator(summary)

            if self._has_new_consent(
                consent_status=summary.consentForStudyEnrollment,
                authored=summary.consentForStudyEnrollmentFirstYesAuthored,
                min_authored=min_consent_date
            ):
                validation_results.extend(self._process_validation_results(validator.get_primary_validation_results()))
            if self._has_new_consent(
                consent_status=summary.consentForCABoR,
                authored=summary.consentForCABoRAuthored,
                min_authored=min_consent_date
            ):
                validation_results.extend(self._process_validation_results(validator.get_cabor_validation_results()))
            if self._has_new_consent(
                consent_status=summary.consentForElectronicHealthRecords,
                authored=summary.consentForElectronicHealthRecordsAuthored,
                min_authored=min_consent_date
            ):
                validation_results.extend(self._process_validation_results(validator.get_ehr_validation_results()))
            if self._has_new_consent(
                consent_status=summary.consentForGenomicsROR,
                authored=summary.consentForGenomicsRORAuthored,
                min_authored=min_consent_date
            ):
                validation_results.extend(self._process_validation_results(validator.get_gror_validation_results()))

        results_to_store = []
        previous_results = self.consent_dao.get_validation_results_for_participants(
            participant_ids=validated_participant_ids
        )
        for possible_new_result in validation_results:
            if not any(
                [possible_new_result.file_path == previous_result.file_path for previous_result in previous_results]
            ):
                results_to_store.append(possible_new_result)

        self.consent_dao.batch_update_consent_files(results_to_store)

    @classmethod
    def _process_validation_results(cls, results: List[ParsingResult]):
        ready_file = cls._find_file_ready_for_sync(results)
        if ready_file:
            return [ready_file]
        else:
            return results

    @classmethod
    def _has_new_consent(cls, consent_status, authored, min_authored):
        return consent_status == QuestionnaireStatus.SUBMITTED and authored > min_authored

    def _build_validator(self, participant_summary: ParticipantSummary) -> 'ConsentValidator':
        consent_factory = files.ConsentFileAbstractFactory.get_file_factory(
            participant_id=participant_summary.participantId,
            participant_origin=participant_summary.participantOrigin,
            storage_provider=self.storage_provider
        )
        return ConsentValidator(
            consent_factory=consent_factory,
            participant_summary=participant_summary,
            va_hpo_id=self.va_hpo_id
        )

    @classmethod
    def _organize_results(cls, results: List[ParsingResult]):
        """
        Organize the validation results by participant id and then
        consent type to make it easier for checking for updates for them
        """

        def new_participant_results():
            return defaultdict(lambda: [])
        organized_results = defaultdict(new_participant_results)
        for result in results:
            organized_results[result.participant_id][result.type].append(result)
        return organized_results

    @classmethod
    def _find_file_ready_for_sync(cls, results: List[ParsingResult]):
        for result in results:
            if result.sync_status == ConsentSyncStatus.READY_FOR_SYNC:
                return result

        return None

    @classmethod
    def _find_matching_validation_result(cls, new_result: ParsingResult, previous_results: List[ParsingResult]):
        """Return the corresponding object from the list. They're matched up based on the file path."""
        for previous_result in previous_results:
            if new_result.file_path == previous_result.file_path:
                return previous_result

        return None


class ConsentValidator:
    def __init__(self, consent_factory: files.ConsentFileAbstractFactory,
                 participant_summary: ParticipantSummary,
                 va_hpo_id: int):
        self.factory = consent_factory
        self.participant_summary = participant_summary
        self.va_hpo_id = va_hpo_id

        self._central_time = pytz.timezone('America/Chicago')

    def get_primary_validation_results(self) -> List[ParsingResult]:
        return self._generate_validation_results(
            consent_files=self.factory.get_primary_consents(),
            consent_type=ConsentType.PRIMARY,
            additional_validation=self._validate_is_va_file,
            expected_sign_datetime=self.participant_summary.consentForStudyEnrollmentFirstYesAuthored
        )

    def get_ehr_validation_results(self) -> List[ParsingResult]:
        return self._generate_validation_results(
            consent_files=self.factory.get_ehr_consents(),
            consent_type=ConsentType.EHR,
            expected_sign_datetime=self.participant_summary.consentForElectronicHealthRecordsAuthored
        )

    def get_cabor_validation_results(self) -> List[ParsingResult]:
        return self._generate_validation_results(
            consent_files=self.factory.get_cabor_consents(),
            consent_type=ConsentType.CABOR,
            expected_sign_datetime=self.participant_summary.consentForCABoRAuthored
        )

    def get_gror_validation_results(self) -> List[ParsingResult]:

        def check_for_checkmark(consent: files.GrorConsentFile, result):
            if not consent.is_confirmation_selected():
                result.other_errors = 'missing consent check mark'
                result.sync_status = ConsentSyncStatus.NEEDS_CORRECTING

        return self._generate_validation_results(
            consent_files=self.factory.get_gror_consents(),
            consent_type=ConsentType.GROR,
            additional_validation=check_for_checkmark,
            expected_sign_datetime=self.participant_summary.consentForGenomicsRORAuthored
        )

    def _validate_is_va_file(self, consent, result: ParsingResult):
        is_va_consent = consent.get_is_va_consent()
        if self.participant_summary.hpoId == self.va_hpo_id and not is_va_consent:
            result.other_errors = 'non-veteran consent for veteran participant'
            result.sync_status = ConsentSyncStatus.NEEDS_CORRECTING
        elif self.participant_summary.hpoId != self.va_hpo_id and is_va_consent:
            result.other_errors = 'veteran consent for non-veteran participant'
            result.sync_status = ConsentSyncStatus.NEEDS_CORRECTING

    def _generate_validation_results(self, consent_files: List[files.ConsentFile], consent_type: ConsentType,
                                     expected_sign_datetime: datetime,
                                     additional_validation=None) -> List[ParsingResult]:
        results = []
        for consent in consent_files:
            result = self._build_validation_result(consent, consent_type, expected_sign_datetime)
            if additional_validation:
                additional_validation(consent, result)
            results.append(result)

        if not results:
            results.append(ParsingResult(
                participant_id=self.participant_summary.participantId,
                file_exists=False,
                type=consent_type,
                sync_status=ConsentSyncStatus.NEEDS_CORRECTING
            ))
        return results

    def _build_validation_result(self, consent: files.ConsentFile, consent_type: ConsentType,
                                 expected_sign_datetime: datetime):
        """
        Used to check generic data found on all consent types,
        additional result information should be validated for each type
        """
        result = ParsingResult(
            participant_id=self.participant_summary.participantId,
            file_exists=True,
            type=consent_type,
            file_upload_time=consent.upload_time,
            file_path=consent.file_path
        )
        self._store_signature(result=result, consent_file=consent)

        result.signing_date = consent.get_date_signed()
        result.expected_sign_date = self._get_date_from_datetime(expected_sign_datetime)
        result.is_signing_date_valid = self._is_signing_date_valid(
            signing_date=result.signing_date,
            expected_date=result.expected_sign_date
        )

        if result.is_signature_valid and result.is_signing_date_valid:
            result.sync_status = ConsentSyncStatus.READY_FOR_SYNC
        else:
            result.sync_status = ConsentSyncStatus.NEEDS_CORRECTING

        return result

    @classmethod
    def _store_signature(cls, result: ParsingResult, consent_file: files.ConsentFile):
        signature = consent_file.get_signature_on_file()
        result.is_signature_valid = bool(signature)
        if signature is True:  # True returned for when images are found
            result.is_signature_image = True
        elif signature is not None:
            result.signature_str = signature

    @classmethod
    def _is_signing_date_valid(cls, signing_date, expected_date: date):
        if not signing_date or not expected_date:
            return False
        else:
            return signing_date == expected_date

    def _get_date_from_datetime(self, timestamp: datetime):
        return timestamp.replace(tzinfo=pytz.utc).astimezone(self._central_time).date()
