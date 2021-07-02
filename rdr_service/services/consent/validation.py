from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import date, datetime, timedelta
from io import StringIO
import pytz
from typing import Collection, List

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile as ParsingResult, ConsentSyncStatus, ConsentType
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.consent import files
from rdr_service.storage import GoogleCloudStorageProvider


class ValidationOutputStrategy(ABC):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process_results()

    def add_all(self, result_collection: Collection[ParsingResult]):
        for result in result_collection:
            self.add_result(result)

    @abstractmethod
    def add_result(self, result: ParsingResult):
        ...

    @abstractmethod
    def process_results(self):
        ...


class StoreResultStrategy(ValidationOutputStrategy):
    def __init__(self, session, consent_dao: ConsentDao):
        self.session = session
        self.results = []
        self.consent_dao = consent_dao

    def add_result(self, result: ParsingResult):
        self.results.append(result)

    def process_results(self):
        previous_results = self.consent_dao.get_validation_results_for_participants(
            session=self.session,
            participant_ids={result.participant_id for result in self.results}
        )
        new_results_to_store = []
        for possible_new_result in self.results:
            if not any(
                [possible_new_result.file_path == previous_result.file_path for previous_result in previous_results]
            ):
                new_results_to_store.append(possible_new_result)

        self.consent_dao.batch_update_consent_files(self.session, new_results_to_store)


class ReplacementStoringStrategy(ValidationOutputStrategy):
    def __init__(self, session, consent_dao: ConsentDao):
        self.session = session
        self.consent_dao = consent_dao
        self.participant_ids = set()

        def new_participant_results():
            return defaultdict(lambda: [])
        self.results = defaultdict(new_participant_results)

    def add_result(self, result: ParsingResult):
        self.results[result.participant_id][result.type].append(result)
        self.participant_ids.add(result.participant_id)

    def process_results(self):
        previous_results = self.consent_dao.get_validation_results_for_participants(
            session=self.session,
            participant_ids=self.participant_ids
        )
        def new_participant_results():
            return defaultdict(lambda: [])
        organized_previous_results = defaultdict(new_participant_results)
        for result in previous_results:
            organized_previous_results[result.participant_id][result.type].append(result)

        results_to_update = []
        for participant_id, consent_type_dict in self.results.items():
            for consent_type, result_list in consent_type_dict.items():
                ready_for_sync = self._find_file_ready_for_sync(result_list)
                previous_type_list: Collection[ParsingResult] = organized_previous_results[participant_id][consent_type]
                if ready_for_sync:
                    for result in previous_type_list:
                        if result.sync_status == ConsentSyncStatus.NEEDS_CORRECTING:
                            result.sync_status = ConsentSyncStatus.OBSOLETE
                            results_to_update.append(result)
                    results_to_update.append(ready_for_sync)
                else:
                    for possible_new_result in result_list:
                        if not any([possible_new_result.file_path == previous_result.file_path
                                    for previous_result in previous_type_list]):
                            results_to_update.append(possible_new_result)

        self.consent_dao.batch_update_consent_files(self.session, results_to_update)

    @classmethod
    def _find_file_ready_for_sync(cls, results: List[ParsingResult]):
        for result in results:
            if result.sync_status == ConsentSyncStatus.READY_FOR_SYNC:
                return result

        return None


class LogResultStrategy(ValidationOutputStrategy):
    def __init__(self, logger, verbose, storage_provider: GoogleCloudStorageProvider):
        self.logger = logger
        self.verbose = verbose
        self.storage_provider = storage_provider

        def new_participant_results():
            return defaultdict(lambda: [])
        self.results = defaultdict(new_participant_results)

    def add_result(self, result: ParsingResult):
        self.results[result.participant_id][result.type].append(result)

    def process_results(self):
        report_lines = []
        for validation_categories in self.results.values():
            if self.verbose:
                report_lines.append('')
            for result_list in validation_categories.values():
                for result in result_list:
                    report_lines.append(self._line_output_for_validation(result, verbose=self.verbose))

        self.logger.info('\n'.join(report_lines))

    def _line_output_for_validation(self, file: ParsingResult, verbose: bool):
        output_line = StringIO()
        if verbose:
            output_line.write(f'{file.id} - ')  # TODO: ljust the id
        output_line.write(f'P{file.participant_id} - {str(file.type).ljust(10)} ')

        if not file.file_exists:
            output_line.write('missing file')
        else:
            errors_with_file = []
            if not file.is_signature_valid:
                errors_with_file.append('invalid signature')
            if not file.is_signing_date_valid:
                errors_with_file.append(self._get_date_error_details(file, verbose))
            if file.other_errors is not None:
                errors_with_file.append(file.other_errors)

            output_line.write(', '.join(errors_with_file))
            if verbose:
                output_line.write(f'\n{self._get_link(file)}')

        return output_line.getvalue()

    @classmethod
    def _get_date_error_details(cls, file: ParsingResult, verbose: bool = False):
        extra_info = ''
        if verbose and file.signing_date and file.expected_sign_date:
            time_difference = file.signing_date - file.expected_sign_date
            extra_info = f', diff of {time_difference.days} days'
        return f'invalid signing date (expected {file.expected_sign_date} '\
               f'but file has {file.signing_date}{extra_info})'

    def _get_link(self, file: ParsingResult):
        bucket_name, *name_parts = file.file_path.split('/')
        blob = self.storage_provider.get_blob(
            bucket_name=bucket_name,
            blob_name='/'.join(name_parts)
        )
        return blob.generate_signed_url(datetime.utcnow() + timedelta(hours=2))


class ConsentValidationController:
    def __init__(self, consent_dao: ConsentDao, participant_summary_dao: ParticipantSummaryDao,
                 storage_provider: GoogleCloudStorageProvider):
        self.consent_dao = consent_dao
        self.participant_summary_dao = participant_summary_dao
        self.storage_provider = storage_provider

        self.va_hpo_id = 15

    def check_for_corrections(self):
        """Load all of the current consent issues and see if they have been resolved yet"""
        files_needing_correction = self.consent_dao.get_files_needing_correction()

        # Organize the corrections needed into a dict where the key is the participant id
        # and the value is another dictionary. That secondary dictionary is keyed by consent type
        # and the values in it are lists of corrections needed for that participant and consent type
        organized_results = self._organize_results(files_needing_correction)

        validation_updates: List[ParsingResult] = []
        for participant_id, corrections_needed in organized_results.items():
            participant_summary: ParticipantSummary = self.participant_summary_dao.get(participant_id)
            validator = self._build_validator(participant_summary)

            for consent_type, previous_file_records in corrections_needed.items():
                new_validation_results = []
                if consent_type == ConsentType.PRIMARY:
                    new_validation_results = validator.get_primary_validation_results()
                elif consent_type == ConsentType.CABOR:
                    new_validation_results = validator.get_cabor_validation_results()
                elif consent_type == ConsentType.EHR:
                    new_validation_results = validator.get_ehr_validation_results()
                elif consent_type == ConsentType.GROR:
                    new_validation_results = validator.get_gror_validation_results()

                # TODO: if there are multiple files that need correcting, the check only needs to happen once

                file_ready_for_sync = self._find_file_ready_for_sync(new_validation_results)
                if file_ready_for_sync is not None:
                    # If there is a file ready to sync, then mark all previous invalid files as obsolete
                    for previous_validation_result in previous_file_records:
                        previous_validation_result.sync_status = ConsentSyncStatus.OBSOLETE
                        validation_updates.append(previous_validation_result)
                    validation_updates.append(file_ready_for_sync)
                else:
                    # Add any new validation results to the list for updating
                    # (ignoring records for files already validated)
                    for new_result in new_validation_results:
                        matching_previous_result = self._find_matching_validation_result(
                            new_result=new_result,
                            previous_results=previous_file_records
                        )
                        if matching_previous_result is None:
                            validation_updates.append(new_result)

        self.consent_dao.batch_update_consent_files(validation_updates)

    def _build_ce_result(self, wrapper: files._ConsentBlobWrapper, participant_id):
        return ParsingResult(
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            file_exists=True,
            file_upload_time=wrapper.blob.updated,
            file_path=f'{wrapper.blob.bucket.name}/{wrapper.blob.name}',
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )

    def generate_records_for_ce(self, session, min_consent_date):
        validation_results: List[ParsingResult] = []
        validated_participant_ids = []
        summaries = self.consent_dao.get_ce_participants_with_consents(session, start_date=min_consent_date)
        for summary in summaries:
            print(f'on {summary.participantId}')
            validated_participant_ids.append(summary.participantId)
            consent_factory: files.ConsentFileAbstractFactory = files.ConsentFileAbstractFactory.get_file_factory(
                participant_id=summary.participantId,
                participant_origin=summary.participantOrigin,
                storage_provider=self.storage_provider
            )
            validation_records = [
                self._build_ce_result(participant_id=summary.participantId, wrapper=blob)
                for blob in consent_factory.consent_blobs
                if blob.blob.updated >= min_consent_date
            ]
            validation_results.extend(validation_records)

        results_to_store = []
        previous_results = self.consent_dao.get_validation_results_for_participants(
            session,
            participant_ids=validated_participant_ids
        )
        for possible_new_result in validation_results:
            if not any(
                [possible_new_result.file_path == previous_result.file_path for previous_result in previous_results]
            ):
                results_to_store.append(possible_new_result)

        self.consent_dao.batch_update_consent_files(session, results_to_store)

    def generate_new_validations(self, participant_id, consent_type: ConsentType,
                                 output_strategy: ValidationOutputStrategy):
        summary = self.participant_summary_dao.get(participant_id)
        validator = self._build_validator(summary)
        results = []
        if consent_type == ConsentType.PRIMARY:
            results = validator.get_primary_validation_results()
        elif consent_type == ConsentType.CABOR:
            results = validator.get_cabor_validation_results()
        elif consent_type == ConsentType.EHR:
            results = validator.get_ehr_validation_results()
        elif consent_type == ConsentType.GROR:
            results = validator.get_gror_validation_results()
        output_strategy.add_all(results)

    def validate_recent_uploads(self, session, output_strategy: ValidationOutputStrategy, min_consent_date=None,
                                max_consent_date=None):
        """Find all the expected consents since the minimum date and check the files that have been uploaded"""
        for summary in self.consent_dao.get_participants_with_consents_in_range(
            session, start_date=min_consent_date, end_date=max_consent_date
        ):
            print(summary.participantId)
            validator = self._build_validator(summary)

            if self._has_new_consent(
                consent_status=summary.consentForStudyEnrollment,
                authored=summary.consentForStudyEnrollmentFirstYesAuthored,
                min_authored=min_consent_date
            ):
                output_strategy.add_all(self._process_validation_results(validator.get_primary_validation_results()))
            if self._has_new_consent(
                consent_status=summary.consentForCABoR,
                authored=summary.consentForCABoRAuthored,
                min_authored=min_consent_date
            ):
                output_strategy.add_all(self._process_validation_results(validator.get_cabor_validation_results()))
            if self._has_new_consent(
                consent_status=summary.consentForElectronicHealthRecords,
                authored=summary.consentForElectronicHealthRecordsAuthored,
                min_authored=min_consent_date
            ):
                output_strategy.add_all(self._process_validation_results(validator.get_ehr_validation_results()))
            if self._has_new_consent(
                consent_status=summary.consentForGenomicsROR,
                authored=summary.consentForGenomicsRORAuthored,
                min_authored=min_consent_date
            ):
                output_strategy.add_all(self._process_validation_results(validator.get_gror_validation_results()))

    def generate_org_change_sync_records(self, session):
        validation_results = []
        participant_ids = []
        for summary in self.consent_dao.get_org_change_summaries(session):
            print(f'pid {summary.participantId}')
            participant_ids.append(summary.participantId)
            validator = self._build_validator(summary)

            if isinstance(validator.factory, files.VibrentConsentFactory):
                if summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED:
                    validation_results.extend(
                        self._process_validation_results(validator.get_primary_validation_results()))
                if summary.consentForCABoR == QuestionnaireStatus.SUBMITTED:
                    validation_results.extend(
                        self._process_validation_results(validator.get_cabor_validation_results()))
                if summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED:
                    validation_results.extend(
                        self._process_validation_results(validator.get_ehr_validation_results()))
                if summary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED:
                    validation_results.extend(
                        self._process_validation_results(validator.get_gror_validation_results()))
            else:
                validation_results.extend([
                    self._build_ce_result(participant_id=summary.participantId, wrapper=blob)
                    for blob in validator.factory.consent_blobs
                ])

        results_to_store = []
        previous_results = self.consent_dao.get_validation_results_for_participants(session,
                                                                                    participant_ids=participant_ids)
        for possible_new_result in validation_results:
            if not any(
                [possible_new_result.file_path == previous_result.file_path for previous_result in previous_results]
            ):
                results_to_store.append(possible_new_result)

        self.consent_dao.batch_update_consent_files(session, results_to_store)

    def revalidate_record(self, session, record_id):
        # TODO: revalidation needs to consider if there is another validation record ready to sync that
        #  would make the current obsolete
        # validation_record: ParsingResult = self.consent_dao.get_with_session(session, record_id)
        print(f'ignoring {record_id}')
        for validation_record in self.consent_dao.get_all_ehr_for_revalidation(session):
            print(f'reval for {validation_record.participant_id}')
            summary = self.participant_summary_dao.get_with_session(session, validation_record.participant_id)
            validator = self._build_validator(summary)
            validator.revalidate_file(validation_record)
            # self.consent_dao.batch_update_consent_files(session, [validation_record])

    @classmethod
    def _process_validation_results(cls, results: List[ParsingResult]):
        ready_file = cls._find_file_ready_for_sync(results)
        if ready_file:
            return [ready_file]
        else:
            return results

    @classmethod
    def _has_new_consent(cls, consent_status, authored, min_authored):
        return consent_status == QuestionnaireStatus.SUBMITTED and (not min_authored or authored > min_authored)

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
            additional_validation=self._validate_is_va_file,
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

    def revalidate_file(self, validation_result: ParsingResult):
        if not validation_result.file_exists:
            return

        new_consent_data = self.factory.get_consent_for_path(validation_result.file_path)
        if isinstance(new_consent_data, files.PrimaryConsentFile):
            expected_sign_date = self.participant_summary.consentForStudyEnrollmentFirstYesAuthored
            self._build_validation_result(
                consent=new_consent_data,
                consent_type=validation_result.type,
                expected_sign_datetime=expected_sign_date,
                result=validation_result
            )
            self._validate_is_va_file(consent=new_consent_data, result=validation_result)
        elif isinstance(new_consent_data, files.EhrConsentFile):
            expected_sign_date = self.participant_summary.consentForElectronicHealthRecordsAuthored
            self._build_validation_result(
                consent=new_consent_data,
                consent_type=validation_result.type,
                expected_sign_datetime=expected_sign_date,
                result=validation_result
            )
            self._validate_is_va_file(consent=new_consent_data, result=validation_result)
        else:
            print(f'record {validation_result.id} does not point to a primary file')

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
                                 expected_sign_datetime: datetime, result: ParsingResult = None):
        """
        Used to check generic data found on all consent types,
        additional result information should be validated for each type
        """
        if result is None:
            result = ParsingResult(
                participant_id=self.participant_summary.participantId,
                type=consent_type,
                file_path=consent.file_path
            )
        result.file_exists = True
        result.file_upload_time = consent.upload_time

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
            days_off = (signing_date - expected_date).days
            return abs(days_off) < 10

    def _get_date_from_datetime(self, timestamp: datetime):
        return timestamp.replace(tzinfo=pytz.utc).astimezone(self._central_time).date()
