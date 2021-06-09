from datetime import datetime
from typing import List

from rdr_service.model.consent_file import ConsentFile as ParsingResult, ConsentSyncStatus, ConsentType
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.consent import files


class ConsentValidator:
    def __init__(self, consent_factory: files.ConsentFileAbstractFactory,
                 participant_summary: ParticipantSummary,
                 va_hpo_id: int):
        self.factory = consent_factory
        self.participant_summary = participant_summary
        self.va_hpo_id = va_hpo_id

    def get_primary_validation_results(self):

        def validate_va(consent, result):
            is_va_consent = consent.get_is_va_consent()
            if self.participant_summary.hpoId == self.va_hpo_id and not is_va_consent:
                result.other_errors = 'non-veteran consent for veteran participant'
                result.sync_status = ConsentSyncStatus.NEEDS_CORRECTING
            elif self.participant_summary.hpoId != self.va_hpo_id and is_va_consent:
                result.other_errors = 'veteran consent for non-veteran participant'
                result.sync_status = ConsentSyncStatus.NEEDS_CORRECTING

        return self._generate_validation_results(
            consent_files=self.factory.get_primary_consents(),
            consent_type=ConsentType.PRIMARY,
            additional_validation=validate_va
        )

    def _generate_validation_results(self, consent_files: List[files.ConsentFile], consent_type: ConsentType,
                                     additional_validation=None):
        results = []
        for consent in consent_files:
            result = self._build_validation_result(consent, consent_type)
            if additional_validation:
                additional_validation(consent, result)
            results.append(result)
        return results

    def _build_validation_result(self, consent: files.ConsentFile, consent_type: ConsentType):
        """
        Used to check generic data found on all consent types,
        additional result information should be validated for each type
        """
        result = ParsingResult(
            participant_id=self.participant_summary.participantId,
            file_exists=True,
            type=consent_type
        )
        self._store_signature(result=result, consent_file=consent)

        result.signing_date = consent.get_date_signed()
        result.is_signing_date_valid = self._is_signing_date_valid(
            signing_date=result.signing_date,
            expected_datetime=self.participant_summary.consentForStudyEnrollmentFirstYesAuthored
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
    def _is_signing_date_valid(cls, signing_date, expected_datetime: datetime):
        if not signing_date or not expected_datetime:
            return False
        else:
            return signing_date == expected_datetime.date()
