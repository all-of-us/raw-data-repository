#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from dateutil.relativedelta import relativedelta
from datetime import date

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.resource import generators, schemas
from rdr_service.model.consent_file import ConsentType, ConsentSyncStatus, ConsentFile, ConsentErrors
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization

# Note:  Determination was made to treat a calculated age > 124 years at time of consent as an invalid DOB
# Currently must be 18 to consent to the AoU study.   These values could change in the future
INVALID_DOB_MAX_AGE_VALUE = 124
VALID_AGE_AT_CONSENT = 18
MISSING_SIGNATURE_FALSE_POSITIVE_CUTOFF_DATE = date(year=2018, month=7, day=13)
METRICS_ERROR_LIST = [
    'missing_file', 'invalid_signing_date', 'signature_missing', 'checkbox_unchecked', 'non_va_consent_for_va',
    'va_consent_for_non_va', 'invalid_dob', 'invalid_age_at_consent'
]

class ConsentMetricsGenerator(generators.BaseGenerator):
    """
    Generate a ConsentMetrics resource object
    """
    ro_dao = None

    @classmethod
    def _get_authored_dates_from_rec(cls, rec):
        """ Find authored dates in the record (from participant_summary joined fields) based on consent type"""
        # Lower environments havs some dirty/incomplete data (e.g., "FirstYesAuthored" fields were not backfilled)
        # Make a best effort to assign the appropriate authored date for a consent
        return {
            ConsentType.PRIMARY: rec.consentForStudyEnrollmentFirstYesAuthored \
                                 or rec.consentForStudyEnrollmentAuthored,
            ConsentType.EHR: rec.consentForElectronicHealthRecordsFirstYesAuthored \
                             or rec.consentForElectronicHealthRecordsAuthored,
            ConsentType.CABOR: rec.consentForCABoRAuthored,
            ConsentType.GROR: rec.consentForGenomicsRORAuthored,
            ConsentType.PRIMARY_UPDATE: rec.consentForStudyEnrollmentAuthored
        }

    def make_resource(self, _pk, consent_file_rec=None):
        """
        Build a Resource object for the requested consent_file record
        :param _pk: Primary key id value from consent_file table
        :param consent_file_rec:  A consent_file table row, if one was already retrieved
        :return: ResourceDataObject object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=True)

        if not consent_file_rec:
            # Retrieve a single validation record for the provided id/primary key value
            result = self.get_consent_validation_records(id_list=[_pk])
            if not len(result):
                logging.warning(f'Consent metrics record retrieval failed for consent_file id {_pk}')
                return None
            else:
                consent_file_rec = result[0]

        data = self._make_consent_validation_dict(consent_file_rec)
        return generators.ResourceRecordSet(schemas.ConsentMetricSchema, data)

    @staticmethod
    def _make_consent_validation_dict(row):
        """
        Transforms a consent_file record into a consent metrics resource object dictionary.  Reproduces the
        CONSENT_REPORT_SQL logic from the consent-report tool, for calculating the error columns and dates
        """

        def _is_potential_false_positive_for_consent_version(resource, hpo):
            """
            In isolated cases, consent validation could run before a participant pairing to VA was complete, and could
            result in a potential false positive for va_consent_for_non_va errors.  Ignore NEEDS_CORRECTING records
            where the current HPO pairing is VA, and the only error was va_consent_for_non_va
            """
            # This filter only applies to participants whose current pairing is VA
            if hpo != 'VA':
                return False

            # Confirm no other errors except va_consent_for_non_va were detected
            for error in METRICS_ERROR_LIST:
                if error == 'va_consent_for_non_va':
                    continue
                if resource[error]:
                    return False

            return True

        # TODO:  This may be deprecated
        def _is_potential_false_positive_for_missing_signature(resource, expected_sign_date, signing_date):
            """
            Returns True if the following conditions in the consent_metrics_fields data dict are met:
            missing_file == 0 AND
            expected_sign_date < 2018-07-13 AND
            signing_date is null AND
            resource data dict has no other errors set
            Consents fitting these descriptions are known to cause false positive missing signature errors
            """
            # Confirm signing_date is null and expected_sign_date is in the known false positive range
            if signing_date or expected_sign_date >= MISSING_SIGNATURE_FALSE_POSITIVE_CUTOFF_DATE:
                return False

            # Confirm no other errors except for signature_missing were detected for this consent validation
            for error in METRICS_ERROR_LIST:
                if error == 'signature_missing':
                    continue
                if resource[error]:
                    return False

            return True

        # -- MAIN BODY OF GENERATOR --
        if not row or not len(row):
            raise (ValueError, 'Missing consent_file record')

        consent_type = row.type
        consent_status = row.sync_status

        # Set up defaults values
        data = {'id': row.id,
                'created': row.created,
                'modified': row.modified,
                'hpo_id': row.hpoId,
                'organization_id': row.organizationId,
                # TODO:  Confirm if we need the 'P' prefix here?
                'participant_id': f'P{row.participant_id}',
                'consent_type': str(consent_type),
                'consent_type_id': int(consent_type),
                'sync_status': str(consent_status),
                'sync_status_id': int(consent_status),
                'hpo': row.hpo_name,
                'organization': row.organization_name,
                'consent_authored_date': None,
                'resolved_date': None,
                'missing_file': False,
                'signature_missing': False,
                'invalid_signing_date': False,
                'checkbox_unchecked': False,
                'non_va_consent_for_va': False,
                'va_consent_for_non_va': False,
                'invalid_dob': False,
                'invalid_age_at_consent': False,
                'ignore': False
        }

        authored_date_map = ConsentMetricsGenerator._get_authored_dates_from_rec(row)
        if authored_date_map[consent_type]:
            data['consent_authored_date'] = authored_date_map[consent_type].date()

        # Resolved/OBSOLETE records use the consent_file modified date as the resolved date
        if consent_status == ConsentSyncStatus.OBSOLETE and row.modified:
            data['resolved_date'] = row.modified.date()

        # For file-related errors, there is an implied hierarchy of errors for metrics.  If file is missing, then
        # missing signature is irrelevant, and if signature is missing, invalid signing date is irrelevant
        data['missing_file'] = not row.file_exists
        data['signature_missing'] = (row.file_exists and not row.is_signature_valid)
        data['invalid_signing_date'] = (row.is_signature_valid and not row.is_signing_date_valid)

        # Errors based on parsing the consent_file.other_errors string field:
        if row.other_errors:
            data['checkbox_unchecked'] = row.other_errors.find(ConsentErrors.MISSING_CONSENT_CHECK_MARK) != -1
            data['non_va_consent_for_va'] = row.other_errors.find(ConsentErrors.NON_VETERAN_CONSENT_FOR_VETERAN) != -1
            data['va_consent_for_non_va'] = row.other_errors.find(ConsentErrors.VETERAN_CONSENT_FOR_NON_VETERAN) != -1

        # Populate DOB-related errors.  These are not tracked in the RDR consent_file table.  They are derived from
        # fields stored in the participant_summary record and only apply to the primary consent.
        dob = row.dateOfBirth
        if consent_type == ConsentType.PRIMARY:
            age_delta = relativedelta(authored_date_map[ConsentType.PRIMARY], dob) if dob else None
            data['invalid_dob'] = (dob is None or age_delta.years <= 0 or age_delta.years > INVALID_DOB_MAX_AGE_VALUE)
            data['invalid_age_at_consent'] = age_delta.years < VALID_AGE_AT_CONSENT if dob else False

        # Special conditions where these records may be ignored.  Some known "false positive" conditions or
        # cases where the record has a non-standard sync_status value (anything "above" SYNC_COMPLETE, such as
        # UNKNOWN or DELAYING_SYNC values)
        data['ignore'] = (_is_potential_false_positive_for_missing_signature(data,
                                                                             row.expected_sign_date,
                                                                             row.signing_date)
                          or _is_potential_false_positive_for_consent_version(data, row.hpo_name)
                          or row.sync_status > ConsentSyncStatus.SYNC_COMPLETE
                          )
        return data

    def get_consent_validation_records(self, dao=None, id_list=None, date_filter='2021-06-01'):
        """
        Retrieve a block of consent_file validation records based on an id list or  "modified since" date filter
        If an id list is provided, the date_filter will be ignored
        :param dao:  Read-only DAO object if one was already instantiated by the caller
        :param id_list: List of specific consent_file record IDs to retrieve.  Takes precedence over date_filter
        :param date_filter:  A date string in YYYY-MM-DD format to use for filtering consent_file records. The
                             default retrieves all records since consent validation started (in all environments)
        :return:  A result set from the query of consent validation data
        """
        if not dao:
            dao = self.ro_dao or ResourceDataDao(backup=True)

        with dao.session() as session:
            query = session.query(ConsentFile.id,
                                  ConsentFile.created,
                                  ConsentFile.modified,
                                  ConsentFile.participant_id,
                                  ConsentFile.type,
                                  ConsentFile.sync_status,
                                  ConsentFile.file_exists,
                                  ConsentFile.is_signature_valid,
                                  ConsentFile.is_signing_date_valid,
                                  ConsentFile.other_errors,
                                  # These ConsentFile fields are used to filter out false positives
                                  ConsentFile.expected_sign_date,
                                  ConsentFile.signing_date,
                                  ParticipantSummary.dateOfBirth,
                                  ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored,
                                  ParticipantSummary.consentForStudyEnrollmentAuthored,
                                  ParticipantSummary.consentForCABoRAuthored,
                                  ParticipantSummary.consentForElectronicHealthRecordsFirstYesAuthored,
                                  ParticipantSummary.consentForElectronicHealthRecordsAuthored,
                                  ParticipantSummary.consentForGenomicsRORAuthored,
                                  HPO.hpoId,
                                  HPO.name.label('hpo_name'),
                                  Organization.organizationId,
                                  Organization.displayName.label('organization_name'))\
                  .join(ParticipantSummary, ParticipantSummary.participantId == ConsentFile.participant_id)\
                  .outerjoin(HPO, HPO.hpoId == ParticipantSummary.hpoId)\
                  .outerjoin(Organization, ParticipantSummary.organizationId == Organization.organizationId)

            if id_list and len(id_list):
                query = query.filter(ConsentFile.id.in_(id_list))
            else:
                query = query.filter(ConsentFile.modified >= date_filter)

            # TODO:  Remove this vibrent filter once we're validating CE consents
            results = query.filter(ParticipantSummary.participantOrigin == 'vibrent').all()
            if not len(results):
                logging.warning('No consent metrics results found.  Please check the query filters')

            return results
