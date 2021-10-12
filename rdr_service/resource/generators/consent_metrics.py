#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from dateutil.relativedelta import relativedelta

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

class ConsentMetricsGenerator(generators.BaseGenerator):
    """
    Generate a ConsentMetrics resource object
    """
    ro_dao = None

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
        if not isinstance(row, ConsentFile):
            raise (ValueError, 'Missing or invalid consent_file record')

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
                'invalid_signature': False,
                'invalid_signing_date': False,
                'checkbox_unchecked': False,
                'non_va_consent_for_va': False,
                'va_consent_for_non_va': False,
                'invalid_dob': False,
                'invalid_age_at_consent': False
        }

        # Lower environments have some dirty data/missing authored fields (e.g. "FirstYesAuthored" fields were never
        # populated).  Make best attempt to find appropriate authored dates
        primary_consent_authored = row.consentForStudyEnrollmentFirstYesAuthored\
                                   or row.consentForStudyEnrollmentAuthored
        ehr_consent_authored = row.consentForElectronicHealthRecordsFirstYesAuthored\
                               or row.consentForElectronicHealthRecordsAuthored
        cabor_authored = row.consentForCABoRAuthored
        gror_authored = row.consentForGenomicsRORAuthored
        primary_consent_update_authored = row.consentForStudyEnrollmentAuthored

        # Convert timestamp into date value (YYYY-MM-DD)
        if consent_type == ConsentType.PRIMARY and primary_consent_authored:
            data['consent_authored_date'] = primary_consent_authored.date()
        elif consent_type == ConsentType.CABOR and cabor_authored:
            data['consent_authored_date'] = cabor_authored.date()
        elif consent_type == ConsentType.EHR and ehr_consent_authored:
            data['consent_authored_date'] = ehr_consent_authored.date()
        elif consent_type == ConsentType.GROR and gror_authored:
            data['consent_authored_date'] = gror_authored.date()
        elif consent_type == ConsentType.PRIMARY_UPDATE and primary_consent_update_authored:
            data['consent_authored_date'] = primary_consent_update_authored.date()

        # Resolved/OBSOLETE records use the consent_file modified date as the resolved date
        if consent_status == ConsentSyncStatus.OBSOLETE and row.modified:
            data['resolved_date'] = row.modified.date()

        # For file-related errors, there is an implied hierarchy of errors for metrics.  If file is missing, then
        # missing signature is irrelevant, and if signature is missing, invalid signing date is irrelevant
        data['missing_file'] = not row.file_exists
        data['signature_missing'] = (row.file_exists and not row.is_signature_valid)
        data['invalid_signing_date'] = (row.is_signature_valid and not row.is_signing_date_valid)

        # Errors based on parsing the consent_file.other_errors string field:
        # TODO:  Make the known error strings constants
        if row.other_errors:
            data['checkbox_unchecked'] = row.other_errors.find(ConsentErrors.MISSING_CONSENT_CHECK_MARK) != -1
            data['non_va_consent_for_va'] = row.other_errors.find(ConsentErrors.NON_VETERAN_CONSENT_FOR_VETERAN) != -1
            data['va_consent_for_non_va'] = row.other_errors.find(ConsentErrors.VETERAN_CONSENT_FOR_NON_VETERAN) != -1

        # Populate DOB-related errors.  These are not tracked in the RDR consent_file table.  They are derived from
        # fields stored in the participant_summary record and only apply to the primary consent.
        if consent_type == ConsentType.PRIMARY:
            dob = row.dateOfBirth
            age_delta = relativedelta(primary_consent_authored, dob)
            data['invalid_dob'] = (dob is None or age_delta.years <= 0 or age_delta.years > INVALID_DOB_MAX_AGE_VALUE)
            data['invalid_age_at_consent'] = age_delta.years < VALID_AGE_AT_CONSENT if dob else False

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
                                  ParticipantSummary.dateOfBirth,
                                  ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored,
                                  ParticipantSummary.consentForStudyEnrollmentAuthored,
                                  ParticipantSummary.consentForCABoRAuthored,
                                  ParticipantSummary.consentForElectronicHealthRecordsFirstYesAuthored,
                                  ParticipantSummary.consentForElectronicHealthRecordsAuthored,
                                  ParticipantSummary.consentForGenomicsRORAuthored,
                                  HPO.hpoId,
                                  HPO.displayName.label('hpo_name'),
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
