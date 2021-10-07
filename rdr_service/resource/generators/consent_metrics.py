#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging


from sqlalchemy import func
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pprint import pprint

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.resource import generators, schemas
from rdr_service.model.consent_file import ConsentType, ConsentSyncStatus, ConsentFile
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization


class ConsentMetricsGenerator(generators.BaseGenerator):
    """
    Generate a ConsentMetrics resource object
    """
    ro_dao = None


    def make_resource(self, _pk, consent_file_rec=None):
        """
        Build a Resource object from the given consent_file record
        :param _pk: Primary key value from consent_file table
        :param rec:  A consent_file table row, if one was already retrieved
        :return: ResourceDataObject object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao()

        if not consent_file_rec:
            consent_file_rec = self.get_single_validation_record(_pk)

        data = self.prep_validation_data(consent_file_rec)
        return generators.ResourceRecordSet(schemas.ConsentMetricSchema, data)


    def prep_validation_data(self, row):
        """
        Transforms a consent_file record into a consent validation resource dictionary.  Reproduces the
        consent report SQL from the consent-report tool with its calculated columns into a Python generator
        """
        if not row:
            raise(ValueError, 'Missing consent_file record')

        data = {}
        unmodified_columns = ['id', 'created', 'modified', 'file_path', 'file_upload_time',
                              'signing_date', 'expected_sign_date', 'date_of_birth',
                              'hpo_id', 'organization_id']
        # Populate fields extracted directly from the row
        for column in unmodified_columns:
            data[column] = getattr(row, column)

        # Populate fields with minor transformations
        data['participant_id'] = f'P{row.participant_id}'
        data['sync_status'] = str(row.sync_status)
        data['sync_status_id'] = int(row.sync_status)
        data['hpo'] = row.hpo if row.hpo else '(Unpaired)'
        data['organization'] = row.organization if row.organization else '(No organization pairing)'

        # Populate details specific to the type of consent
        consent_type = ConsentType(int(row.type))
        data['consent_type'] = str(consent_type)
        data['consent_type_id'] = int(consent_type)
        if consent_type == ConsentType.PRIMARY:
            data['consent_authored_date'] = row.primary_consent_authored_date
        elif consent_type == ConsentType.CABOR:
            data['consent_authored_date'] = row.cabor_authored_date
        elif consent_type == ConsentType.EHR:
            data['consent_authored_date'] = row.ehr_authored_date
        elif consent_type == ConsentType.GROR:
            data['consent_authored_date'] = row.gror_authored_date
        elif consent_type == ConsentType.PRIMARY_UPDATE:
            data['consent_authored_date'] = row.primary_consent_update_authored_date

        # Populate the calculated fields
        data['resolved_date'] = datetime.date(row.modified) if row.sync_status == ConsentSyncStatus.OBSOLETE else None
        data['missing_file'] = True if not row.file_exists else False
        data['signature_missing'] = True if (row.file_exists and not row.is_signature_valid) else False
        data['invalid_signing_date'] = True if (row.is_signature_valid and not row.is_signing_date_valid) else False
        # Errors based on parsing strings in the other_errors field:
        data['checkbox_unchecked'] =\
            False if not row.other_errors else (row.other_errors.find('missing consent check mark') != -1)
        data['non_va_consent_for_va'] =\
            False if not row.other_errors else (row.other_errors.find('non-veteran consent for veteran') != -1)
        data['va_consent_for_non_va'] =\
            False if not row.other_errors else (row.other_errors.find('veteran consent for non-veteran') != -1)

        # Populate errors not tracked in the consent_file table; based on participant DOB/primary consent authored
        # Note:  Determination was made to treat a calculated age > 124 years at time of consent as an invalid DOB
        age_delta = relativedelta(row.primary_consent_authored_date, row.date_of_birth)
        data['invalid_dob'] = (
            consent_type == ConsentType.PRIMARY and (row.date_of_birth is None or age_delta.years <= 0
                                                     or age_delta.years > 124)
        )
        data['invalid_age_at_consent'] = (
            consent_type == ConsentType.PRIMARY and row.date_of_birth and age_delta.years < 18
        )
        # !DEBUG!
        if data['invalid_dob'] or data['invalid_age_at_consent']:
            pprint(data)
            print('\n')

        return data

    def get_consent_validation_records(self, ro_dao=None, date_filter='2021-10-04'):
        """
        Retrieve a block of consent_file validation records all at once, based on a "modified since" date filter
        :param ro_dao:  Read-only DAO object if one was already instantiated by the caller
        :param date_filter:  A date string in YYYY-MM-DD format to use for filtering consent_file records
        :return:  A result set from the query of consent validation data
        """
        if not ro_dao:
            ro_dao = self.ro_dao or ResourceDataDao()

        with ro_dao.session() as session:
            results = session.query(ConsentFile.id,
                                    ConsentFile.created,
                                    ConsentFile.modified,
                                    ConsentFile.participant_id,
                                    ConsentFile.type,
                                    ConsentFile.file_path,
                                    ConsentFile.file_upload_time,
                                    ConsentFile.signing_date,
                                    ConsentFile.expected_sign_date,
                                    ConsentFile.file_exists,
                                    ConsentFile.is_signature_valid,
                                    ConsentFile.is_signing_date_valid,
                                    ConsentFile.other_errors,
                                    ConsentFile.sync_status,
                                    # Match the string format of the other column names (underscores)
                                    ParticipantSummary.dateOfBirth.label('date_of_birth'),
                                    func.date(ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored)\
                                    .label('primary_consent_authored_date'),
                                    func.date(ParticipantSummary.consentForElectronicHealthRecordsFirstYesAuthored)\
                                    .label('ehr_authored_date'),
                                    func.date(ParticipantSummary.consentForCABoRAuthored)\
                                    .label('cabor_authored_date'),
                                    func.date(ParticipantSummary.consentForGenomicsRORAuthored)\
                                    .label('gror_authored_date'),
                                    func.date(ParticipantSummary.consentForStudyEnrollmentAuthored)\
                                    .label('primary_consent_update_authored_date'),
                                    HPO.displayName.label('hpo'),
                                    HPO.hpoId.label('hpo_id'),
                                    Organization.displayName.label('organization'),
                                    Organization.organizationId.label('organization_id')
                                    )\
                .join(ParticipantSummary, ParticipantSummary.participantId == ConsentFile.participant_id)\
                .outerjoin(HPO, HPO.hpoId == ParticipantSummary.hpoId)\
                .outerjoin(Organization, ParticipantSummary.organizationId == Organization.organizationId)\
                .filter(ConsentFile.modified >= date_filter)\
                .all()

            return results

def rebuild_consent_validation_resources_task(ro_dao=None, date_filter='2021-06-25'):
    """
    Cloud Tasks: Refresh the consent file metrics
    """
    gen = ConsentMetricsGenerator()
    if not ro_dao:
        gen.ro_dao = ro_dao = ResourceDataDao()

    results = gen.get_consent_validation_records(ro_dao=ro_dao, date_filter=date_filter)
    logging.info('Consent metrics: rebuilding {0} resource records...'.format(len(results)))
    for row in results:
        resource = gen.make_resource(row.id, consent_file_rec=row)
        resource.save()
