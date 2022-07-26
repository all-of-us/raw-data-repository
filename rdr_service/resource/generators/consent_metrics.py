#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from dateutil.relativedelta import relativedelta
from datetime import datetime, date

from re import findall as re_findall

from rdr_service import config
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.dao.consent_dao import ConsentErrorReportDao
from rdr_service.resource import generators, schemas
from rdr_service.model.consent_file import (ConsentType, ConsentSyncStatus, ConsentFile, ConsentOtherErrors,
                                            ConsentErrorReport)
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.services import email_service

# Note:  Determination was made to treat a calculated age > 124 years at time of consent as an invalid DOB
# Currently must be 18 to consent to the AoU study.   These values could change in the future
INVALID_DOB_AGE_CUTOFF = 124
# TODO:  VALID_AGE_AT_CONSENT and related checks must be updated when AoU enrollment is opened to participants under 18
VALID_AGE_AT_CONSENT = 18
MISSING_SIGNATURE_FALSE_POSITIVE_CUTOFF_DATE = date(year=2018, month=7, day=13)
# Map error flag fields to text strings used in generating error reports for PTSC
METRICS_ERROR_TYPES = {
    'missing_file': 'Missing file',
    'invalid_signing_date': 'Signing date invalid',
    'signature_missing': 'Missing signature',
    'checkbox_unchecked': 'Checkbox not checked',
    'non_va_consent_for_va': 'Non-VA consent version for VA participant',
    'va_consent_for_non_va': 'VA consent version for non-VA participant',
    'invalid_dob': 'Invalid date of birth',
    'invalid_age_at_consent': 'Invalid age at consent',
    'invalid_printed_name': 'Invalid printed name',
    'sensitive_ehr_expected': 'Non-sensitive EHR consent version when sensitive EHR expected',
    'non_sensitive_ehr_expected': 'Sensitive EHR consent version when non-sensitive EHR expected',
    'sensitive_ehr_missing_initials': 'Missing expected initials on sensitive EHR'
}

class ConsentMetricGenerator(generators.BaseGenerator):
    """
    Generate a ConsentMetric resource object
    """
    def __init__(self, ro_dao=None):
        self.ro_dao = ro_dao

    @classmethod
    def _get_authored_timestamps(cls, rec):
        """
        Find authored dates in the record (from participant_summary / joined fields) based on consent type
        :param rec: A result row from ConsentMetricGenerator.get_consent_validation_records()
        :returns:  A dictionary of consent type keys and their authored date values from the result row
        """
        # Default values taken from participant_summary fields, in case the consent_file record we're generating the
        # metric for predates the linking to a questionnaire_response_id (via the added consent_response table).
        consent_authored_values = {
            ConsentType.PRIMARY: rec.consentForStudyEnrollmentFirstYesAuthored\
                                 or rec.consentForStudyEnrollmentAuthored,
            ConsentType.EHR: rec.consentForElectronicHealthRecordsFirstYesAuthored\
                             or rec.consentForElectronicHealthRecordsAuthored,
            ConsentType.CABOR: rec.consentForCABoRAuthored,
            ConsentType.GROR: rec.consentForGenomicsRORAuthored,
            ConsentType.PRIMARY_UPDATE: rec.consentForStudyEnrollmentAuthored
        }
        # Add the authored value for the specific record passed, using authored time in the linked
        # questionnaire_response record if available
        default_authored = consent_authored_values.get(rec.type, None)
        consent_authored_values[rec.type] = rec.questionnaire_response_authored or default_authored

        if not consent_authored_values[rec.type]:
            logging.error(f'Unresolved {str(ConsentType(rec.type))} authored timestamp for consent_file id {rec.id}')

        return consent_authored_values

    @staticmethod
    def _calculate_age(dob: date, at_date: date) -> int:
        """
        Calculate age in years from two date objects
        :param dob:   Date of birth (date object)
        :param at_date:  Date at which to calculate age (date object)
        """
        if not (isinstance(dob, date) and isinstance(at_date, date)):
            return None

        age_delta = relativedelta(at_date, dob)
        return age_delta.years

    def make_resource(self, _pk, consent_validation_rec=None):
        """
        Build a Resource object for the requested consent_file record
        :param _pk: Primary key id value from consent_file table
        :param consent_validation_rec:  A result row from get_consent_validation_records(), if one was already retrieved
        :return: ResourceDataObject object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=True)

        if not consent_validation_rec:
            # Retrieve a single validation record for the provided id/primary key value
            result = self.get_consent_validation_records(id_list=[_pk])
            if not len(result):
                logging.warning(f'Consent metrics record retrieval failed for consent_file id {_pk}')
                return None
            else:
                consent_validation_rec = result[0]

        data = self._make_consent_validation_dict(consent_validation_rec)
        return generators.ResourceRecordSet(schemas.ConsentMetricSchema, data)

    @classmethod
    def pairing_at_consent(cls, pid, consent_authored, dao=None) -> str:
        """
        Searches a participant's pairing history to determine their HPO pairing at the time of consent
        :param pid:  integer participant id
        :param consent_authored: time of consent (authored) datetime value
        :return: The hpo.name string value from the HPO table, or 'UNSET' if there is no paired HPO
        """
        if not dao:
            dao = ResourceDataDao(backup=True)

        pairing_at_consent = 'UNSET'   # Default for unpaired/no pairing history present
        with dao.session() as session:
            results = session.query(
                        ParticipantHistory, HPO.name.label('hpo_name')
                      ).outerjoin(
                        HPO, ParticipantHistory.hpoId == HPO.hpoId
                      ).filter(
                        ParticipantHistory.participantId == pid
                      ).order_by(
                        ParticipantHistory.lastModified
                      ).all()
            for row in results:
                if row.ParticipantHistory.lastModified <= consent_authored:
                    pairing_at_consent = row.hpo_name if row.hpo_name else 'UNSET'
                else:
                    break

        return pairing_at_consent

    @classmethod
    def has_errors(cls, resource_data, exclude=[]):
        """
        Confirms if the provided data dictionary as any error flags (other than the exclusions) set
        Convenience routine when filtering for false positives and want to look for error conditions other than
        the false positive error type
        :param resource_data:  data dictionary to check
        :param exclude: list of error keys to ignore/exclude from check
        """
        error_list = [e for e in METRICS_ERROR_TYPES.keys() if e not in exclude]
        for error in error_list:
            if resource_data[error]:
                return True

        return False

    @classmethod
    def is_valid_dob(cls, consent_authored : datetime, dob: date) -> bool:
        """
        Verifies if a date of birth (date object) value is valid / in range relative to when participant consented
        :param consent_authored:  Primary consent authored (datetime)
        :param dob:  DOB date object (e.g., from participantSummary.dateOfBirth)
        :return:  True if DOB is valid, else False
        """
        if not isinstance(dob, date):
            # E.g., can capture cases were DOB value was unexpectedly null
            return False

        # Age <= 0 (e.g., future date given for DOB) or > reasonable max age is flagged as invalid DOB value
        # TODO:  May need to refine check when calculated age is 0 years, if AoU enrollment criteria changes
        age = cls._calculate_age(dob, consent_authored.date())
        return 0 < age <= INVALID_DOB_AGE_CUTOFF

    @classmethod
    def is_valid_age_at_consent(cls, consent_authored : datetime, dob : date) -> bool:
        """
        Verifies if the participant is of valid age for AoU enrollment, based on the date of birth value they
        provided and the time the consent was authored.
        :param consent_authored: Consent authored (datetime)
        :param dob:  DOB date object (e.g., from participantSummary.dateOfBirth)
        :return: True if age at consent is valid, else False
        """
        if not isinstance(dob, date):
            return False

        age = cls._calculate_age(dob, consent_authored.date())
        return age >= VALID_AGE_AT_CONSENT

    def _make_consent_validation_dict(self, row):
        """
        Transforms a result record from ConsentMetricGenerator.get_consent_validation_records() into a
        consent metrics resource object dictionary.  The content mirrors that of the pandas dataframe(s)
        generated by the tools/tool_libs/consent_validation_report.py manual spreadsheet report generator
        :param row: Result from get_consent_validation_records()
        """

        def _is_potential_false_positive_for_consent_version(resource, authored):
            """
            Known false positive "VA Consent for non-VA Participant" error scenarios:
            - Common case: Participant signed up via VA vanity link/was automatically paired to VA, consented, and then
              re-paired (e.g., to PITT) before the consent validation process ran.
            - In isolated cases, consent validation could run before a participant pairing to VA was complete
            This check returns True only if:
                Status is NEEDS_CORRECTING
                Pairing at time of consent was VA (common case) or current pairing is VA
                resource data dict has no other errors set besides va_consent_for_non_va
            """
            if (
                resource['sync_status'] != str(ConsentSyncStatus.NEEDS_CORRECTING)
                # participant_id extracted from resource data is a string with leading 'P'; convert to int
                or (self.pairing_at_consent(int(resource.get('participant_id')[1:]), authored) != 'VA'
                    and resource.get('hpo') != 'VA')
                or ConsentMetricGenerator.has_errors(resource, exclude=['va_consent_for_non_va'])
            ):
                return False

            return True

        def _is_potential_false_positive_for_missing_signature(resource, expected_sign_date, signing_date):
            """
            This identifies results that are known to be associated with false positives for
            missing signatures.  Returns True only if all the following conditions exist:
                status is NEEDS_CORRECTING
                expected_sign_date < 2018-07-13 AND
                signing_date is null AND
                resource data dict has no other errors set besides signature_missing and/or invalid_signing_date
            """
            if (resource['sync_status'] != str(ConsentSyncStatus.NEEDS_CORRECTING)
                    or signing_date
                    or (expected_sign_date and expected_sign_date >= MISSING_SIGNATURE_FALSE_POSITIVE_CUTOFF_DATE)
                    or ConsentMetricGenerator.has_errors(resource, exclude=['signature_missing',
                                                                            'invalid_signing_date'])):
                return False

            return True

        # -- MAIN BODY OF GENERATOR -- #
        if not row:
            raise (ValueError, 'Missing consent_file validation record')

        consent_type = row.type
        consent_status = row.sync_status

        # Set up default values
        data = {'id': row.id,
                'created': row.created,
                'modified': row.modified,
                'participant_id': f'P{row.participant_id}',
                'participant_origin': row.participantOrigin,
                'hpo': row.hpo_name,
                'hpo_id': row.hpoId,
                'organization': row.organization_name,
                'organization_id': row.organizationId,
                'consent_type': str(consent_type),
                'consent_type_id': int(consent_type),
                'sync_status': str(consent_status),
                'sync_status_id': int(consent_status),
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
                'invalid_printed_name': False,
                'sensitive_ehr_expected': False,
                'non_sensitive_ehr_expected': False,
                'sensitive_ehr_missing_initials': False,
                'test_participant': False,
                'ignore': False
        }

        # The record/row will have a questionnaire_response_authored value if there was a related consent_response entry
        # (true for more recently created consent_file records in RDR).  Otherwise, use the pre-fetched timestamps
        # from participant_summary to find the one associated with this consent type.
        authored_ts_from_row = row.questionnaire_response_authored or \
                               ConsentMetricGenerator._get_authored_timestamps(row).get(consent_type, None)
        if authored_ts_from_row:
            data['consent_authored_date'] = authored_ts_from_row.date()

        # Resolved/OBSOLETE records use the consent_file modified date as the resolved date
        if consent_status == ConsentSyncStatus.OBSOLETE and row.modified:
            data['resolved_date'] = row.modified.date()

        # There is an implied hierarchy of some errors for metrics reporting.  Missing signature errors are not flagged
        # unless the file exists, and invalid signing date is not flagged unless there is a signature
        data['missing_file'] = not row.file_exists
        data['signature_missing'] = (row.file_exists and not row.is_signature_valid)
        data['invalid_signing_date'] = (row.is_signature_valid and not row.is_signing_date_valid)

        # Errors based on parsing the consent_file.other_errors string field:
        if row.other_errors:
            data['checkbox_unchecked'] =\
                row.other_errors.find(ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK) != -1
            data['non_va_consent_for_va'] =\
                row.other_errors.find(ConsentOtherErrors.NON_VETERAN_CONSENT_FOR_VETERAN) != -1
            data['va_consent_for_non_va'] =\
                row.other_errors.find(ConsentOtherErrors.VETERAN_CONSENT_FOR_NON_VETERAN) != -1
            data['invalid_printed_name'] =\
                row.other_errors.find(ConsentOtherErrors.INVALID_PRINTED_NAME) != -1
            data['sensitive_ehr_expected'] =\
                row.other_errors.find(ConsentOtherErrors.SENSITIVE_EHR_EXPECTED) != -1
            data['non_sensitive_ehr_expected'] =\
                row.other_errors.find(ConsentOtherErrors.NONSENSITIVE_EHR_EXPECTED) != -1
            data['sensitive_ehr_missing_initials'] =\
                row.other_errors.find(ConsentOtherErrors.INITIALS_MISSING_ON_SENSITIVE_EHR) != -1

        # DOB-related errors are not tracked in the RDR consent_file table.  They are derived from
        # participant_summary data and only apply to the primary consent.  invalid_age_at_consent will only be true
        # if a valid DOB value is present
        if consent_type == ConsentType.PRIMARY:
            dob_valid = self.is_valid_dob(authored_ts_from_row, row.dateOfBirth)
            data['invalid_dob'] = not dob_valid
            data['invalid_age_at_consent'] = dob_valid and\
                                             not self.is_valid_age_at_consent(authored_ts_from_row,
                                                                              row.dateOfBirth)

        # PDR convention: map RDR ghost and test participants to test_participant = True
        data['test_participant'] = (row.hpo_name == 'TEST' or row.isTestParticipant == 1 or row.isGhostId == 1)

        # Special conditions where these records may be ignored for reporting.  Some known "false positive" conditions
        # or the record has a non-standard sync_status (LEGACY, UNKNOWN, DELAYING_SYNC),
        data['ignore'] = (_is_potential_false_positive_for_missing_signature(data,
                                                                             row.expected_sign_date,
                                                                             row.signing_date)
                          or _is_potential_false_positive_for_consent_version(data, authored_ts_from_row)
                          or row.sync_status > ConsentSyncStatus.SYNC_COMPLETE
                          )
        return data

    def get_consent_validation_records(self, dao=None, id_list=None, sync_statuses=None, consent_types=None,
                                       origin=None, date_filter='2021-06-01'):
        """
        Retrieve consent_file validation records based on provided filter(s)
        :param dao:  Read-only DAO object if one was already instantiated by the caller
        :param id_list: List of specific consent_file record IDs to retrieve.  Overrides date_filter
        :param sync_statuses: List of ConsentSyncStatus values to filter on.  Always applied if present
        :param consent_types: List of ConsentType values to filter on.  Always applied if present
        :param date_filter: Date value; default value will return all consent_file records (from any RDR environment)
        :param origin: participant origin string filter
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
                                  ConsentFile.expected_sign_date,
                                  ConsentFile.signing_date,
                                  ConsentFile.file_path,
                                  ConsentFile.file_upload_time,
                                  ConsentFile.consent_response_id,
                                  ConsentFile.consent_error_report,
                                  QuestionnaireResponse.authored.label('questionnaire_response_authored'),
                                  ParticipantSummary.dateOfBirth,
                                  ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored,
                                  ParticipantSummary.consentForStudyEnrollmentAuthored,
                                  ParticipantSummary.consentForCABoRAuthored,
                                  ParticipantSummary.consentForElectronicHealthRecordsFirstYesAuthored,
                                  ParticipantSummary.consentForElectronicHealthRecordsAuthored,
                                  ParticipantSummary.consentForGenomicsRORAuthored,
                                  Participant.participantOrigin,
                                  Participant.isGhostId,
                                  Participant.isTestParticipant,
                                  HPO.hpoId,
                                  HPO.name.label('hpo_name'),
                                  Organization.organizationId,
                                  Organization.displayName.label('organization_name'))\
                .select_from(ConsentFile).outerjoin(ConsentResponse)\
                .outerjoin(ConsentErrorReport, ConsentFile.id == ConsentErrorReport.consent_file_id)\
                .outerjoin(ParticipantSummary, ConsentFile.participant_id == ParticipantSummary.participantId)\
                .outerjoin(Participant, ConsentFile.participant_id == Participant.participantId)\
                .outerjoin(QuestionnaireResponse,
                           QuestionnaireResponse.questionnaireResponseId == ConsentResponse.questionnaire_response_id) \
                .outerjoin(HPO, HPO.hpoId == Participant.hpoId)\
                .outerjoin(Organization, Participant.organizationId == Organization.organizationId)

            if origin:
                query = query.filter(Participant.participantOrigin == origin)

            # List of ids takes precedence over date filter
            if id_list and len(id_list):
                query = query.filter(ConsentFile.id.in_(id_list))
            else:
                query = query.filter(ConsentFile.modified >= date_filter)

            if sync_statuses:
                query = query.filter(ConsentFile.sync_status.in_(sync_statuses))
            if consent_types:
                query = query.filter(ConsentFile.type.in_(consent_types))

            results = query.all()
            if not results:
                logging.debug('No consent metrics results matching filters were found.')

            return results

class ConsentErrorReportGenerator(ConsentMetricGenerator):
    """
    This class is used to build a text report from ConsentMetric data which can be emailed so it triggers automatic
    creation of a PTSC Service Desk ticket
    """

    def __init__(self):
        super(ConsentErrorReportGenerator, self).__init__()
        self.ro_dao = ResourceDataDao(backup=True)
        self.error_list = self._initialize_error_list()

    @staticmethod
    def _initialize_error_list():
        """
        Build a dict where the key is the consent error type and the value will contain a list of error
        reports (dicts) for that error type, if any are detected.  Initialize each to an empty list
        """
        error_dict = dict()
        for error_key in METRICS_ERROR_TYPES.keys():
            error_dict[error_key] = list()
        return error_dict

    @staticmethod
    def _output_to_file(fname, txt_lines):

        with open(fname, 'w') as f:
            f.writelines(txt_lines)

    @staticmethod
    def _resolve_email_addresses(to_list, cc_list, origin):
        """ Determine to and cc lists for consent error reports based on app config settings and participant origin """
        email_config = config.getSettingJson(config.PTSC_SERVICE_DESK_EMAIL, {})
        if origin == 'vibrent':
            to_list = to_list if isinstance(to_list, list) else email_config.get('recipients')
            cc_list = cc_list if isinstance(cc_list, list) else email_config.get('cc_recipients')
        # TODO:  For now, use the DRC recipient list (cc: list for PTSC emails) as the "to" list for CE-related reports
        # and blank out the cc: list
        elif origin == 'careevolution':
            to_list = to_list if isinstance(to_list, list) else email_config.get('cc_recipients')
            cc_list = []

        return to_list, cc_list

    @staticmethod
    def _generate_email_subject(error_type, error_reports, origin):
        """ Create the subject line text for a consent error report """
        # Subject line content and format suggested/agreed upon by PTSC
        # E.g.:  DRC Consent Validation Issue | PRIMARY, EHR | Missing signature
        #   or   DRC Consent Validation Issue | GROR | Checkbox not checked
        consent_types_in_error = set([e['Consent Type'] for e in error_reports])
        ce_origin = '(CE) ' if origin == 'careevolution' else ''
        subject_line = ' | '.join([f'{ce_origin}DRC Consent Validation Issue',
                                   ', '.join(consent_types_in_error),
                                   METRICS_ERROR_TYPES.get(error_type)])
        return subject_line

    @staticmethod
    def _generate_email_body(error_reports):
        """
        :param error_reports:  List of dicts containing error details field keys and text strings.  Each list element
                               represents an instance of a failed validation for one of the known error types
        :return:  String containing email body text
        """
        # The report body will have an entry/paragraph for each instance of the error; e.g. missing file entry:
        # Participant           P123456789
        # Consent Type          PRIMARY
        # Authored on           2021-10-10
        # Error Detected        Missing File
        body = ''
        for report in error_reports:
            # Format report details/dict items into two aligned columns (example above)
            err_txt = '\n'.join([f'{k:30}{v}' for k, v in report.items()])
            body += err_txt + '\n\n'

        return body

    @staticmethod
    def send_consent_error_email(subject, body, recipients=None, cc_list=None):
        """
        Send an email to the generic address that will trigger creation of a PTSC Service Desk ticket from the
        email content.
        :param subject:  A string in the expected format (agreed upon w/PTSC) summarizing the error condition
        :param body: Text/string (multi-line/multi-paragraph format agreed upon w/PTSC ) with details of each instance
                     of the detected error condition
        :param recipients: Destination email address list
        :param cc_list: List of cc email addresses
        """
        if recipients is None:
            logging.error('No recipient address list available for consent error email generation')
        elif not isinstance(recipients, list):
            raise ValueError("Consent error report recipients param is not in expected list format")
        else:
            email_obj = email_service.Email(subject, recipients=recipients,  cc_recipients=cc_list,
                                            from_email=None, plain_text_content=body)
            email_service.EmailService.send_email(email_obj)

        # Construct string w/pids from the body text and log indication that PTSC ticket should've been created
        # Use a set to filter duplicate occurrences of a pid in the same email body
        participants = set(re_findall(r"Participant\s+(P\d+)\s*", body))
        msg = '\n'.join([f'{subject} error detected for pids {",".join(participants)}',
                         'Please confirm successful PTSC SD ticket creation'])
        logging.warning(msg)

    def send_error_reports(self, output_file=None, recipients=None, cc_list=None, origin='vibrent'):
        """
        Loop through the results from create_error_reports() and send related emails or output all data to a file
        :param output_file:  File pathname for output, in lieu of sending emails.
        :param recipients:  List of email addresses to send report to, if overriding default config item
        :param cc_list:  List of cc email addresses, if overriding default config item
        :param origin: Participant origin (default is vibrent)
        """
        recipients, cc_list = self._resolve_email_addresses(recipients, cc_list, origin)

        # PTSC wants tickets identified by error type detected.  Each error type is a key in the error_list dict where
        # the value is a list of reports (dicts) with details about each detected instance of that error type
        report_lines = list()
        for err_type, error_reports in self.error_list.items():
            if not len(error_reports):
                # No errors of this err_type were detected
                continue

            subject_line = self._generate_email_subject(err_type, error_reports, origin)
            email_body = self._generate_email_body(error_reports)
            if output_file:
                report_lines.extend(['\n\nSubject: ', subject_line, '\n\n', email_body])
            # A separate email/ticket is generated for each detected error type (per PTSC request).
            else:
                self.send_consent_error_email(subject_line, email_body.rstrip(),
                                              recipients=recipients, cc_list=cc_list)
        if output_file:
            self._output_to_file(output_file, report_lines)

    def _generate_error_details(self, err_key, rec, rsc_data):
        """
        Generate dict with error report details from the consent validation results details
        :param err_key:   METRICS_ERROR_TYPES key value
        :param rec:  Result record from ConsentMetricGenerator.get_consent_validation_records()
        :param rsc_data: Result dict from ConsentMetricGenerator.make_resource()
        :returns: Dict of report fields and text strings used to populate a formatted error report
        """
        authored = self._get_authored_timestamps(rec)
        consent = ConsentType(rsc_data.get('consent_type'))
        error_details = {
            'Participant': rsc_data.get('participant_id'), 'Consent Type': str(consent),
            'Authored On': authored[consent].strftime("%Y-%m-%dT%H:%M:%S"),
            'Error Detected': METRICS_ERROR_TYPES[err_key], 'DRC Tracking ID': rec.id,
        }
        # All but 'missing file' reports will contain details on the file that failed validation
        if err_key != 'missing_file':
            error_details['File'] = rec.file_path or ''
            error_details['File Upload Time'] = \
                rec.file_upload_time.strftime("%Y-%m-%dT%H:%M:%S") if rec.file_upload_time else ''

        # Consent version errors:  take into account pairing history details
        if err_key in ['non_va_consent_for_va', 'va_consent_for_non_va']:
            pairing_at_consent = self.pairing_at_consent(rec.participant_id, authored[consent],
                                                         dao=self.ro_dao)
            current_pairing = rec.hpo_name or 'UNSET'
            error_details['Current Pairing'] = current_pairing
            error_details['Pairing at Consent'] = pairing_at_consent
            if err_key == 'non_va_consent_for_va':
                if pairing_at_consent == 'VA':
                    error_details['Notes'] = 'Incorrect version; was paired to VA at time of consent'
                elif current_pairing == 'VA':
                    error_details['Notes'] = 'May require re-consent using VA consent version'
            elif pairing_at_consent != 'VA':
                error_details['Notes'] = 'Incorrect version; was not paired to VA at time of consent'

        elif err_key == 'invalid_signing_date':
            error_details['Expected signing date'] = rec.expected_sign_date
            error_details['Signing date found'] = rec.signing_date

        # Per DA-2611, the invalid dob/age at consent error reports will only be generated if they are
        # associated with a consent that also had another PDF validation issue (NEEDS_CORRECTING status)
        elif err_key in ['invalid_dob', 'invalid_age_at_consent']:
            primary_consent_authored = authored[ConsentType.PRIMARY]
            error_details['Primary Consent Authored'] = primary_consent_authored
            dob = rec.dateOfBirth
            if err_key == 'invalid_dob':
                if not dob:
                    note_text = 'DOB value was missing from primary consent data'
                else:
                    # Invalid DOB means invalid year in the date object; don't include full DOB str (PII)
                    note_text = f'Provided DOB value contained invalid year {str(dob.year).zfill(4)}'
            else:
                age = self._calculate_age(dob, primary_consent_authored.date())
                note_text = f'Age at consent was {age} years based on provided DOB value'
            error_details['Notes'] = note_text

        return error_details

    def create_error_reports(self, id_list=None, to_file=None, recipients=None, cc_list=None,
                             participant_origin='vibrent'):
        """
        Generate consent error reports.  May be called as part of the daily consent validation cron job,
        or from the manual consent-error-report tool, which can optionally redirect output to file as a dry run or
        provide alternate email destination lists
        :param id_list: list of consent_file primary key id values
        :param participant_origin: Default is 'vibrent'
        :param to_file: File pathname if error reports are to be routed to output file instead of emailed.
        :param recipients: List of email address to send reports to, if overriding default config item
        :param cc_list: List of cc email addresses, if overriding default config item
        """
        if not isinstance(id_list, list):
            logging.info('No ids provided for consent error report creation')
            return

        reported_errors_for_db = list()
        needs_correcting_recs = self.get_consent_validation_records(id_list=id_list, origin=participant_origin)
        if not needs_correcting_recs:
            msg = f'No consent errors to report for provided ids and origin {participant_origin}\n'
            if to_file:
                self._output_to_file(to_file, msg)
            else:
                logging.info(msg)
            return
        for rec in needs_correcting_recs:
            # Skip records that already have an entry in the ConsentErrorReport table
            if rec.consent_error_report:
                logging.debug(f'Error report already sent for consent_file id {rec.id}')
                continue

            # ConsentMetric resource generator provides data dict used in reporting.
            rsc_data = self.make_resource(rec.id, consent_validation_rec=rec).get_data()
            if rsc_data.get('ignore', False) or rsc_data.get('test_participant', False):
                continue

            # Generate a report entry for any validation error that was detected for this consent
            for err_key in METRICS_ERROR_TYPES.keys():
                if rsc_data.get(err_key, False):
                    self.error_list[err_key].append(self._generate_error_details(err_key, rec, rsc_data))
                    # Unless output is being redirected to a file, add to list of records to insert into DB
                    if not to_file:
                        reported_errors_for_db.append(ConsentErrorReport(consent_file_id=rec.id,
                                                                         notes=METRICS_ERROR_TYPES[err_key]))

        self.send_error_reports(output_file=to_file, recipients=recipients, cc_list=cc_list,
                                origin=participant_origin)

        if len(reported_errors_for_db):
            dao = ConsentErrorReportDao()
            dao.batch_update_consent_error_reports(reported_errors_for_db)

    def get_unreported_error_ids(self):
        """
        Find all NEEDS_CORRECTING records from the consent_file table that do not have an existing record
        in the consent_error_report table (added for DA-2611).
        :returns: A list of consent_file id primary keys, or None
        """
        unreported_error_ids = None
        dao = self.ro_dao or ResourceDataDao()
        with dao.session() as session:
            # ~ any() construct will produce results where no related consent_error_report records exist
            # TODO:  Revise if/when permanent method for tracking outstanding DOB issues is implemented, update
            # query to combine those with the NEEDS_CORRECTING consent_file records
            query = session.query(ConsentFile.id).filter(ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING,
                                                         ~ConsentFile.consent_error_report.any())
            results = query.all()
            if results:
                unreported_error_ids = [r.id for r in results]

        return unreported_error_ids
