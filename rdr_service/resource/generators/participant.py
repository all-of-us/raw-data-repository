import datetime
import json
import logging
import re

from collections import OrderedDict
from dateutil import parser, tz
from dateutil.parser import ParserError
from sqlalchemy import func, desc, exc
from werkzeug.exceptions import NotFound

from rdr_service import config
from rdr_service.resource.helpers import DateCollection
from rdr_service.code_constants import (
    CONSENT_GROR_YES_CODE,
    CONSENT_PERMISSION_YES_CODE,
    CONSENT_PERMISSION_NO_CODE,
    DVEHR_SHARING_QUESTION_CODE,
    EHR_CONSENT_QUESTION_CODE,
    DVEHRSHARING_CONSENT_CODE_YES,
    GROR_CONSENT_QUESTION_CODE,
    EHR_CONSENT_EXPIRED_YES,
    CONSENT_COPE_YES_CODE,
    CONSENT_COPE_NO_CODE,
    CONSENT_COPE_DEFERRED_CODE,
    CABOR_SIGNATURE_QUESTION_CODE,
    PMI_SKIP_CODE
)
from rdr_service.dao.resource_dao import ResourceDataDao
# TODO: Replace BQRecord here with a Resource alternative.
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_participant_summary import BQStreetAddressTypeEnum, \
    BQModuleStatusEnum, COHORT_1_CUTOFF, COHORT_2_CUTOFF, BQConsentCohort
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import PhysicalMeasurements, PhysicalMeasurementsStatus
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_cohort_pilot import ParticipantCohortPilot
# TODO:  Using participant_summary as a workaround.  Replace with new participant_profile when it's available
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.participant_enums import EnrollmentStatusV2, WithdrawalStatus, WithdrawalReason, SuspensionStatus, \
    SampleStatus, BiobankOrderStatus, PatientStatusFlag, ParticipantCohortPilotFlag, EhrStatus, DeceasedStatus, \
    DeceasedReportStatus, QuestionnaireResponseStatus
from rdr_service.resource import generators, schemas
from rdr_service.resource.constants import SchemaID

_consent_module_question_map = {
    # module: question code string
    'ConsentPII': None,
    'DVEHRSharing': 'DVEHRSharing_AreYouInterested',
    'EHRConsentPII': 'EHRConsentPII_ConsentPermission',
    'GROR': 'ResultsConsent_CheckDNA',
    'PrimaryConsentUpdate': 'Reconsent_ReviewConsentAgree',
    'ProgramUpdate': None,
    'COPE': 'section_participation',
    'cope_nov': 'section_participation',
    'cope_dec': 'section_participation',
    'GeneticAncestry': 'GeneticAncestry_ConsentAncestryTraits'
}

# _consent_expired_question_map must contain every module ID from _consent_module_question_map.
_consent_expired_question_map = {
    'ConsentPII': None,
    'DVEHRSharing': None,
    'EHRConsentPII': 'EHRConsentPII_ConsentExpired',
    'GROR': None,
    'PrimaryConsentUpdate': None,
    'ProgramUpdate': None,
    'COPE': None,
    'cope_nov': None,
    'cope_dec': None,
    'GeneticAncestry': None
}

# Possible answer codes for the consent module questions and what submittal status the answers correspond to
_consent_answer_status_map = {
    'ConsentPermission_Yes': BQModuleStatusEnum.SUBMITTED,
    'ConsentPermission_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'DVEHRSharing_Yes': BQModuleStatusEnum.SUBMITTED,
    'DVEHRSharing_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'DVEHRSharing_NotSure': BQModuleStatusEnum.SUBMITTED_NOT_SURE,
    'CheckDNA_Yes': BQModuleStatusEnum.SUBMITTED,
    'CheckDNA_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'CheckDNA_NotSure': BQModuleStatusEnum.SUBMITTED_NOT_SURE,
    'ReviewConsentAgree_Yes': BQModuleStatusEnum.SUBMITTED,
    'ReviewConsentAgree_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    # COPE_A_44
    CONSENT_COPE_YES_CODE: BQModuleStatusEnum.SUBMITTED,
    # COPE_A_13
    CONSENT_COPE_NO_CODE: BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    # COPE_A_231
    CONSENT_COPE_DEFERRED_CODE: BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'ConsentAncestryTraits_Yes': BQModuleStatusEnum.SUBMITTED,
    'ConsentAncestryTraits_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'ConsentAncestryTraits_NotSure': BQModuleStatusEnum.SUBMITTED_NOT_SURE
}

# See hotfix ticket ROC-447 / backfill ticket ROC-475.  The first GROR consent questionnaire was immediately
# deprecated and replaced by a revised consent questionnaire.  Early GROR consents (~200) already received
# were replayed using the revised questionnaire format.  When retrieving GROR module answers, we'll look for
# deprecated GROR consent questions/answers to map them to the revised consent question/answer format.
_deprecated_gror_consent_questionnaire_id = 415
_deprecated_gror_consent_question_code_names = ('CheckDNA_Yes', 'CheckDNA_No', 'CheckDNA_NotSure')

class ParticipantSummaryGenerator(generators.BaseGenerator):
    """
    Generate a Participant Summary Resource object
    """

    # Temporary
    cdm_db_exists = False

    ro_dao = None
    # Retrieve module and sample test lists from config.
    _baseline_modules = [mod.replace('questionnaireOn', '')
                         for mod in config.getSettingList('baseline_ppi_questionnaire_fields')]
    _baseline_sample_test_codes = config.getSettingList('baseline_sample_test_codes')
    _dna_sample_test_codes = config.getSettingList('dna_sample_test_codes')

    def make_resource(self, p_id):
        """
        Build a Participant Summary Resource object for the given participant id.
        :param p_id: Participant ID
        :return: ResourceDataObject object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=True)

        with self.ro_dao.session() as ro_session:
            # Temporary
            self.cdm_db_exists = self._test_cdm_db_exists(ro_session)
            # prep participant info from Participant record
            summary = self._prep_participant(p_id, ro_session)
            # prep additional participant profile info
            summary = self._merge_schema_dicts(summary, self._prep_participant_profile(p_id, ro_session))
            # prep ConsentPII questionnaire information
            summary = self._merge_schema_dicts(summary, self._prep_consentpii_answers(p_id))
            # prep questionnaire modules information, includes gathering extra consents.
            summary = self._merge_schema_dicts(summary, self._prep_modules(p_id, ro_session))
            # prep physical measurements
            summary = self._merge_schema_dicts(summary, self._prep_physical_measurements(p_id, ro_session))
            # prep race and gender
            summary = self._merge_schema_dicts(summary, self._prep_the_basics(p_id, ro_session))
            # prep biobank orders and samples
            summary = self._merge_schema_dicts(summary, self._prep_biobank_info(p_id, summary['biobank_id'],
                                                                                ro_session))
            # prep patient status history
            summary = self._merge_schema_dicts(summary, self._prep_patient_status_info(p_id, ro_session))
            # calculate enrollment status for participant
            summary = self._merge_schema_dicts(summary, self._calculate_enrollment_status(summary))
            # # Depreciated for now: calculate enrollment status times
            # summary = self._merge_schema_dicts(summary, self._calculate_enrollment_timestamps(summary))
            # calculate distinct visits
            summary = self._merge_schema_dicts(summary, self._calculate_distinct_visits(summary))
            # calculate test participant status (if it was not already set by _prep_participant() )
            # TODO:  If a backfill for the DA-1800 Participant.isTestParticipant field is done, we may be able to
            # remove this call/method entirely and rely solely on value assigned by _prep_participant()
            if summary['test_participant'] == 0:
                summary = self._merge_schema_dicts(summary, self._calculate_test_participant(summary))

            # data = self.ro_dao.to_resource_dict(summary, schema=schemas.ParticipantSchema)

            return generators.ResourceRecordSet(schemas.ParticipantSummarySchema, summary)

    def patch_resource(self, p_id, data):
        """
        Upsert data into an existing resource.  Warning: No data recalculation is performed in this method.
        Note: This method uses the MySQL JSON_SET function to update the resource field in the backend.
              It does not return the full resource record here.
        https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-set
        :param p_id: participant id
        :param data: dict object
        :return: dict
        """
        sql_json_set_values = ', '.join([f"'$.{k}', :p_{k}" for k, v in data.items()])

        args = {'pid': p_id, 'type_uid': SchemaID.participant.value, 'modified': datetime.datetime.utcnow()}
        for k, v in data.items():
            args[f'p_{k}'] = v

        sql = f"""
            update resource_data rd inner join resource_type rt on rd.resource_type_id = rt.id
              set rd.modified = :modified, rd.resource = json_set(rd.resource, {sql_json_set_values})
              where rd.resource_pk_id = :pid and rt.type_uid = :type_uid
        """
        dao = ResourceDataDao(backup=False)
        with dao.session() as session:
            session.execute(sql, args)

            sql = """
                select resource from resource_data rd inner join resource_type rt on rd.resource_type_id = rt.id
                 where rd.resource_pk_id = :pid and rt.type_uid = :type_uid limit 1"""

            rec = session.execute(sql, args).first()
            if rec:
                summary = json.loads(rec.resource)
                return generators.ResourceRecordSet(schemas.ParticipantSchema, summary)

        return None

    def _test_cdm_db_exists(self, ro_session):
        """
        Temporary function to detect if the 'cdm' database and 'tmp_questionnaire_response' table exists.
        :param ro_session: Readonly DAO session object
        :return: True if 'cdm' database exists otherwise False.
        """
        sql = "SELECT * FROM cdm.tmp_questionnaire_response LIMIT 1"
        try:
            ro_session.execute(sql)
            return True
        except exc.ProgrammingError:
            pass
        except exc.OperationalError:
            msg = 'Unexpected error found when checking for tmp_questionnaire_response table'
            logging.warning(msg, exc_info=False)
        return False

    def _prep_participant(self, p_id, ro_session):
        """
        Get the information from the participant record
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        # Note: We need to be careful here, there is a delay from when a participant is inserted in the primary DB
        # and when it shows up in the replica DB instance.
        p: Participant = ro_session.query(Participant).filter(Participant.participantId == p_id).first()
        if not p:
            msg = f'Participant lookup for P{p_id} failed.'
            logging.error(msg)
            raise NotFound(msg)

        hpo = ro_session.query(HPO.name).filter(HPO.hpoId == p.hpoId).first()
        organization = ro_session.query(Organization.externalId). \
            filter(Organization.organizationId == p.organizationId).first()

        # See DeceasedReportDao._update_participant_summary() for the logic for populating
        # the deceased status details in participant_summary.  DENIED reports are not reflected in
        # participant_summary, only PENDING and APPROVED.  Also, when records are inserted into the deceased_report
        # table there is a check to ensure each participant only has one report that is PENDING or APPROVED
        deceased = ro_session.query(DeceasedReport) \
            .filter(DeceasedReport.participantId == p_id,
                    DeceasedReport.status != DeceasedReportStatus.DENIED).one_or_none()
        if deceased:
            deceased_status = DeceasedStatus(str(deceased.status))
            deceased_authored = deceased.reviewed if deceased_status == DeceasedStatus.APPROVED else deceased.authored
            deceased_date_of_death = deceased.dateOfDeath
        else:
            deceased_status = DeceasedStatus.UNSET
            deceased_authored = None
            deceased_date_of_death = None

        withdrawal_status = WithdrawalStatus(p.withdrawalStatus)
        withdrawal_reason = WithdrawalReason(p.withdrawalReason if p.withdrawalReason else 0)
        suspension_status = SuspensionStatus(p.suspensionStatus)

        # The cohort_2_pilot_flag field values in participant_summary were set via a one-time backfill based on a
        # list of participant IDs provided by PTSC and archived in the participant_cohort_pilot table.  See:
        # https://precisionmedicineinitiative.atlassian.net/browse/DA-1622
        # TODO:  A participant_profile table may be implemented as part of the effort to eliminate dependencies on
        # participant_summary.  The cohort_2_pilot_flag could be moved into _prep_participant_profile() in the future
        #
        # Note this query assumes participant_cohort_pilot only contains entries for the cohort 2 pilot
        # participants for genomics and has not been used for identifying participants in more recent pilots
        cohort_2_pilot = ro_session.query(ParticipantCohortPilot.participantCohortPilot). \
            filter(ParticipantCohortPilot.participantId == p_id).first()

        cohort_2_pilot_flag = \
            ParticipantCohortPilotFlag.COHORT_2_PILOT if cohort_2_pilot else ParticipantCohortPilotFlag.UNSET
        data = {
            'participant_id': f'P{p_id}',
            'biobank_id': p.biobankId,
            'research_id': p.researchId,
            'participant_origin': p.participantOrigin,
            'last_modified': p.lastModified,
            'sign_up_time': p.signUpTime,
            'hpo': hpo.name if hpo else None,
            'hpo_id': p.hpoId,
            'organization': organization.externalId if organization else None,
            'organization_id': p.organizationId,

            'withdrawal_status': str(withdrawal_status),
            'withdrawal_status_id': int(withdrawal_status),
            'withdrawal_reason': str(withdrawal_reason),
            'withdrawal_reason_id': int(withdrawal_reason),
            'withdrawal_time': p.withdrawalTime,
            'withdrawal_authored': p.withdrawalAuthored,
            'withdrawal_reason_justification': p.withdrawalReasonJustification,

            'suspension_status': str(suspension_status),
            'suspension_status_id': int(suspension_status),
            'suspension_time': p.suspensionTime,

            'site': self._lookup_site_name(p.siteId, ro_session),
            'site_id': p.siteId,
            'is_ghost_id': 1 if p.isGhostId is True else 0,
            'test_participant': 1 if p.isTestParticipant else 0,
            'cohort_2_pilot_flag': str(cohort_2_pilot_flag),
            'cohort_2_pilot_flag_id': int(cohort_2_pilot_flag),
            'deceased_status': str(deceased_status),
            'deceased_status_id': int(deceased_status),
            'deceased_authored': deceased_authored,
            # TODO:  Enable this field definition in the BQ model if it's determined it should be included in PDR
            'date_of_death': deceased_date_of_death
        }

        return data

    def _prep_participant_profile(self, p_id, ro_session):
        """
        Get additional participant status fields that were incorporated into the RDR participant_summary
        but can't be derived from other RDR tables.  Example is EHR status information which is
        read from a curation dataset by a daily cron job that then applies updates to RDR participant_summary directly.

        PDR-166:
        As of DA-1781/RDR 1.83.1, a participant_ehr_receipt table was implemented in RDR to track
        when EHR files are received/"seen" for a participant.   See the technical design (DA-1780)
        The PDR data will mirror the revised RDR EHR status fields.  Note:  There is still a dependency on
        participant_summary for old EHR receipt timestamps that predated the creation of participant_ehr_receipt
        TODO:  May be able to get the old EHR timestamp data from bigquery_sync / resource data instead

        :param p_id: participant_id
        :return: dict

        """
        # TODO: Workaround for PDR-106 is to pull needed EHR fields from participant_summary. LIMITED USE CASE ONLY
        # Goal is to eliminate dependencies on participant_summary, which may go away someday.
        # Long term solution may mean creating a participant_profile table for these outlier fields that are managed
        # outside of the RDR API, and query that table instead.
        data = {}
        ps = ro_session.query(ParticipantSummary.ehrStatus, ParticipantSummary.ehrReceiptTime,
                              ParticipantSummary.ehrUpdateTime,
                              ParticipantSummary.enrollmentStatusCoreOrderedSampleTime,
                              ParticipantSummary.enrollmentStatusCoreStoredSampleTime,
                              ParticipantSummary.isEhrDataAvailable) \
            .filter(ParticipantSummary.participantId == p_id).first()

        if not ps:
            logging.debug(f'No participant_summary record found for {p_id}')
        else:
            # SqlAlchemy may return None for our zero-based NOT_PRESENT EhrStatus Enum, so map None to NOT_PRESENT
            # See rdr_service.model.utils Enum decorator class
            ehr_status = EhrStatus.NOT_PRESENT if ps.ehrStatus is None else ps.ehrStatus
            ehr_receipts = []
            data = {
                'ehr_status': str(ehr_status),
                'ehr_status_id': int(ehr_status),
                'ehr_receipt': ps.ehrReceiptTime,
                'ehr_update': ps.ehrUpdateTime,
                'enrollment_core_ordered': ps.enrollmentStatusCoreOrderedSampleTime,
                'enrollment_core_stored': ps.enrollmentStatusCoreStoredSampleTime,
                # New alias field added in RDR 1.83.1 for DA-1781.  Add for PDR/RDR consistency, retain old ehr_status
                # (and ehr_status_id) fields for PDR backwards compatibility
                'was_ehr_data_available': int(ehr_status),
                # Brand new field as of RDR 1.83.1/DA-1781; convert boolean to integer for our BQ data dict
                'is_ehr_data_available': int(ps.isEhrDataAvailable)
            }
            # Note:  None of the columns in the participant_ehr_receipt table are nullable
            pehr_results = ro_session.query(ParticipantEhrReceipt.id,
                                            ParticipantEhrReceipt.fileTimestamp,
                                            ParticipantEhrReceipt.firstSeen,
                                            ParticipantEhrReceipt.lastSeen
                                            ) \
                .filter(ParticipantEhrReceipt.participantId == p_id) \
                .order_by(ParticipantEhrReceipt.firstSeen,
                          ParticipantEhrReceipt.fileTimestamp).all()

            if len(pehr_results):
                for row in pehr_results:
                    # This pid's earliest EHR receipt timestamp may have predated implementation of the
                    # participant_ehr_receipt table, and may only exist in the participant_summary data retrieved above
                    data['ehr_receipt'] = min(row.fileTimestamp, data['ehr_receipt'] or datetime.datetime.max)
                    data['ehr_update'] = max(row.fileTimestamp, data['ehr_update'] or datetime.datetime.min)

                    ehr_receipts.append({
                        'participant_ehr_receipt_id': row.id,
                        'file_timestamp': row.fileTimestamp,
                        'first_seen': row.firstSeen,
                        'last_seen': row.lastSeen}
                    )

            # More field aliases for deprecated fields, as of RDR 1.83.1/DA-1781 (see tech design/DA-1780).
            # The old fields/keys are still populated for PDR/metrics backwards compatibility
            data['first_ehr_receipt_time'] = data['ehr_receipt']
            data['latest_ehr_receipt_time'] = data['ehr_update']

            if len(ehr_receipts):
                data['ehr_receipts'] = ehr_receipts

        return data

    def _prep_consentpii_answers(self, p_id):
        """
        Get participant information from the ConsentPII questionnaire
        :param p_id: participant id
        :return: dict
        """

        # PDR-178:  Retrieve both the processed (layered) answers result, and the raw responses. This allows us to
        # do some extra processing of the ConsentPII data without having to query all over again.
        qnans, responses = self.get_module_answers(self.ro_dao, 'ConsentPII', p_id, return_responses=True,
                                                   cdm_db_exists=self.cdm_db_exists)
        if not qnans:
            # return the minimum data required when we don't have the questionnaire data.
            return {'email': None, 'is_ghost_id': 0}

        qnan = BQRecord(schema=None, data=qnans)  # use only most recent response.

        try:
            # Value can be None, 'PMISkip' or date string.
            dob = parser.parse(qnan.get('PIIBirthInformation_BirthDate')).date()
        except (ParserError, TypeError):
            dob = None

        data = {
            'first_name': qnan.get('PIIName_First'),
            'middle_name': qnan.get('PIIName_Middle'),
            'last_name': qnan.get('PIIName_Last'),
            'date_of_birth': dob,
            'primary_language': qnan.get('language'),
            'email': qnan.get('ConsentPII_EmailAddress'),
            'phone_number': qnan.get('PIIContactInformation_Phone'),
            'login_phone_number': qnan.get('ConsentPII_VerifiedPrimaryPhoneNumber'),
            'addresses': [
                {
                    'addr_type': BQStreetAddressTypeEnum.RESIDENCE.name,
                    'addr_type_id': BQStreetAddressTypeEnum.RESIDENCE.value,
                    'addr_street_address_1': qnan.get('PIIAddress_StreetAddress'),
                    'addr_street_address_2': qnan.get('PIIAddress_StreetAddress2'),
                    'addr_city': qnan.get('StreetAddress_PIICity'),
                    'addr_state': qnan.get('StreetAddress_PIIState', '').replace('PIIState_', '').upper(),
                    'addr_zip': qnan.get('StreetAddress_PIIZIP'),
                    'addr_country': 'US'
                }
            ],
            'cabor_authored': None
        }

        # PDR-178:  RDR handles CABoR consents a little differently in that once it receives an initial CABoR
        # "signature" in a ConsentPII, it sets the participant_summary.consent_for_cabor / consent_for_cabor_authored
        # fields and will never update them based on subsequent ConsentPII payloads.  To match RDR in determining
        # the CABoR consent authored date, we can't rely on "layered" answers returned by get_module_answers().
        # Instead we'll go back through the raw response dict of dicts; top level key is a questionnaire_response_id,
        # its value is a dict of key/value pairs (metadata and question codes/answers) associated with the response
        # Search for the first response with a signed CABoR (if one exists) and use that response's authored date
        for response_id_key in responses:
            fields = responses.get(response_id_key)
            if fields.get(CABOR_SIGNATURE_QUESTION_CODE, PMI_SKIP_CODE) != PMI_SKIP_CODE:
                data['cabor_authored'] = fields.get('authored')
                break

        return data

    def _prep_modules(self, p_id, ro_session):
        """
        Find all questionnaire modules the participant has completed and loop through them.
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """

        if not self.cdm_db_exists:
            code_id_query = ro_session.query(func.max(QuestionnaireConcept.codeId)). \
                filter(QuestionnaireResponse.questionnaireId ==
                       QuestionnaireConcept.questionnaireId).label('codeId')

            # Responses are sorted by authored date ascending and then created date descending
            # This should result in a list where any replays of a response are adjacent (most recently created first)
            query = ro_session.query(
                    QuestionnaireResponse.questionnaireResponseId, QuestionnaireResponse.authored,
                    QuestionnaireResponse.created, QuestionnaireResponse.language, QuestionnaireHistory.externalId,
                    QuestionnaireResponse.status, code_id_query). \
                join(QuestionnaireHistory). \
                filter(QuestionnaireResponse.participantId == p_id). \
                order_by(QuestionnaireResponse.authored, QuestionnaireResponse.created.desc())
            # sql = self.ro_dao.query_to_text(query)
            results = query.all()

        # Temporary: Use raw sql instead to facilitate joining across databases to the cdm.tmp_questionnaire_response
        # table where we have flagged duplicate response payloads sent by PTSC for filtering out during PDR data
        # rebuilds.  See:  ROC-825
        else:
            # This SQL statement is based off of the query_to_text() output for the SQLAlchemy query above, and then
            # modified to add the additional filtering when joined with cdm.temp_questionnaire_response (and match
            # expected column names to what the SQLAlchemy query also returns)
            _raw_sql = """
                 SELECT qr.questionnaire_response_id questionnaireResponseId,
                        qr.authored,
                        qr.created,
                        qr.language,
                        qh.external_id externalId,
                        qr.status,
                   (SELECT max(questionnaire_concept.code_id) AS max_1
                    FROM questionnaire_concept
                    WHERE qr.questionnaire_id = questionnaire_concept.questionnaire_id) AS `codeId`
                 FROM questionnaire_response qr
                 INNER JOIN questionnaire_history qh
                       ON qh.questionnaire_id = qr.questionnaire_id
                       AND qh.version = qr.questionnaire_version
                 LEFT OUTER JOIN cdm.tmp_questionnaire_response tqr
                       ON qr.questionnaire_response_id = tqr.questionnaire_response_id
                 WHERE qr.participant_id = :pid
                       AND (tqr.duplicate is NULL or tqr.duplicate = 0)
                 ORDER BY qr.authored, qr.created DESC;
             """
            results = ro_session.execute(_raw_sql, {'pid': p_id})

        data = {
            'consent_cohort': BQConsentCohort.UNSET.name,
            'consent_cohort_id': BQConsentCohort.UNSET.value
        }
        modules = list()
        consents = list()
        consent_dt = None

        if results:
            # Track the last module/consent data dictionaries generated, so we can detect and omit replayed responses
            last_mod_processed = {}
            last_consent_processed = {}
            for row in results:
                consent_added = False
                module_name = self._lookup_code_value(row.codeId, ro_session)
                # Start with a default submittal status.  May be updated if this is a consent module with a specific
                # consent question/answer that determines module submittal status
                module_status = BQModuleStatusEnum.SUBMITTED
                module_data = {
                    'module': module_name,
                    'baseline_module': 1 if module_name in self._baseline_modules else 0,  # Boolean field
                    'module_authored': row.authored,
                    'module_created': row.created,
                    'language': row.language,
                    'status': module_status.name,
                    'status_id': module_status.value,
                    'external_id': row.externalId,
                    'response_status': str(QuestionnaireResponseStatus(row.status)),
                    'response_status_id': int(QuestionnaireResponseStatus(row.status))
                }

                # check if this is a module with consents.
                if module_name in _consent_module_question_map:
                    # Calculate Consent Cohort from ConsentPII authored
                    if consent_dt is None and module_name == 'ConsentPII' and row.authored:
                        consent_dt = row.authored
                        if consent_dt < COHORT_1_CUTOFF:
                            cohort = BQConsentCohort.COHORT_1
                        elif COHORT_1_CUTOFF <= consent_dt <= COHORT_2_CUTOFF:
                            cohort = BQConsentCohort.COHORT_2
                        else:
                            cohort = BQConsentCohort.COHORT_3
                        data['consent_cohort'] = cohort.name
                        data['consent_cohort_id'] = cohort.value

                    qnans = self.get_module_answers(self.ro_dao, module_name, p_id, row.questionnaireResponseId)
                    if qnans:
                        qnan = BQRecord(schema=None, data=qnans)  # use only most recent questionnaire.
                        consent = {
                            'consent': _consent_module_question_map[module_name],
                            'consent_id': self._lookup_code_id(_consent_module_question_map[module_name], ro_session),
                            'consent_date': parser.parse(qnan['authored']).date() if qnan['authored'] else None,
                            'consent_module': module_name,
                            'consent_module_authored': row.authored,
                            'consent_module_created': row.created,
                            'consent_module_external_id': row.externalId,
                            'consent_response_status': str(QuestionnaireResponseStatus(row.status)),
                            'consent_response_status_id': int(QuestionnaireResponseStatus(row.status))
                        }
                        # Note:  Based on currently available modules when a module has no
                        # associated answer options (like ConsentPII or ProgramUpdate), any submitted response is given
                        # an implicit ConsentPermission_Yes value.   May need adjusting if there are ever modules where
                        # that may no longer be true
                        if _consent_module_question_map[module_name] is None:
                            consent['consent'] = module_name
                            consent['consent_id'] = self._lookup_code_id(module_name, ro_session)
                            consent['consent_value'] = 'ConsentPermission_Yes'
                            consent['consent_value_id'] = self._lookup_code_id('ConsentPermission_Yes', ro_session)
                        else:
                            consent_value = qnan.get(_consent_module_question_map[module_name], None)
                            consent['consent_value'] = consent_value
                            consent['consent_value_id'] = self._lookup_code_id(consent_value, ro_session)
                            consent['consent_expired'] = \
                                qnan.get(_consent_expired_question_map[module_name] or 'None', None)

                            # Note: Currently, consent_expired only applies for EHRConsentPII.  Expired EHR consent
                            # (EHRConsentPII_ConsentExpired_Yes) is always accompanied by consent_value
                            # ConsentPermission_No so we can rely on consent_value alone to map the module_status
                            module_status = _consent_answer_status_map.get(consent_value, None)
                            if not module_status:
                                # This seems the appropriate status for cases where there wasn't any recognized
                                #  consent answer found in the payload (not even PMI_SKIP, which maps to UNSET)
                                module_status = BQModuleStatusEnum.SUBMITTED_INVALID
                                logging.warning("""
                                    No consent answer for module {0}.  Defaulting status to SUBMITTED_INVALID
                                    (pid {1}, response {2})
                                """.format(module_name, p_id, row.questionnaireResponseId))

                        module_data['status'] = module_status.name
                        module_data['status_id'] = module_status.value

                        # Compare against the last consent response processed to filter replays/duplicates
                        if not self.is_replay(last_consent_processed, consent, created_key='consent_module_created'):
                            consents.append(consent)
                            consent_added = True

                        last_consent_processed = consent.copy()

                # consent_added == True means we already know it wasn't a replayed consent
                if consent_added or not self.is_replay(last_mod_processed, module_data, created_key='module_created'):
                    modules.append(module_data)

                last_mod_processed = module_data.copy()


        if len(modules) > 0:
            # remove any duplicate modules and consents because of replayed responses.
            data['modules'] = [dict(t) for t in {tuple(d.items()) for d in modules}]
            if len(consents) > 0:
                data['consents'] = [dict(t) for t in {tuple(d.items()) for d in consents}]
                # Fall back to 'created' if there are None values in 'authored'.
                try:
                    data['consents'].sort(key=lambda consent_data: consent_data['consent_module_authored'],
                                          reverse=True)
                except TypeError:
                    data['consents'].sort(key=lambda consent_data: consent_data['consent_module_created'],
                                          reverse=True)
        return data

    def _prep_the_basics(self, p_id, ro_session):
        """
        Get the participant's race and gender selections
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        qnans = self.get_module_answers(self.ro_dao, 'TheBasics', p_id, return_responses=False,
                                        cdm_db_exists=self.cdm_db_exists)
        if not qnans or len(qnans) == 0:
            return {}

        # get TheBasics questionnaire response answers
        qnan = BQRecord(schema=None, data=qnans)  # use only most recent questionnaire.
        data = {}
        if qnan.get('Race_WhatRaceEthnicity'):
            rl = list()
            for val in qnan.get('Race_WhatRaceEthnicity').split(','):
                rl.append({'race': val, 'race_id': self._lookup_code_id(val, ro_session)})
            data['races'] = rl
        # get gender question answers
        gl = list()
        if qnan.get('Gender_GenderIdentity'):
            for val in qnan.get('Gender_GenderIdentity').split(','):
                if val == 'GenderIdentity_AdditionalOptions':
                    continue
                gl.append({'gender': val, 'gender_id': self._lookup_code_id(val, ro_session)})
        # get additional gender answers, if any.
        if qnan.get('GenderIdentity_SexualityCloserDescription'):
            for val in qnan.get('GenderIdentity_SexualityCloserDescription').split(','):
                gl.append({'gender': val, 'gender_id': self._lookup_code_id(val, ro_session)})

        if len(gl) > 0:
            data['genders'] = gl

        data['education'] = qnan.get('EducationLevel_HighestGrade')
        data['education_id'] = self._lookup_code_id(qnan.get('EducationLevel_HighestGrade'), ro_session)
        data['income'] = qnan.get('Income_AnnualIncome')
        data['income_id'] = self._lookup_code_id(qnan.get('Income_AnnualIncome'), ro_session)
        data['sex'] = qnan.get('BiologicalSexAtBirth_SexAtBirth')
        data['sex_id'] = self._lookup_code_id(qnan.get('BiologicalSexAtBirth_SexAtBirth'), ro_session)
        data['sexual_orientation'] = qnan.get('TheBasics_SexualOrientation')
        data['sexual_orientation_id'] = self._lookup_code_id(qnan.get('TheBasics_SexualOrientation'), ro_session)

        return data

    def _prep_physical_measurements(self, p_id, ro_session):
        """
        Get participant's physical measurements information
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        data = {}
        pm_list = list()

        query = ro_session.query(PhysicalMeasurements.physicalMeasurementsId, PhysicalMeasurements.created,
                                 PhysicalMeasurements.createdSiteId, PhysicalMeasurements.final,
                                 PhysicalMeasurements.finalized, PhysicalMeasurements.finalizedSiteId,
                                 PhysicalMeasurements.status). \
            filter(PhysicalMeasurements.participantId == p_id). \
            order_by(desc(PhysicalMeasurements.created))
        # sql = self.dao.query_to_text(query)
        results = query.all()

        for row in results:

            if row.final == 1 and row.status != PhysicalMeasurementsStatus.CANCELLED:
                pm_status = PhysicalMeasurementsStatus.COMPLETED
            else:
                pm_status = PhysicalMeasurementsStatus(row.status) if row.status else PhysicalMeasurementsStatus.UNSET

            pm_list.append({
                'physical_measurement_id': row.physicalMeasurementsId,
                'status': str(pm_status),
                'status_id': int(pm_status),
                'created': row.created,
                'created_site': self._lookup_site_name(row.createdSiteId, ro_session),
                'created_site_id': row.createdSiteId,
                'finalized': row.finalized,
                'finalized_site': self._lookup_site_name(row.finalizedSiteId, ro_session),
                'finalized_site_id': row.finalizedSiteId,
            })

        if len(pm_list) > 0:
            data['pm'] = pm_list
        return data

    def _prep_biobank_info(self, p_id, p_bb_id, ro_session):
        """
        Look up biobank orders / ordered samples / stored samples
        This was refactored during implementation of PDR-122, to add additional biobank details to PDR
        :param p_id: participant id
        :param p_bb_id:  participant's biobank id
        :param ro_session: Readonly DAO session object
        :return:
        """

        def _get_stored_sample_row(stored_samples, ordered_sample):
            """
            Search a list of biobank_stored_sample rows to find a match to the biobank_ordered_sample record
            (same test and order identifier)
            :param stored_samples: list of biobank_stored_sample rows
            :param ordered_sample: a biobank_ordered_sample row
            :return:
            """
            match = None
            for sample in stored_samples:
                if sample.test == ordered_sample.test and sample.biobank_order_id == ordered_sample.order_id:
                    match = sample
                    break

            return match

        def _make_sample_dict_from_row(bss=None, bos=None):
            """"
            Internal helper routine to populate a sample dict entry from the available ordered sample and
            stored sample information.
            :param bss:   A biobank_stored_sample row
            :param bos:   A biobank_ordered_sample row
            Note that there should never be an instance where neither parameter has content
            """

            # When a stored sample row is provided, use its confirmed and status fields
            if bss:
                test = bss.test
                stored_confirmed = bss.confirmed
                stored_status = bss.status
            elif bos:
                test = bos.test
                stored_confirmed = None
                stored_status = None
            else:
                # Should never get here, but don't throw an error if something went wrong;  let the generator continue
                logging.error(f'No stored or ordered sample info provided for biobank id {p_bb_id}. Please investigate')
                return {}

            return {
                'test': test,
                'baseline_test': 1 if test in self._baseline_sample_test_codes else 0,  # Boolean field
                'dna_test': 1 if test in self._dna_sample_test_codes else 0,  # Boolean field
                'confirmed': stored_confirmed,
                'status': str(SampleStatus.RECEIVED) if stored_confirmed else None,
                'status_id': int(SampleStatus.RECEIVED) if stored_confirmed else None,
                'collected': bos.collected if bos else None,
                'processed': bos.processed if bos else None,
                'finalized': bos.finalized if bos else None,
                'created': bss.created if bss else None,
                'disposed': bss.disposed if bss else None,
                'disposed_reason': str(SampleStatus(stored_status)) if stored_status else None,
                'disposed_reason_id': int(SampleStatus(stored_status)) if stored_status else None,
            }

        # SQL to generate a list of biobank orders associated with a participant
        _biobank_orders_sql = """
           select bo.biobank_order_id, bo.created, bo.order_status,
                   bo.collected_site_id, (select google_group from site where site.site_id = bo.collected_site_id) as collected_site,
                   bo.processed_site_id, (select google_group from site where site.site_id = bo.processed_site_id) as processed_site,
                   bo.finalized_site_id, (select google_group from site where site.site_id = bo.finalized_site_id) as finalized_site,
                   case when exists (
                     select bmko.participant_id
                     from biobank_mail_kit_order bmko
                     where bmko.biobank_order_id = bo.biobank_order_id
                        and bo.participant_id = bmko.participant_id
                        and bmko.associated_hpo_id is null
                   )
                   then 1 else 0 end as dv_order
             from biobank_order bo where participant_id = :p_id
             order by bo.created desc;
         """

        # SQL to collect all the ordered samples associated with a biobank order
        _biobank_ordered_samples_sql = """
            select bo.biobank_order_id, bos.*
            from biobank_order bo
            inner join biobank_ordered_sample bos on bo.biobank_order_id = bos.order_id
            where bo.participant_id = :p_id and bo.biobank_order_id = :bo_id
            order by bos.order_id, test;
        """

        # SQL to select all the stored samples associated with a participant's biobank_id
        # This may include stored samples for which we don't have an associated biobank order
        # See: https://precisionmedicineinitiative.atlassian.net/browse/PDR-89.
        _biobank_stored_samples_sql = """
            select
                (select distinct boi.biobank_order_id from
                   biobank_order_identifier boi where boi.`value` = bss.biobank_order_identifier
                ) as biobank_order_id,
                bss.*
            from biobank_stored_sample bss
            where bss.biobank_id = :bb_id
            order by biobank_order_id, test;
        """

        data = {}
        orders = list()
        # Find all biobank orders associated with this participant
        cursor = ro_session.execute(_biobank_orders_sql, {'p_id': p_id})
        biobank_orders = [r for r in cursor]

        # Find stored samples associated with this participant. For any stored samples for which there
        # is no known biobank order, create a separate list that will be consolidated into a "pseudo" order record
        cursor = ro_session.execute(_biobank_stored_samples_sql, {'bb_id': p_bb_id})
        bss_results = [r for r in cursor]
        bss_missing_orders = list(filter(lambda r: r.biobank_order_id is None, bss_results))

        # Create an order record for each of this participant's biobank orders
        # This will reconcile ordered samples and stored samples (when available) to create sample summary records
        # for each sample associated with the order record
        for row in biobank_orders:
            cursor = ro_session.execute(_biobank_ordered_samples_sql, {'p_id': p_id, 'bo_id': row.biobank_order_id})
            bos_results = [r for r in cursor]
            bbo_samples = list()
            stored_count = 0
            for ordered_sample in bos_results:
                # This will look for a matching stored sample based on matching the biobank order id and test
                # to what's in the ordered sample record
                stored_sample = _get_stored_sample_row(bss_results, ordered_sample)
                bbo_samples.append(_make_sample_dict_from_row(bss=stored_sample, bos=ordered_sample))
                if stored_sample:
                    stored_count += 1

            order = {
                'biobank_order_id': row.biobank_order_id,
                'created': row.created,
                'status': str(
                    BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET),
                'status_id': int(
                    BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET),
                'dv_order': row.dv_order,
                'collected_site': row.collected_site,
                'collected_site_id': row.collected_site_id,
                'processed_site': row.processed_site,
                'processed_site_id': row.processed_site_id,
                'finalized_site': row.finalized_site,
                'finalized_site_id': row.finalized_site_id,
                'tests_ordered': len(bos_results),
                'tests_stored': stored_count,
                'samples': bbo_samples
            }

            orders.append(order)

        # Add any "orderless" stored samples for this participant.  They will all be associated with a
        # pseudo order with an order id of 'UNSET'
        if len(bss_missing_orders):
            orderless_stored_samples = list()
            for bss_row in bss_missing_orders:
                orderless_stored_samples.append(_make_sample_dict_from_row(bss=bss_row, bos=None))

            orders.append({
                'biobank_order_id': 'UNSET',
                'tests_stored': len(orderless_stored_samples),
                'samples': orderless_stored_samples
            })

        if len(orders) > 0:
            data['biobank_orders'] = orders

        return data

    def _prep_patient_status_info(self, p_id, ro_session):
        """
        Lookup patient status history
        :param p_id: participant_id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        data = {}
        sql = """
            SELECT psh.id,
                   psh.created,
                   psh.modified,
                   psh.authored,
                   psh.patient_status,
                   psh.hpo_id,
                   (select t.name from hpo t where t.hpo_id = psh.hpo_id) as hpo_name,
                   psh.organization_id,
                   (select t.external_id from organization t where t.organization_id = psh.organization_id) AS organization_name,
                   psh.site_id,
                   (select t.google_group from site t where t.site_id = psh.site_id) as site_name,
                   psh.comment,
                   psh.user
            FROM patient_status_history psh
            WHERE psh.participant_id = :pid
            ORDER BY psh.id
        """
        try:
            cursor = ro_session.execute(sql, {'pid': p_id})
        except exc.ProgrammingError:
            # The patient_status_history table does not exist when running unittests.
            return data
        results = [r for r in cursor]
        if results:
            status_recs = list()
            for row in results:
                status_recs.append({
                    'patient_status_history_id': row.id,
                    'patient_status_created': row.created,
                    'patient_status_modified': row.modified,
                    'patient_status_authored': row.authored,
                    'patient_status': str(PatientStatusFlag(row.patient_status)),
                    'patient_status_id': int(PatientStatusFlag(row.patient_status)),
                    'hpo': row.hpo_name,
                    'hpo_id': row.hpo_id,
                    'organization': row.organization_name,
                    'organization_id': row.organization_id,
                    'site': row.site_name,
                    'site_id': row.site_id,
                    'comment': row.comment,
                    'user': row.user
                })
            data['patient_statuses'] = status_recs

        return data

    def _calculate_enrollment_status(self, summary):
        """
        Calculate the participant's enrollment status
        :param summary: summary data
        :return: dict
        """
        status = EnrollmentStatusV2.REGISTERED
        data = {
            'enrollment_status': str(status),
            'enrollment_status_id': int(status)
        }
        if 'consents' not in summary:
            return data

        consents = {}
        study_consent = ehr_consent = pm_complete = gror_consent = had_gror_consent = had_ehr_consent = \
            ehr_consent_expired = False
        study_consent_date = datetime.date.max
        enrollment_member_time = datetime.datetime.max
        # iterate over consents, sorted by 'consent_module_authored' descending.
        for consent in summary['consents']:
            response_value = consent['consent_value']
            response_date = consent['consent_date'] or datetime.date.max
            if consent['consent'] == 'ConsentPII':
                study_consent = True
                study_consent_date = min(study_consent_date, response_date)
            elif consent['consent'] == EHR_CONSENT_QUESTION_CODE:
                if not 'EHRConsent' in consents:  # We only want the most recent consent answer.
                    consents['EHRConsent'] = (response_value, response_date)
                had_ehr_consent = had_ehr_consent or response_value == CONSENT_PERMISSION_YES_CODE
                consents['EHRConsentExpired'] = (consent.get('consent_expired'), response_date)
                ehr_consent_expired = consent.get('consent_expired') == EHR_CONSENT_EXPIRED_YES
                enrollment_member_time = min(enrollment_member_time,
                                             consent['consent_module_created'] or datetime.datetime.max)
            elif consent['consent'] == DVEHR_SHARING_QUESTION_CODE:
                if not 'DVEHRConsent' in consents:  # We only want the most recent consent answer.
                    consents['DVEHRConsent'] = (response_value, response_date)
                had_ehr_consent = had_ehr_consent or response_value == DVEHRSHARING_CONSENT_CODE_YES
                enrollment_member_time = min(enrollment_member_time,
                                             consent['consent_module_created'] or datetime.datetime.max)
            elif consent['consent'] == GROR_CONSENT_QUESTION_CODE:
                if not 'GRORConsent' in consents:  # We only want the most recent consent answer.
                    consents['GRORConsent'] = (response_value, response_date)
                had_gror_consent = had_gror_consent or response_value == CONSENT_GROR_YES_CODE

        if 'EHRConsent' in consents and 'DVEHRConsent' in consents:
            if consents['DVEHRConsent'][0] == DVEHRSHARING_CONSENT_CODE_YES \
                    and consents['EHRConsent'][0] != CONSENT_PERMISSION_NO_CODE:
                ehr_consent = True
            if consents['EHRConsent'][0] == CONSENT_PERMISSION_YES_CODE:
                ehr_consent = True
        elif 'EHRConsent' in consents:
            if consents['EHRConsent'][0] == CONSENT_PERMISSION_YES_CODE:
                ehr_consent = True
        elif 'DVEHRConsent' in consents:
            if consents['DVEHRConsent'][0] == DVEHRSHARING_CONSENT_CODE_YES:
                ehr_consent = True

        if 'GRORConsent' in consents:
            gror_answer = consents['GRORConsent'][0]
            gror_consent = gror_answer == CONSENT_GROR_YES_CODE

        # check physical measurements
        physical_measurements_date = datetime.datetime.max
        if 'pm' in summary:
            for pm in summary['pm']:
                if pm['status_id'] == int(PhysicalMeasurementsStatus.COMPLETED) or \
                        (pm['finalized'] and pm['status_id'] != int(PhysicalMeasurementsStatus.CANCELLED)):
                    pm_complete = True
                    physical_measurements_date = \
                        min(physical_measurements_date, pm['finalized'] or datetime.datetime.max)

        baseline_module_count = 0
        latest_baseline_module_completion = datetime.datetime.min
        completed_all_baseline_modules = False
        if 'modules' in summary:
            for module in summary['modules']:
                if module['baseline_module'] == 1:
                    baseline_module_count += 1
                    latest_baseline_module_completion = \
                        max(latest_baseline_module_completion, module['module_created'] or datetime.datetime.min)
            completed_all_baseline_modules = baseline_module_count >= len(self._baseline_modules)

        dna_sample_count = 0
        first_dna_sample_date = datetime.datetime.max
        bb_orders = summary.get('biobank_orders', list())
        for order in bb_orders:
            for sample in order.get('samples', list()):
                if sample['dna_test'] and sample['confirmed']:
                    dna_sample_count += 1
                    first_dna_sample_date = min(first_dna_sample_date, sample['created'] or datetime.datetime.max)

        if study_consent is True:
            status = EnrollmentStatusV2.PARTICIPANT
        if status == EnrollmentStatusV2.PARTICIPANT and ehr_consent is True:
            status = EnrollmentStatusV2.FULLY_CONSENTED
        if (status == EnrollmentStatusV2.FULLY_CONSENTED or (ehr_consent_expired and not ehr_consent)) and \
            pm_complete and \
            (summary['consent_cohort'] != BQConsentCohort.COHORT_3.name or gror_consent) and \
            'modules' in summary and \
            completed_all_baseline_modules and \
            dna_sample_count > 0:
            status = EnrollmentStatusV2.CORE_PARTICIPANT

        if status == EnrollmentStatusV2.PARTICIPANT or status == EnrollmentStatusV2.FULLY_CONSENTED:
            # Check to see if the participant might have had all the right ingredients to be Core at some point
            # This assumes consent for study, completion of baseline modules, stored dna sample,
            # and physical measurements can't be reversed
            if study_consent and completed_all_baseline_modules and dna_sample_count > 0 and pm_complete and \
                    had_ehr_consent and \
                    (summary['consent_cohort'] != BQConsentCohort.COHORT_3.name or had_gror_consent):
                # If they've had everything right at some point, go through and see if there was any time that they
                # had them all at once
                study_consent_date_range = DateCollection()
                study_consent_date_range.add_start(study_consent_date)

                pm_date_range = DateCollection()
                pm_date_range.add_start(physical_measurements_date)

                baseline_modules_date_range = DateCollection()
                baseline_modules_date_range.add_start(latest_baseline_module_completion)

                dna_date_range = DateCollection()
                dna_date_range.add_start(first_dna_sample_date)

                ehr_date_range = DateCollection()
                gror_date_range = DateCollection()

                current_ehr_response = current_dv_ehr_response = None
                # These consent responses are expected to be in order by their authored date ascending.
                # Fall back to created if there are None values in authored
                try:
                    _consents = sorted(summary['consents'], key=lambda k: k['consent_module_authored'])
                except TypeError:
                    _consents = sorted(summary['consents'], key=lambda k: k['consent_module_created'])
                for consent in _consents:
                    consent_question = consent['consent']
                    consent_response = consent['consent_value']
                    response_date = consent['consent_module_authored']
                    if consent_question == EHR_CONSENT_QUESTION_CODE:
                        current_ehr_response = consent_response
                        if current_ehr_response == CONSENT_PERMISSION_YES_CODE:
                            ehr_date_range.add_start(response_date)
                        elif current_ehr_response == CONSENT_PERMISSION_NO_CODE or \
                                current_dv_ehr_response != CONSENT_PERMISSION_YES_CODE:
                            # dv_ehr should be honored if ehr value is UNSURE
                            ehr_date_range.add_stop(response_date)
                    elif consent_question == DVEHR_SHARING_QUESTION_CODE:
                        current_dv_ehr_response = consent_response
                        if current_dv_ehr_response == DVEHRSHARING_CONSENT_CODE_YES and \
                                current_ehr_response != CONSENT_PERMISSION_NO_CODE:
                            ehr_date_range.add_start(response_date)
                        elif current_dv_ehr_response != DVEHRSHARING_CONSENT_CODE_YES and \
                                current_ehr_response != CONSENT_PERMISSION_YES_CODE:
                            ehr_date_range.add_stop(response_date)
                    elif consent_question == GROR_CONSENT_QUESTION_CODE:
                        if consent_response == CONSENT_GROR_YES_CODE:
                            gror_date_range.add_start(response_date)
                        else:
                            gror_date_range.add_stop(response_date)

                try:
                    date_overlap = study_consent_date_range\
                        .get_intersection(pm_date_range)\
                        .get_intersection(baseline_modules_date_range) \
                        .get_intersection(dna_date_range)\
                        .get_intersection(ehr_date_range)

                    if summary['consent_cohort'] == BQConsentCohort.COHORT_3.name:
                        date_overlap = date_overlap.get_intersection(gror_date_range)

                    # If there's any time that they had everything at once, then they should be a Core participant
                    if date_overlap.any():
                        status = EnrollmentStatusV2.CORE_PARTICIPANT
                except TypeError:
                    pid = summary["participant_id"]
                    logging.warning(
                        f'Enrollment Status Re-Calc: P{pid} is missing a date value, please investigate.')

        data['enrollment_status'] = str(status)
        data['enrollment_status_id'] = int(status)
        if status > EnrollmentStatusV2.REGISTERED:
            data['enrollment_member'] = \
                enrollment_member_time if enrollment_member_time != datetime.datetime.max else None

        return data

    #
    # Depreciated for now, but keep this code around for later.
    #
    # def _calculate_enrollment_timestamps(self, summary):
    #     """
    #     Calculate all enrollment status timestamps, based on calculate_max_core_sample_time() method in
    #     participant summary dao.
    #     :param summary: summary data
    #     :return: dict
    #     """
    #     if summary['enrollment_status_id'] != int(EnrollmentStatusV2.CORE_PARTICIPANT):
    #         return {}
    #
    #     # Calculate the min ordered sample and max stored sample times.
    #     ordered_time = stored_time = datetime.datetime.max
    #     stored_sample_times = dict(zip(self._dna_sample_test_codes, [
    #         {
    #             'confirmed': datetime.datetime.min, 'confirmed_count': 0,
    #             'disposed': datetime.datetime.min, 'disposed_count': 0
    #         } for i in range(0, 5)]))  # pylint: disable=unused-variable
    #
    #     for bbo in summary['biobank_orders']:
    #         if not bbo['samples']:
    #             continue
    #
    #         for bboi in bbo['samples']:
    #             if bboi['dna_test'] == 1:
    #                 # See: biobank_order_dao.py:_set_participant_summary_fields()
    #                 #      biobank_order_dao.py:_get_order_status_and_time()
    #                 if ordered_time == datetime.datetime.max:
    #                     ordered_time = (bboi['finalized'] or bboi['processed'] or
    #                                     bboi['collected'] or bbo['created'] or datetime.datetime.max)
    #                 # See: participant_summary_dao.py:calculate_max_core_sample_time() and
    #                 #       _participant_summary_dao.py:126
    #                 sst = stored_sample_times[bboi['test']]
    #                 if bboi['confirmed']:
    #                     sst['confirmed'] = max(sst['confirmed'], bboi['confirmed'])
    #                     sst['confirmed_count'] += 1
    #                 if bboi['disposed']:
    #                     sst['disposed'] = max(sst['disposed'], bboi['disposed'])
    #                     sst['disposed_count'] += 1
    #
    #     sstl = list()
    #     for k, v in stored_sample_times.items():  # pylint: disable=unused-variable
    #         if v['confirmed_count'] != v['disposed_count']:
    #             ts = v['confirmed']
    #         else:
    #             ts = v['disposed']
    #         if ts != datetime.datetime.min:
    #             sstl.append(ts)
    #
    #     if sstl:
    #         stored_time = min(sstl)
    #
    #     ordered_time = ordered_time if ordered_time != datetime.datetime.max else None
    #     stored_time = stored_time if stored_time != datetime.datetime.max else None
    #
    #     data = {
    #         'enrollment_core_ordered': ordered_time,
    #         'enrollment_core_stored': stored_time
    #     }
    #
    #     if not ordered_time and not stored_time:
    #         return data
    #
    #     # This logic [DA-769] added to RDR on 10/31/2018, but the backfill [DA-784] only applied this logic where
    #     # the enrollment core stored and ordered field values were null. I think its impossible to fully recreate
    #     # the timestamp values in the RDR enrollment core ordered and stored fields here. For example see: [PDR-114].
    #     # See: participant_summary_dao.py:calculate_max_core_sample_time()
    #     # If we have ordered or stored sample times, ensure that it is not before the alt_time value.
    #     alt_time = max(
    #         summary.get('enrollment_member', datetime.datetime.min),
    #         max(mod['module_created'] or datetime.datetime.min for mod in summary['modules']
    #                 if mod['baseline_module'] == 1),
    #         max(pm['finalized'] or datetime.datetime.min for pm in summary['pm']) if 'pm' in summary
    #                 else datetime.datetime.min
    #     )
    #
    #     if ordered_time:
    #         data['enrollment_core_ordered'] = max(ordered_time, alt_time)
    #     if stored_time:
    #         data['enrollment_core_stored'] = max(stored_time, alt_time)
    #
    #     return data

    def _calculate_distinct_visits(self, summary):  # pylint: disable=unused-argument
        """
        Calculate the distinct number of visits.
        :param summary: summary data
        :return: dict
        """

        def datetime_to_date(val):
            """
            Change from UTC to middle of the US before extracting date. That way if we have an early and late visit
            they will end up as the same day.
            """
            tmp = val.replace(tzinfo=tz.tzutc()).astimezone(tz.gettz('America/Denver'))
            return datetime.date(tmp.year, tmp.month, tmp.day)

        data = {}
        dates = list()

        if 'pm' in summary:
            for pm in summary['pm']:
                if pm['status_id'] != int(PhysicalMeasurementsStatus.CANCELLED) and pm['finalized']:
                    dates.append(datetime_to_date(pm['finalized']))

        if 'biobank_orders' in summary:
            for order in summary['biobank_orders']:
                if order['biobank_order_id'] != 'UNSET' \
                   and ['status_id'] != int(BiobankOrderStatus.CANCELLED) and 'samples' in order:
                    for sample in order['samples']:
                        if 'finalized' in sample and sample['finalized'] and \
                            isinstance(sample['finalized'], datetime.datetime):
                            dates.append(datetime_to_date(sample['finalized']))
        dates = list(set(dates))  # de-dup list
        data['distinct_visits'] = len(dates)
        return data

    def _calculate_test_participant(self, summary):
        """
        Calculate if this participant is a test participant or not.
        :param summary: summary data
        :return: dict
        """
        test_participant = summary['is_ghost_id']

        # Check for @example.com in email address
        if not test_participant:
            if summary.get('test_participant') == 1:
                test_participant = 1
            # Check to see if the participant is in the Test HPO.
            elif (summary.get('hpo') or 'None').lower() == 'test':
                test_participant = 1
            # Test if @example.com is in email address.
            elif '@example.com' in (summary.get('email') or ''):
                test_participant = 1
            # Check for SMS phone number for test participants.
            elif re.sub('[\(|\)|\-|\s]', '', (summary.get('login_phone_number') or 'None')).startswith('4442'):
                test_participant = 1
            elif re.sub('[\(|\)|\-|\s]', '', (summary.get('phone_number') or 'None')).startswith('4442'):
                test_participant = 1

        data = {'test_participant': test_participant}
        return data

    @staticmethod
    def get_module_answers(ro_dao, module, p_id, qr_id=None, return_responses=False, cdm_db_exists=False):
        """
        Retrieve the questionnaire module answers for the given participant id.  This retrieves all responses to
        the module and applies/layers the answers from each response to the final data dict returned.
        :param ro_dao: Readonly ro_dao object
        :param module: Module name
        :param p_id: participant id.
        :param qr_id: questionnaire response id
        :param return_responses:  Return the responses (unlayered) in addition to the processed answer data
        :param cdm_db_exists: Temporary arg.
        :return: dicts
        """
        _module_info_sql = """
            SELECT DISTINCT qr.questionnaire_id,
                   qr.questionnaire_response_id,
                   qr.created,
                   q.version,
                   qr.authored,
                   qr.language,
                   qr.participant_id,
                   qr.status
            FROM questionnaire_response qr
                    INNER JOIN questionnaire_concept qc on qr.questionnaire_id = qc.questionnaire_id
                    INNER JOIN questionnaire q on q.questionnaire_id = qc.questionnaire_id
        """
        # Temporary
        if cdm_db_exists:
            _module_info_sql += """
                LEFT OUTER JOIN cdm.tmp_questionnaire_response tqr
                    ON qr.questionnaire_response_id = tqr.questionnaire_response_id
                WHERE qr.participant_id = :p_id and qc.code_id in (select c1.code_id from code c1 where c1.value = :mod) AND
                   (tqr.duplicate is null or tqr.duplicate = 0)
                ORDER BY qr.created;
            """
        else:
            _module_info_sql += """
                WHERE qr.participant_id = :p_id and qc.code_id in (select c1.code_id from code c1 where c1.value = :mod)
                ORDER BY qr.created;
            """

        _answers_sql = """
            SELECT qr.questionnaire_id,
                   qq.code_id,
                   (select c.value from code c where c.code_id = qq.code_id) as code_name,
                   COALESCE((SELECT c.value from code c where c.code_id = qra.value_code_id),
                            qra.value_integer, qra.value_decimal,
                            qra.value_boolean, qra.value_string, qra.value_system,
                            qra.value_uri, qra.value_date, qra.value_datetime) as answer
            FROM questionnaire_response qr
                     INNER JOIN questionnaire_response_answer qra
                                ON qra.questionnaire_response_id = qr.questionnaire_response_id
                     INNER JOIN questionnaire_question qq
                                ON qra.question_id = qq.questionnaire_question_id
                     INNER JOIN questionnaire q
                                ON qq.questionnaire_id = q.questionnaire_id
            WHERE qr.questionnaire_response_id = :qr_id;
        """

        answers = OrderedDict()

        if not ro_dao:
            ro_dao = ResourceDataDao(backup=True)

        with ro_dao.session() as session:
            results = session.execute(_module_info_sql, {"p_id": p_id, "mod": module})
            if not results:
                return None

            # Query the answers for all responses found.
            # Note on special logic for GROR module:  the original GROR consent questionnaire was quickly replaced by
            # a revised questionnaire with a different consent question/answer structure.  GROR consents (~200)
            # that came in for the old/deprecated questionnaire_id were resent by PTSC using the new questionnaire_id
            # (See ROC-447/ROC-475)
            #
            # When processing a deprecated GROR response, add a key/value pair to the data
            # simulating what the consent answer would look like in the revised consent.  E.g., if the
            # deprecated GROR consent response had these question codes/boolean answer values (only one will be True/1):
            #   'CheckDNA_Yes': '0',
            #   'CheckDNA_No': '1',
            #   'CheckDNA_NotSure': '0'
            # ... then this key/value pair will be added to simulate the revised GROR consent question code/answer code:
            #    'ResultsConsent_CheckDNA': 'CheckDNA_No'
            #
            # This way the answers returned can have the same logic applied to them by _prep_modules(), for all GROR
            # consents.  This is intended to help resolve some mismatch issues between RDR and PDR GROR data
            for row in results:
                # Save parent record field values into data dict.
                data = ro_dao.to_dict(row, result_proxy=results)
                qnans = session.execute(_answers_sql, {'qr_id': row.questionnaire_response_id})
                # Save answers into data dict.
                for qnan in qnans:
                    data[qnan.code_name] = qnan.answer
                    # Special handling of GROR deprecated responses
                    if module == 'GROR' \
                        and data['questionnaire_id'] == _deprecated_gror_consent_questionnaire_id \
                        and qnan.code_name in _deprecated_gror_consent_question_code_names \
                        and qnan.answer and qnan.answer == '1':
                        # The deprecated consent question code name (if it has the selected/True value), ends up being
                        # the answer code value for the updated GROR consent question
                        data[_consent_module_question_map['GROR']] = qnan.code_name

                # Insert data dict into answers list.
                answers[row.questionnaire_response_id] = data

        # Apply answers to data dict, response by response, until we reach the end or the specific response id.
        data = dict()
        for questionnaire_response_id, qnans in answers.items():
            data.update(qnans)
            if qr_id and qr_id == questionnaire_response_id:
                break

        # Map empty data dict to a None return and return the unlayered raw responses if requested
        # Returning the raw responses enables some additional special case logic in _prep_consentpii()
        rtn_data = None
        if bool(data):
            rtn_data = data
        if return_responses:
            return rtn_data, answers
        else:
            return rtn_data


    @staticmethod
    def is_replay(prev_data_dict, data_dict, created_key=None):
        """
        Compares two module or consent data dictionaries to identify replayed responses
        Replayed/resent responses are usually the result of trying to resolve a data issue, and are basically
        duplicate QuestionnaireResponse payloads except for differing creation timestamps

        :param prev_data_dict: data dictionary to compare
        :param data_dict: data dictionary to compare
        :param created_key:  Dict key for the created timestamp value
        :return:  Boolean, True if the dictionaries match on everything except the created timestamp value
        """
        # Confirm both data dictionaries are populated
        if not bool(prev_data_dict) or not bool(data_dict):
            return False

        # Validate we're comparing "like" dictionaries, with a valid created timestamp key name
        prev_data_dict_keys = sorted(list(prev_data_dict.keys()))
        data_dict_keys = sorted(list(data_dict.keys()))
        if prev_data_dict_keys != data_dict_keys:
            return False
        if not (created_key and created_key in data_dict_keys):
            return False

        # Content must match on everything but created timestamp value
        for key in data_dict.keys():
            if key == created_key:
                continue
            if data_dict[key] != prev_data_dict[key]:
                return False

        return True


def rebuild_participant_summary_resource(p_id, res_gen=None, pdr_gen=None, patch_data=None):
    """
    Rebuild a resource record for a specific participant
    :param p_id: participant id
    :param res_gen: ParticipantSummaryGenerator object
    :param pdr_gen: PDRParticipantSummaryGenerator object
    :param patch_data: dict of resource values to update/insert.
    :return:
    """
    # Allow for batch requests to rebuild participant summary data.
    if not res_gen:
        res_gen = ParticipantSummaryGenerator()

    if not pdr_gen:
        pdr_gen = generators.PDRParticipantSummaryGenerator()

    # See if this is a partial update.
    if patch_data and isinstance(patch_data, dict):
        res_gen.patch_resource(p_id, patch_data)
        return patch_data

    res = res_gen.make_resource(p_id)
    res.save()

    pdr_res = pdr_gen.make_resource(p_id, ps_res=res)
    pdr_res.save()

    return res

def participant_summary_update_resource_task(p_id):
    """
    Cloud task to update the Participant Summary record for the given participant.
    :param p_id: Participant ID
    """
    rebuild_participant_summary_resource(p_id)
