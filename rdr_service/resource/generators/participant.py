import datetime
import enum
import hashlib
import json
import logging
import re

from collections import OrderedDict
from dateutil import parser, tz
from dateutil.parser import ParserError
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, desc, exc
from werkzeug.exceptions import NotFound

from rdr_service import config
from rdr_service.code_constants import (
    CONSENT_COPE_YES_CODE,
    CONSENT_COPE_NO_CODE,
    CONSENT_COPE_DEFERRED_CODE,
    CABOR_SIGNATURE_QUESTION_CODE,
    PMI_SKIP_CODE,
    WITHDRAWAL_CEREMONY_QUESTION_CODE,
    WITHDRAWAL_CEREMONY_YES,
    WITHDRAWAL_CEREMONY_NO
)
from rdr_service.dao.resource_dao import ResourceDataDao
# TODO: Replace BQRecord here with a Resource alternative.
from rdr_service.model.bq_base import BQRecord
# TODO: Create new versions of these ENUMs in resource.constants.
from rdr_service.model.bq_participant_summary import BQStreetAddressTypeEnum, BQModuleStatusEnum
from rdr_service.model.consent_file import ConsentType, ConsentSyncStatus
from rdr_service.model.code import Code
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import (PhysicalMeasurements, PhysicalMeasurementsStatus,
                                            PhysicalMeasurementsCollectType, OriginMeasurementUnit)
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_cohort_pilot import ParticipantCohortPilot
# TODO:  Using participant_summary as a workaround.  Replace with new participant_profile when it's available
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.site import Site
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer, \
    QuestionnaireResponseClassificationType
from rdr_service.participant_enums import (EnrollmentStatusV2, WithdrawalStatus, WithdrawalReason,
                                           SuspensionStatus, DeceasedStatus, DeceasedReportStatus, SampleStatus,
                                           BiobankOrderStatus, PatientStatusFlag, ParticipantCohortPilotFlag, EhrStatus,
                                           QuestionnaireResponseStatus, OrderStatus, WithdrawalAIANCeremonyStatus,
                                           TEST_HPO_NAME, TEST_LOGIN_PHONE_NUMBER_PREFIX, SampleCollectionMethod)
from rdr_service.resource import generators, schemas
from rdr_service.resource.calculators import EnrollmentStatusCalculator, ParticipantUBRCalculator as ubr
from rdr_service.resource.constants import SchemaID, ActivityGroupEnum, ParticipantEventEnum, ConsentCohortEnum, \
    PDREnrollmentStatusEnum
from rdr_service.resource.schemas.participant import StreetAddressTypeEnum, BIOBANK_UNIQUE_TEST_IDS


class ModuleLookupEnum(enum.Enum):
    """ Used to order and limit the number of module responses returned from a lookup """
    ALL = 0
    FIRST = 1
    LAST = 2


_consent_module_question_map = {
    # { module: question code string }
    'ConsentPII': None,
    'DVEHRSharing': 'DVEHRSharing_AreYouInterested',
    'EHRConsentPII': 'EHRConsentPII_ConsentPermission',
    'GROR': 'ResultsConsent_CheckDNA',
    'PrimaryConsentUpdate': 'Reconsent_ReviewConsentAgree',
    'ProgramUpdate': None,
    'COPE': 'section_participation',
    'cope_nov': 'section_participation',
    'cope_dec': 'section_participation',
    'cope_feb': 'section_participation',
    'GeneticAncestry': 'GeneticAncestry_ConsentAncestryTraits',
    'covid_19_serology_results': 'covid_19_serology_results_decision',
    'wear_consent': 'resultsconsent_wear',
    # Reconsent modules for cases where participant may not have initially completed the expected VA vs. Non-VA version
    'vaprimaryreconsent_c1_2': 'vaprimaryreconsent_c1_2_agree',
    'vaprimaryreconsent_c3': 'vaprimaryreconsent_c3_agree',
    'vaehrreconsent': 'vaehrreconsent_agree',
    'nonvaprimaryreconsent': 'nonvaprimaryreconsent_agree'
}

# _consent_expired_question_map, for expired consents. { module: question code string }
_consent_expired_question_map = {
    'EHRConsentPII': 'EHRConsentPII_ConsentExpired'
}

# Possible answer codes for the consent module questions and what submittal status the answers correspond to.
# { answer code string: BQModuleStatusEnum value }
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
    'ConsentAncestryTraits_NotSure': BQModuleStatusEnum.SUBMITTED_NOT_SURE,
    'PMI_Skip': BQModuleStatusEnum.UNSET,
    # covid_19_serology_results_decision
    'Decision_Yes': BQModuleStatusEnum.SUBMITTED,
    'Decision_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'WEAR_Yes': BQModuleStatusEnum.SUBMITTED,
    'WEAR_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    # VA/Non-VA reconsent modules consent answer options (note:  agree_no is defined but may not be transmitted to RDR,
    # participants who decline reconsent may be updated to withdrawn status by PTSC instead)
    'agree_yes': BQModuleStatusEnum.SUBMITTED,
    'agree_no': BQModuleStatusEnum.SUBMITTED_NO_CONSENT
}

# PDR-252:  When RDR starts accepting QuestionnaireResponse payloads for withdrawal screens, AIAN participants
# will be given options for a last rites ceremony for their biobank samples.  Map answer codes to the status enum value
# included with the PDR participant data
_withdrawal_aian_ceremony_status_map = {
    WITHDRAWAL_CEREMONY_YES: WithdrawalAIANCeremonyStatus.REQUESTED,
    WITHDRAWAL_CEREMONY_NO: WithdrawalAIANCeremonyStatus.DECLINED
}

# See hotfix ticket ROC-447 / backfill ticket ROC-475.  The first GROR consent questionnaire was immediately
# deprecated and replaced by a revised consent questionnaire.  Early GROR consents (~200) already received
# were replayed using the revised questionnaire format.  When retrieving GROR module answers, we'll look for
# deprecated GROR consent questions/answers to map them to the revised consent question/answer format.
_deprecated_gror_consent_questionnaire_id = 415
_deprecated_gror_consent_question_code_names = ('CheckDNA_Yes', 'CheckDNA_No', 'CheckDNA_NotSure')

# For cases where we don't want to carry forward previous answers if a subsequent response to the same module is a
# partial.  For example, the only time we see the EHRConsentPII_ConsentExpired hidden question code / answer is for an
# expired consent.   Don't want to carry the EHRConsentPII_ConsentExpired_Yes answer to a subsequent renewed consent
# See: get_module_answers() method.
_unlayered_question_codes_map = {
    'EHRConsentPII': ['EHRConsentPII_ConsentExpired', ]
}

# Temporary:  for finding/debugging mismatches in new EnrollmentStatusCalculator results to old calculation results
_enrollment_status_map = {
    EnrollmentStatusV2.REGISTERED:  PDREnrollmentStatusEnum.Registered,
    EnrollmentStatusV2.PARTICIPANT: PDREnrollmentStatusEnum.Participant,
    EnrollmentStatusV2.FULLY_CONSENTED: PDREnrollmentStatusEnum.ParticipantPlusEHR,
    EnrollmentStatusV2.CORE_MINUS_PM: PDREnrollmentStatusEnum.CoreParticipantMinusPM,
    EnrollmentStatusV2.CORE_PARTICIPANT: PDREnrollmentStatusEnum.CoreParticipant
}


def _act(timestamp, group: ActivityGroupEnum, event: ParticipantEventEnum, **kwargs):
    """ Create and return a activity record. """
    event = {
        'timestamp': timestamp,
        'group': group.name,
        'group_id': group.value,
        'event': event,
        'event_name': event.name
    }
    # Check for additional key/value pairs to add to this activity record.
    if kwargs:
        event.update(kwargs)
    return event


class ParticipantSummaryGenerator(generators.BaseGenerator):
    """
    Generate a Participant Summary Resource object
    """
    ro_dao = None
    # Retrieve module and sample test lists from config.
    _baseline_modules = [mod.replace('questionnaireOn', '')
                         for mod in config.getSettingList('baseline_ppi_questionnaire_fields')]
    _baseline_sample_test_codes = config.getSettingList('baseline_sample_test_codes')
    _dna_sample_test_codes = config.getSettingList('dna_sample_test_codes')

    def make_resource(self, p_id, qc_mode=False):
        """
        Build a Participant Summary Resource object for the given participant id.
        :param p_id: Participant ID
        :param qc_mode:  If True, the resource record will be generated and returned but will not be saved to the DB
        :return: ResourceDataObject object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=True)

        with self.ro_dao.session() as ro_session:
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
            # prep race, gender and sexual orientation
            summary = self._merge_schema_dicts(summary, self._prep_the_basics(p_id, ro_session))
            # prep biobank orders and samples
            summary = self._merge_schema_dicts(summary, self._prep_biobank_info(p_id, summary['biobank_id'],
                                                                                ro_session))
            # prep patient status history
            summary = self._merge_schema_dicts(summary, self._prep_patient_status_info(p_id, ro_session))
            # calculate enrollment status for participant
            summary = self._merge_schema_dicts(summary, self._calculate_enrollment_status(summary, p_id))
            # calculate distinct visits
            summary = self._merge_schema_dicts(summary, self._calculate_distinct_visits(summary))
            # calculate UBR flags
            summary = self._merge_schema_dicts(summary, self._calculate_ubr(p_id, summary, ro_session))
            # calculate test participant status (if it was not already set by _prep_participant() )
            if summary['test_participant'] == 0:
                summary = self._merge_schema_dicts(summary, self._check_for_test_credentials(summary))

            summary['activity'] = self.validate_activity_timestamps(summary['activity'])
            # data = self.ro_dao.to_resource_dict(summary, schema=schemas.ParticipantSchema)

            # DA-2611 related: Closes a gap where primary consent metrics records in PDR have some stale errors for
            # invalid DOB/invalid age at consent
            if summary.get('date_of_birth', None) and not qc_mode:
                self.generate_primary_consent_metrics(p_id, ro_session)

            return generators.ResourceRecordSet(schemas.ParticipantSchema, summary)

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

    @staticmethod
    def validate_activity_timestamps(activity, p_id=None):
        """
        Validate the timestamps in a list of activity events.
        :param activity: List of activity events.
        :param p_id: Participant ID
        :return:
        """
        # Test that all timestamps are datetime or None.
        msg = None
        cleaned = list()
        for ev in activity:
            if isinstance(ev['timestamp'], datetime.datetime):
                cleaned.append(ev)
                continue
            if ev['timestamp'] is not None and not isinstance(ev['timestamp'], datetime.datetime):
                try:
                    ev['timestamp'] = parser.parse(ev['timestamp'])
                    cleaned.append(ev)
                except ParserError:
                    msg = f'Participant activity timestamp is invalid for P{p_id}.'
        if msg:
            logging.error(msg)
        return cleaned

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

        # PDR-252:  The AIAN withdrawal ceremony decision needs to be made available to PDR.  Look for the latest
        # authored answer code, if one exists
        ceremony_question_code = ro_session.query(Code.codeId).filter(Code.value == WITHDRAWAL_CEREMONY_QUESTION_CODE)
        answer_code_filter = Code.value.in_([WITHDRAWAL_CEREMONY_NO, WITHDRAWAL_CEREMONY_YES])
        ceremony_response = ro_session.query(Code.value).\
            join(QuestionnaireResponseAnswer, QuestionnaireResponseAnswer.valueCodeId == Code.codeId).\
            join(QuestionnaireResponse,
                 QuestionnaireResponse.questionnaireResponseId == QuestionnaireResponseAnswer.questionnaireResponseId).\
            join(QuestionnaireQuestion,
                 QuestionnaireResponseAnswer.questionId == QuestionnaireQuestion.questionnaireQuestionId).\
            filter(QuestionnaireResponse.participantId == p_id,
                   QuestionnaireQuestion.codeId == ceremony_question_code, answer_code_filter).\
            order_by(desc(QuestionnaireResponse.authored)).one_or_none()

        if ceremony_response:
            withdrawal_aian_ceremony_status = \
                _withdrawal_aian_ceremony_status_map.get(ceremony_response.value, WithdrawalAIANCeremonyStatus.UNSET)
        else:
            withdrawal_aian_ceremony_status = WithdrawalAIANCeremonyStatus.UNSET

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

        # If RDR paired the pid to hpo TEST or flagged as either ghost or test participant, treat as test participant
        # An additional check will be made later at the end of the participant summary data setup, after we've
        # added details like email and phone numbers to the summary data dict, in case they have fake participant
        # credentials but are not correctly flagged in the participant table
        test_participant = p.isGhostId == 1 or p.isTestParticipant == 1 or (hpo and hpo.name == TEST_HPO_NAME)

        # TODO: Workaround for PDR-364 is to pull cohort value from participant_summary. LIMITED USE CASE ONLY
        cohort = ConsentCohortEnum.UNSET if not p or not p.participantSummary \
                    or p.participantSummary.consentCohort is None \
                    else ConsentCohortEnum(int(p.participantSummary.consentCohort))

        data = {
            'participant_id': f'P{p_id}',
            'biobank_id': p.biobankId,
            'research_id': p.researchId,
            'participant_origin': p.participantOrigin,
            'consent_cohort': cohort.name,
            'consent_cohort_id': cohort.value,
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
            'withdrawal_aian_ceremony_status': str(withdrawal_aian_ceremony_status),
            'withdrawal_aian_ceremony_status_id': int(withdrawal_aian_ceremony_status),

            'suspension_status': str(suspension_status),
            'suspension_status_id': int(suspension_status),
            'suspension_time': p.suspensionTime,

            'site': self._lookup_site_name(p.siteId, ro_session),
            'site_id': p.siteId,
            'is_ghost_id': 1 if p.isGhostId is True else 0,
            'test_participant': 1 if test_participant else 0,
            'cohort_2_pilot_flag': str(cohort_2_pilot_flag),
            'cohort_2_pilot_flag_id': int(cohort_2_pilot_flag),
            'deceased_status': str(deceased_status),
            'deceased_status_id': int(deceased_status),
            'deceased_authored': deceased_authored,
            # TODO:  Enable this field definition in the BQ model if it's determined it should be included in PDR
            'date_of_death': deceased_date_of_death
        }

        # Collect participant pairing history
        pairing_history = None
        query = ro_session.query(ParticipantHistory.lastModified, ParticipantHistory.hpoId, HPO.name.label('hpo'),
                                   ParticipantHistory.organizationId, Organization.externalId.label('organization'),
                                   ParticipantHistory.siteId, Site.googleGroup.label('site')). \
                outerjoin(HPO, HPO.hpoId == ParticipantHistory.hpoId).\
                outerjoin(Organization, Organization.organizationId == ParticipantHistory.organizationId).\
                outerjoin(Site, Site.siteId == ParticipantHistory.siteId).\
                filter(ParticipantHistory.participantId == p_id).order_by(ParticipantHistory.lastModified)
        # sql = self.ro_dao.query_to_text(query)
        pairing = query.all()
        if pairing:
            pairing_history = list()
            for item in pairing:
                pairing_history.append({
                    'last_modified': item.lastModified,
                    'hpo': item.hpo,
                    'hpo_id': item.hpoId,
                    'organization': item.organization,
                    'organization_id': item.organizationId,
                    'site': item.site,
                    'site_id': item.siteId
                })

        data['pairing_history'] = pairing_history

        # Record participant activity events
        data['activity'] = [
            _act(data['sign_up_time'], ActivityGroupEnum.Profile, ParticipantEventEnum.SignupTime),
            _act(data['withdrawal_authored'], ActivityGroupEnum.Profile, ParticipantEventEnum.Withdrawal),
            _act(data['deceased_authored'], ActivityGroupEnum.Profile, ParticipantEventEnum.Deceased)
        ]

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
        ps = ro_session.query(ParticipantSummary.ehrStatus,
                              ParticipantSummary.ehrReceiptTime,
                              ParticipantSummary.ehrUpdateTime,
                              ParticipantSummary.isEhrDataAvailable,
                              ParticipantSummary.consentForStudyEnrollmentAuthored,
                              ParticipantSummary.enrollmentStatus,  # legacy status mapped to EnrollmentStatusV2 in PDR
                              ParticipantSummary.enrollmentStatusMemberTime,
                              ParticipantSummary.enrollmentStatusCoreMinusPMTime,
                              ParticipantSummary.enrollmentStatusCoreOrderedSampleTime,
                              ParticipantSummary.enrollmentStatusCoreStoredSampleTime,
                              ParticipantSummary.enrollmentStatusV3_0,
                              ParticipantSummary.enrollmentStatusParticipantV3_0Time,
                              ParticipantSummary.enrollmentStatusParticipantPlusEhrV3_0Time,
                              ParticipantSummary.enrollmentStatusPmbEligibleV3_0Time,
                              ParticipantSummary.enrollmentStatusCoreMinusPmV3_0Time,
                              ParticipantSummary.enrollmentStatusCoreV3_0Time,
                              ParticipantSummary.enrollmentStatusV3_1,
                              ParticipantSummary.enrollmentStatusParticipantV3_1Time,
                              ParticipantSummary.enrollmentStatusParticipantPlusEhrV3_1Time,
                              ParticipantSummary.enrollmentStatusParticipantPlusBasicsV3_1Time,
                              ParticipantSummary.enrollmentStatusCoreMinusPmV3_1Time,
                              ParticipantSummary.enrollmentStatusCoreV3_1Time,
                              ParticipantSummary.enrollmentStatusParticipantPlusBaselineV3_1Time,
                              ParticipantSummary.healthDataStreamSharingStatusV3_1,
                              ParticipantSummary.healthDataStreamSharingStatusV3_1Time
        ).select_from(
            Participant
        ).join(
            ParticipantSummary, isouter=True
        ).filter(
            Participant.participantId == p_id
        ).first()

        # For PDR, start with REGISTERED as the default enrollment status (all versions).  This identifies participants
        # who have not yet consented / should not have a participant_summary record
        data = {}
        # TODO:  add enrollment_status / enrollment_status_id after Goal 1 QC (move from _calculate_enrollment_status)
        for key in ['enrollment_status_v2', 'enrollment_status_v3_0', 'enrollment_status_v3_1']:
            data[key] = str(EnrollmentStatusV2.REGISTERED)
            data[key + '_id'] = int(EnrollmentStatusV2.REGISTERED)

        if not ps.enrollmentStatus:
            logging.debug(f'No participant_summary record found for {p_id}')
        else:
            enrollment_v2 = EnrollmentStatusV2(int(ps.enrollmentStatus))
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
                'is_ehr_data_available': int(ps.isEhrDataAvailable),
                # TODO:  Move out of _calculate_enrollment_status() after Goal 1 / PEO QC
                # 'enrollment_status': str(enrollment_v2)
                # 'enrollment_status_id': int(enrollment_v2)
                # 'enrollment_member': ps.enrollmentStatusMemberTime,
                #'enrollment_core_minus_pm': ps.enrollmentStatusCoreMinusPMTime,
                'enrollment_status_legacy_v2': str(enrollment_v2),     # temporary, for Goal 1 QC
                'enrollment_status_legacy_v2_id': int(enrollment_v2),  # temporary, for Goal 1 QC
                'enrollment_status_v3_0': str(ps.enrollmentStatusV3_0),
                'enrollment_status_v3_0_id': int(ps.enrollmentStatusV3_0),
                'enrollment_status_v3_0_participant_time': ps.enrollmentStatusParticipantV3_0Time,
                'enrollment_status_v3_0_participant_plus_ehr_time': ps.enrollmentStatusParticipantPlusEhrV3_0Time,
                'enrollment_status_v3_0_pmb_eligible_time': ps.enrollmentStatusPmbEligibleV3_0Time,
                'enrollment_status_v3_0_core_minus_pm_time': ps.enrollmentStatusCoreMinusPmV3_0Time,
                'enrollment_status_v3_0_core_time': ps.enrollmentStatusCoreV3_0Time,
                'enrollment_status_v3_1': str(ps.enrollmentStatusV3_1),
                'enrollment_status_v3_1_id': int(ps.enrollmentStatusV3_1),
                'enrollment_status_v3_1_participant_time': ps.enrollmentStatusParticipantV3_1Time,
                'enrollment_status_v3_1_participant_plus_ehr_time': ps.enrollmentStatusParticipantPlusEhrV3_1Time,
                'enrollment_status_v3_1_participant_plus_basics_time': ps.enrollmentStatusParticipantPlusBasicsV3_1Time,
                'enrollment_status_v3_1_core_minus_pm_time': ps.enrollmentStatusCoreMinusPmV3_1Time,
                'enrollment_status_v3_1_core_time': ps.enrollmentStatusCoreV3_1Time,
                'enrollment_status_v3_1_participant_plus_baseline_time': \
                    ps.enrollmentStatusParticipantPlusBaselineV3_1Time,
                'health_datastream_sharing_status_v3_1': str(ps.healthDataStreamSharingStatusV3_1),
                'health_datastream_sharing_status_v3_1_id': int(ps.healthDataStreamSharingStatusV3_1),
                'health_datastream_sharing_status_v3_1_time': ps.healthDataStreamSharingStatusV3_1Time
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

            # Record participant activity events
            data['activity'] = [
                _act(data['ehr_receipt'], ActivityGroupEnum.Profile, ParticipantEventEnum.EHRFirstReceived),
                _act(data['ehr_update'], ActivityGroupEnum.Profile, ParticipantEventEnum.EHRLastReceived)
            ]

        return data

    def _prep_consentpii_answers(self, p_id):
        """
        Get participant information from the ConsentPII questionnaire
        :param p_id: participant id
        :return: dict
        """

        # PDR-178:  Retrieve both the processed (layered) answers result, and the raw responses. This allows us to
        # do some extra processing of the ConsentPII data without having to query all over again.
        qnans, responses = self.get_module_answers(self.ro_dao, 'ConsentPII', p_id, return_responses=True)
        if not qnans:
            # return the minimum data required when we don't have the questionnaire data.
            return {'email': None, 'is_ghost_id': 0}

        # TODO: Update this to a JSONObject instead of BQRecord object.
        qnan = BQRecord(schema=None, data=qnans)  # use only most recent response.

        # TODO: Should DOB be captured only from the first ConsentPII received?
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
            'cabor_authored': None,
            'email_available': 1 if qnan.get('ConsentPII_EmailAddress') else 0,
            'phone_number_available': 1 if (qnan.get('phone_number') or qnan.get('login_phone_number')) else 0
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
                cabor_ts = fields.get('authored')
                data['cabor_authored'] = parser.parse(cabor_ts) if cabor_ts and isinstance(cabor_ts, str) else cabor_ts
                break

        # Record participant activity events
        data['activity'] = [
            _act(data['cabor_authored'], ActivityGroupEnum.Profile, ParticipantEventEnum.CABOR)
        ]

        return data

    def _prep_modules(self, p_id, ro_session):
        """
        Find all questionnaire modules the participant has completed and loop through them.
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        activity = list()
        code_id_query = ro_session.query(func.max(QuestionnaireConcept.codeId)). \
            filter(QuestionnaireResponse.questionnaireId ==
                   QuestionnaireConcept.questionnaireId).label('codeId')

        # Responses are sorted by authored date ascending and then created date descending
        # This should result in a list where any replays of a response are adjacent (most recently created first).
        # Note: There is at least one instance where there are two responses for the same survey with identical
        #       'authored' and 'created' timestamps, but they are not a duplicate response, so we also add
        #       "externalId" to the order_by. 'questionnaireResponseId' is randomly generated and can't be used.
        query = ro_session.query(
                QuestionnaireResponse.answerHash,
                QuestionnaireResponse.questionnaireResponseId, QuestionnaireResponse.authored,
                QuestionnaireResponse.created, QuestionnaireResponse.language, QuestionnaireHistory.externalId,
                QuestionnaireResponse.status, code_id_query, QuestionnaireResponse.nonParticipantAuthor,
                QuestionnaireResponse.classificationType, QuestionnaireHistory.semanticVersion,
                QuestionnaireHistory.irbMapping). \
            join(QuestionnaireHistory). \
            filter(QuestionnaireResponse.participantId == p_id,
                   QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE). \
            order_by(QuestionnaireResponse.authored, QuestionnaireResponse.created.desc(),
                     QuestionnaireResponse.externalId.desc())
        # sql = self.ro_dao.query_to_text(query)
        results = query.all()

        modules = list()
        consents = list()
        data = dict()
        min_valid_authored = datetime.datetime(2017, 1, 1, 0, 0, 0)

        if results:
            # Track the last module/consent data dictionaries, so we can detect and omit replayed responses
            last_answer_hash = None  # Track the answer hash of the payload of the last response processed (PDR-484)
            last_mod_processed = {}
            last_consent_processed = {}
            for row in results:
                # ROC-692 Exclude CE replayed ConsentPII responses that contain an invalid minimum authored date.
                if row.authored and row.authored < min_valid_authored:
                    continue
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
                    'response_status_id': int(QuestionnaireResponseStatus(row.status)),
                    'questionnaire_response_id': row.questionnaireResponseId,
                    'consent': 1 if module_name in _consent_module_question_map else 0,
                    'non_participant_answer': row.nonParticipantAuthor if row.nonParticipantAuthor else None,
                    'semantic_version': row.semanticVersion,
                    'irb_mapping': row.irbMapping,
                    'classification_type': str(QuestionnaireResponseClassificationType(row.classificationType)),
                    'classification_type_id': int(QuestionnaireResponseClassificationType(row.classificationType))
                }

                mod_ca = {
                    'classification_type': module_data['classification_type'],
                    'classification_type_id': module_data['classification_type_id']
                }
                # check if this is a module with consents.
                if module_name in _consent_module_question_map:
                    qnans = self.get_module_answers(self.ro_dao, module_name, p_id, row.questionnaireResponseId)
                    if qnans:
                        qnan = BQRecord(schema=None, data=qnans)  # use only most recent questionnaire.
                        consent_answer_value, module_status = self._find_consent_response(qnan, module_name)
                        # TODO: Consent table depreciated, remove consent field sets after BigQuery table support
                        #  is removed.
                        consent = {
                            'consent': consent_answer_value,
                            'consent_id': self._lookup_code_id(consent_answer_value, ro_session),
                            'consent_date': parser.parse(qnan['authored']).date() if qnan['authored'] else None,
                            'consent_module': module_name,
                            'consent_module_authored': row.authored,
                            'consent_module_created': row.created,
                            'consent_module_external_id': row.externalId,
                            'consent_response_status': str(QuestionnaireResponseStatus(row.status)),
                            'consent_response_status_id': int(QuestionnaireResponseStatus(row.status)),
                            'questionnaire_response_id': row.questionnaireResponseId
                        }
                        # Note:  Based on currently available modules when a module has no
                        # associated answer options (like ConsentPII or ProgramUpdate), any submitted response is given
                        # an implicit ConsentPermission_Yes value.   May need adjusting if there are ever modules where
                        # that may no longer be true
                        if consent_answer_value is None:
                            consent['consent'] = module_name
                            consent['consent_id'] = self._lookup_code_id(module_name, ro_session)
                            consent['consent_value'] = module_data['consent_value'] = 'ConsentPermission_Yes'
                            consent['consent_value_id'] = module_data['consent_value_id'] = \
                                self._lookup_code_id('ConsentPermission_Yes', ro_session)
                        else:
                            if module_status == BQModuleStatusEnum.SUBMITTED_INVALID:
                                logging.warning("""
                                    No consent answer for module {0}.  Defaulting status to SUBMITTED_INVALID
                                    (pid {1}, response {2})
                                    """.format(module_name, p_id, row.questionnaireResponseId))

                            consent['consent_value'] = module_data['consent_value'] = consent_answer_value
                            consent['consent_value_id'] = module_data['consent_value_id'] = \
                                self._lookup_code_id(consent_answer_value, ro_session)
                            if module_name in _consent_expired_question_map:
                                consent['consent_expired'] = module_data['consent_expired'] = \
                                    qnan.get(_consent_expired_question_map[module_name] or 'None', None)
                            # TODO: Should we have also have a 'consent_expired_id', if so what would the integer
                            #  value be (there is only a question code_id in the code table, no answer code_id)?

                        module_data['status'] = module_status.name
                        module_data['status_id'] = module_status.value
                        mod_ca['answer'] = consent['consent_value']
                        mod_ca['answer_id'] = consent['consent_value_id']

                        # Compare against the last consent response processed to filter replays/duplicates
                        if not self.is_replay(last_consent_processed, last_answer_hash,
                                              consent, row.answerHash,
                                              ignore_keys=['consent_module_created', 'questionnaire_response_id']):
                            consents.append(consent)
                            consent_added = True
                        last_consent_processed = consent.copy()

                # consent_added == True means we already know it wasn't a replayed response
                if consent_added or not self.is_replay(last_mod_processed, last_answer_hash,
                                                       module_data, row.answerHash,
                                                       ignore_keys=['module_created', 'questionnaire_response_id']):
                    modules.append(module_data)
                    # Find module in ParticipantActivity Enum via a case-insensitive way.
                    mod_found = False
                    for en in ParticipantEventEnum:
                        # module_name can be None in the unittests.
                        if module_name and en.name.lower() == module_name.lower():
                            activity.append(_act(row.authored, ActivityGroupEnum.QuestionnaireModule, en, **mod_ca))
                            mod_found = True
                            break
                    if mod_found is False:
                        # The participant's module history often contains modules we aren't explicitly tracking as a
                        # ParticipantActivity yet.  Downgrade log message to debug to avoid noisy warnings
                        # TODO: Determine if ParticipantActivity list needs to be expanded
                        logging.debug(f'Key ({module_name}) not found in ParticipantActivity enum.')

                last_mod_processed = module_data.copy()
                last_answer_hash = row.answerHash

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
        data['activity'] = activity
        return data

    def _prep_the_basics(self, p_id, ro_session):
        """
        Get the participant's race and gender selections
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        qr_id = self.find_questionnaire_response_id(
            ro_session, p_id, "TheBasics", QuestionnaireResponseClassificationType.COMPLETE, ModuleLookupEnum.FIRST)
        if not qr_id:
            return {}

        qnans = self.get_module_answers(self.ro_dao, 'TheBasics', p_id, qr_id=qr_id, return_responses=False)
        if not qnans or len(qnans) == 0:
            return {}

        # get TheBasics questionnaire response answers
        qnan = BQRecord(schema=None, data=qnans)  # use only most recent questionnaire.
        data = {}
        # Turn comma-separated list of answer codes for race and gender into their nested arrays
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
        if qnan.get('Gender_CloserGenderDescription'):
            for val in qnan.get('Gender_CloserGenderDescription').split(','):
                gl.append({'gender': val, 'gender_id': self._lookup_code_id(val, ro_session)})
        if len(gl) > 0:
            data['genders'] = gl

        so = list()
        if qnan.get('TheBasics_SexualOrientation'):
            for val in qnan.get('TheBasics_SexualOrientation').split(','):
                so.append({'sexual_orientation': val, 'sexual_orientation_id': self._lookup_code_id(val, ro_session)})

        # get additional sexual orientation answers, but only if the answer to the parent question was "None of these
        # describe me"/'SexualOrientation_None'.   Any other answer means survey should not have branched to the
        # "additional options" menu ('GenderIdentity_SexualityCloserDescription' answer codes).  Decision was made to
        # ignore these unexpected "additional options" selections in PDR data.
        if (len(so) == 1 and so[0]['sexual_orientation'] == 'SexualOrientation_None'
               and qnan.get('GenderIdentity_SexualityCloserDescription')):
            for val in qnan.get('GenderIdentity_SexualityCloserDescription').split(','):
                so.append({'sexual_orientation': val, 'sexual_orientation_id': self._lookup_code_id(val, ro_session)})

        if len(so) > 0:
            data['sexual_orientations'] = so

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
        activity = list()

        query = ro_session.query(PhysicalMeasurements.physicalMeasurementsId, PhysicalMeasurements.created,
                                 PhysicalMeasurements.createdSiteId, PhysicalMeasurements.final,
                                 PhysicalMeasurements.finalized, PhysicalMeasurements.finalizedSiteId,
                                 PhysicalMeasurements.collectType, PhysicalMeasurements.origin,
                                 PhysicalMeasurements.originMeasurementUnit,
                                 PhysicalMeasurements.questionnaireResponseId,
                                 PhysicalMeasurements.status, PhysicalMeasurements.amendedMeasurementsId). \
            filter(PhysicalMeasurements.participantId == p_id). \
            order_by(desc(PhysicalMeasurements.created))
        # sql = self.dao.query_to_text(query)
        results = query.all()

        for row in results:
            # Imitate some of the RDR 'participant_summary' table logic, the PM status value defaults to COMPLETED
            # unless PM status is CANCELLED.  So we set all NULL values to COMPLETED status here.
            pm_status = PhysicalMeasurementsStatus(row.status or PhysicalMeasurementsStatus.COMPLETED)
            collection_type = PhysicalMeasurementsCollectType(row.collectType or PhysicalMeasurementsCollectType.UNSET)
            origin_measurements_type = OriginMeasurementUnit(row.originMeasurementUnit or OriginMeasurementUnit.UNSET)

            pm_list.append({
                'physical_measurements_id': row.physicalMeasurementsId,
                'questionnaire_response_id': row.questionnaireResponseId,
                'status': str(pm_status),
                'status_id': int(pm_status),
                'created': row.created,
                'created_site': self._lookup_site_name(row.createdSiteId, ro_session),
                'created_site_id': row.createdSiteId,
                'final': 1 if row.final else 0,
                'finalized': row.finalized,
                'finalized_site': self._lookup_site_name(row.finalizedSiteId, ro_session),
                'finalized_site_id': row.finalizedSiteId,
                'amended_measurements_id': row.amendedMeasurementsId,
                'collect_type':  str(collection_type),
                'collect_type_id': int(collection_type),
                'origin': row.origin,
                'origin_measurement_unit': str(origin_measurements_type),
                'origin_measurement_unit_id': int(origin_measurements_type),
                # If status == UNSET in data, then the record has been cancelled and then restored. PM status is
                # only set to UNSET in this scenario.
                'restored': 1 if row.status == 0 else 0
            })
            activity.append(_act(row.finalized or row.created, ActivityGroupEnum.Profile,
                                 ParticipantEventEnum.PhysicalMeasurements,
                                 **{'status': str(pm_status), 'status_id': int(pm_status)}))

        if len(pm_list) > 0:
            data['pm'] = pm_list
            data['activity'] = activity  # Record participant activity events

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
            (same test and order identifier).
            :param stored_samples: list of biobank_stored_sample rows
            :param ordered_sample: a biobank_ordered_sample row
            :return: the biobank_stored_sample row that matches the ordered sample
            """
            match = None

            # Note:  There are a group of biobank ordered samples in RDR that have two different biobank stored sample
            # records (for the same test), one of which is missing a confirmed timestamp.  The reason for this in the
            # RDR data has not been determined, but suggests the biobank included the same sample in two different
            # manifests with inconsistent confirmed details. So, override a match without a confirmed timestamp with a
            # match that has one, if found
            for sample in stored_samples:
                if sample.test == ordered_sample.test and sample.biobank_order_id == ordered_sample.order_id:
                    if not match or (not match.confirmed and sample.confirmed):
                        match = sample

            return match

        def _make_sample_dict_from_row(bss=None, bos=None, bo_pk=None, idx=None):
            """"
            Internal helper routine to populate a sample dict entry from the available ordered sample and
            stored sample information.
            :param bss:   A biobank_stored_sample row
            :param bos:   A biobank_ordered_sample row
            :param bo_pk: The primary key value for the biobank order.
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

            # Create a unique repeatable primary key value for each biobank sample record.
            test_id = BIOBANK_UNIQUE_TEST_IDS[test] if test in BIOBANK_UNIQUE_TEST_IDS else '99'
            if bo_pk:
                # From known biobank orders
                id_ = int(f'{bo_pk}{test_id}{idx}')
            else:
                # For unknown biobank orders, use participant_id + 99 + test id.
                # All tests here will be grouped together under this 99 id.
                id_ = int(f'{bss.participant_id}99{test_id}{idx}')

            # Create a hash integer value that will fit in a 32-bit data field, as an alternate unique id.
            if bss:
                hash_str = f'{id_}{bss.test}{bss.created}{bss.biobank_stored_sample_id}{bss.family_id}'.encode('utf-8')
            else:
                hash_str = f'{id_}'.encode('utf-8')
            hash_id = int(str(int(hashlib.sha512(hash_str).hexdigest()[:12], 16))[:9])

            return {
                'id': id_,
                'hash_id': hash_id,
                'biobank_stored_sample_id': bss.biobank_stored_sample_id if bss else None,
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
           select bo.participant_id, bo.biobank_order_id, bo.created, bo.order_status,
                   bo.collected_site_id, (select google_group from site where site.site_id = bo.collected_site_id) as collected_site,
                   bo.processed_site_id, (select google_group from site where site.site_id = bo.processed_site_id) as processed_site,
                   bo.finalized_site_id, (select google_group from site where site.site_id = bo.finalized_site_id) as finalized_site,
                   bo.finalized_time,
                   case when bmko.id is not null then 1 else 2 end as collection_method
             from biobank_order bo left outer join biobank_mail_kit_order bmko on bmko.biobank_order_id = bo.biobank_order_id
             where bo.participant_id = :p_id
             order by bo.created desc;
         """

        # SQL to collect all the ordered samples associated with a biobank order
        _biobank_ordered_samples_sql = """
            select bo.participant_id, bo.biobank_order_id, bos.*
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
                (select p.participant_id from participant p where p.biobank_id = bss.biobank_id) as participant_id,
                (select distinct boi.biobank_order_id from
                   biobank_order_identifier boi where boi.`value` = bss.biobank_order_identifier
                ) as biobank_order_id,
                bss.*
            from biobank_stored_sample bss
            where bss.biobank_id = :bb_id
            order by biobank_order_id, bss.test, bss.created;
        """

        data = {}
        orders = list()
        activity = list()
        # Find all biobank orders associated with this participant
        cursor = ro_session.execute(_biobank_orders_sql, {'p_id': p_id})
        biobank_orders = [r for r in cursor]
        # Create a unique identifier for each biobank order. This uid must be repeatable, so we sort by 'created'.
        # This unique biobank order id will be used as the prefix of the unique id for each biobank sample record.
        # Note: This is why every database table should have an 'id' integer field as the primary key, so we don't
        #       have to fudge up a primary key value in code.
        bbo_pks = dict()
        bbo_tmp = [[bo.participant_id, bo.biobank_order_id, bo.created] for bo in biobank_orders]
        sorted(bbo_tmp, key=lambda i: i[2])
        for x in range(len(bbo_tmp)):
            # bo pk = participant_id + order index left padded 2 zeros.
            bbo_pks[bbo_tmp[x][1]] = int(f'{bbo_tmp[x][0]}{str(x).zfill(2)}')

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
            # Count the number of DNA and Baseline tests in this order.
            dna_tests = 0
            baseline_tests = 0
            # RDR has a small number of biobank_order records (mail kit salivary orders) without a related
            # biobank_ordered_sample record, but with biobank_stored_sample records.  Make sure those stored samples
            # are included with the participant's biobank_order data
            idx = 0
            if len(bos_results) == 0 and len(bss_results) > 0:
                for bss in bss_results:
                    if bss.biobank_order_id == row.biobank_order_id:
                        idx += 1
                        bbo_samples.append(_make_sample_dict_from_row(
                                bss=bss, bos=None, bo_pk=bbo_pks[row.biobank_order_id], idx=idx))
                        stored_count += 1
            # PDR-400: There are about 20 participants that have less ordered samples than stored samples.
            elif len(bss_results) > 0 and len(bos_results) < len(bss_results):
                for bss in bss_results:
                    if bss.biobank_order_id == row.biobank_order_id:
                        idx += 1
                        bbo_samples.append(_make_sample_dict_from_row(
                                bss=bss, bos=bos_results[0], bo_pk=bbo_pks[row.biobank_order_id], idx=idx))
                        stored_count += 1
            else:
                for ordered_sample in bos_results:
                    idx += 1
                    # Look for a matching stored sample result based on the biobank order id and test type
                    # from the ordered sample record, to add to the order's list of samples
                    stored_sample = _get_stored_sample_row(bss_results, ordered_sample)
                    bbo_samples.append(_make_sample_dict_from_row(
                            bss=stored_sample, bos=ordered_sample, bo_pk=bbo_pks[row.biobank_order_id], idx=idx))
                    if stored_sample:
                        stored_count += 1

            for test in bbo_samples:
                if test['dna_test'] == 1:
                    dna_tests += 1
                # PDR-134:  Add baseline tests counts
                if test['baseline_test'] == 1:
                    baseline_tests += 1

            # PDR-243:  calculate an UNSET or FINALIZED OrderStatus to include with the biobank order data.  Aligns
            # with how RDR summarizes biospecimen details in participant_summary.biospecimen_* fields.  Intended to
            # replace the need for a separate BQPDRBiospecimenSchema nested field in the participant data once PDR
            # users update their queries to use the biobank order data instead of the biospec data.
            bb_order_status = BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET
            if row.finalized_time and bb_order_status != BiobankOrderStatus.CANCELLED:
                finalized_status = OrderStatus.FINALIZED
            else:
                finalized_status = OrderStatus.UNSET

            order = {
                'id': bbo_pks[row.biobank_order_id],
                'biobank_order_id': row.biobank_order_id,
                'created': row.created,
                'status': str(bb_order_status),
                'status_id': int(bb_order_status),
                'collection_method': str(SampleCollectionMethod(row.collection_method)),
                'collection_method_id': int(SampleCollectionMethod(row.collection_method)),
                'collected_site': row.collected_site,
                'collected_site_id': row.collected_site_id,
                'processed_site': row.processed_site,
                'processed_site_id': row.processed_site_id,
                'finalized_site': row.finalized_site,
                'finalized_site_id': row.finalized_site_id,
                'finalized_time': row.finalized_time,
                'finalized_status': str(finalized_status),
                'finalized_status_id': int(finalized_status),
                'tests_ordered': len(bos_results),
                'tests_stored': stored_count,
                'samples': bbo_samples,
                'isolate_dna': dna_tests,
                'isolate_dna_confirmed': 0,  # Fill in below.
                'baseline_tests': baseline_tests,
                'baseline_tests_confirmed': 0  # Fill in below.
            }
            orders.append(order)

        # Add any "orderless" stored samples for this participant.  They will all be associated with a
        # pseudo order with an order id of 'UNSET'
        if len(bss_missing_orders):
            orderless_stored_samples = list()
            idx = 0
            for bss_row in bss_missing_orders:
                idx += 1
                sr = _make_sample_dict_from_row(bss=bss_row, bos=None, idx=idx)
                if sr not in orderless_stored_samples:  # Don't put duplicates in samples list.
                    orderless_stored_samples.append(sr)

            order = {
                'id': int(f'{bss_missing_orders[0].participant_id}99'),
                'finalized_time': None,
                'biobank_order_id': 'UNSET',
                'collection_method': str(SampleCollectionMethod.UNSET),
                'collection_method_id': int(SampleCollectionMethod.UNSET),
                'tests_stored': len(orderless_stored_samples),
                'samples': orderless_stored_samples,
                'isolate_dna': sum([r['dna_test'] for r in orderless_stored_samples]),
                'isolate_dna_confirmed': 0,  # Fill in below.
                'baseline_test': sum([r['baseline_test'] for r in orderless_stored_samples]),
                'baseline_tests_confirmed': 0  # Fill in below.
            }
            orders.append(order)

        if len(orders) > 0:
            data['biobank_orders'] = orders

            # Calculate confirmed tests and save activity events.
            for order in orders:
                act_key = ParticipantEventEnum.BiobankOrder
                act_ts = order['finalized_time']
                if act_ts:
                    act_key = ParticipantEventEnum.BiobankShipped

                if order['samples']:
                    order['baseline_tests_confirmed'] = \
                        sum([r['baseline_test'] for r in order['samples'] if r['confirmed'] is not None])
                    order['isolate_dna_confirmed'] = \
                        sum([r['dna_test'] for r in order['samples'] if r['confirmed'] is not None])
                    # Find minimum confirmed date of DNA tests if we have any.
                    if order['isolate_dna']:
                        try:
                            tmp_ts = min([r['confirmed'] for r in order['samples'] if r['confirmed'] is not None])
                            act_key = ParticipantEventEnum.BiobankConfirmed
                            act_ts = tmp_ts
                        except ValueError:  # No confirmed timestamps in list.
                            pass

                activity.append(_act(act_ts, ActivityGroupEnum.Biobank, act_key,
                    **{'dna_tests': order['isolate_dna_confirmed'],
                       'baseline_tests': order['baseline_tests_confirmed']}))

            data['activity'] = activity  # Record participant activity events

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

    # Leaving PDR calculations for EnrollmentStatusV2 enabled temporarily during Goal 1 transition, for QC/debugging
    # See _prep_participant_profile() for integration of RDR-calculated fields into the PDR participant_data record
    def _calculate_enrollment_status(self, summary, p_id):
        """
        Calculate the participant's enrollment status
        :param summary: summary data
        :param p_id:  (int) participant ID
        :return: dict
        """
        # Verify activity timestamps are correct.
        activity = self.validate_activity_timestamps(summary['activity'], p_id)
        # Make sure activity has been sorted by timestamp before we run the enrollment status calculator.
        esc = EnrollmentStatusCalculator()
        esc.run(activity)

        # Support depreciated enrollment status field values.
        status = EnrollmentStatusV2.REGISTERED
        if esc.status == PDREnrollmentStatusEnum.Participant:
            status = EnrollmentStatusV2.PARTICIPANT
        elif esc.status == PDREnrollmentStatusEnum.ParticipantPlusEHR:
            status = EnrollmentStatusV2.FULLY_CONSENTED
        elif esc.status == PDREnrollmentStatusEnum.CoreParticipantMinusPM:
            status = EnrollmentStatusV2.CORE_MINUS_PM
        elif esc.status == PDREnrollmentStatusEnum.CoreParticipant:
            status = EnrollmentStatusV2.CORE_PARTICIPANT

        data = {
            # Fields from EnrollmentStatusCalculator results.
            'enrl_status': esc.status.name,
            'enrl_status_id': esc.status.value,
            'enrl_registered_time': esc.registered_time,
            'enrl_participant_time': esc.participant_time,
            'enrl_participant_plus_ehr_time': esc.participant_plus_ehr_time,
            'enrl_core_participant_minus_pm_time': esc.core_participant_minus_pm_time,
            'enrl_core_participant_time': esc.core_participant_time,
            # TODO: PDR-calculated fields that can be deprecated / moved to _prep_participant_profile after goal 1 QC
            'enrollment_status': str(status),
            'enrollment_status_id': int(status),
            'enrollment_member': esc.participant_time,
            'enrollment_core_minus_pm': esc.core_participant_minus_pm_time

        }

        # Calculate age at consent.
        if isinstance(data['enrl_participant_time'], datetime.datetime) and \
                    'date_of_birth' in summary and isinstance(summary['date_of_birth'], datetime.date):
            rd = relativedelta(data['enrl_participant_time'], summary['date_of_birth'])
            data['age_at_consent'] = rd.years

        return data

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
                   and order['status_id'] != int(BiobankOrderStatus.CANCELLED) and 'samples' in order:
                    for sample in order['samples']:
                        if 'finalized' in sample and sample['finalized'] and \
                         isinstance(sample['finalized'], datetime.datetime):
                            dates.append(datetime_to_date(sample['finalized']))
        dates = list(set(dates))  # de-dup list
        data['distinct_visits'] = len(dates)
        return data

    def _check_for_test_credentials(self, summary):
        """
        Check if this participant is a test participant or not based on email or phone number values
        that are only supposed to be used for test participant creation.  Note:  test participant status
        is primarily determined by checking RDR participant table fields (is_ghost_id, is_test_participant, or HPO
        pairing to TEST) which is done in _prep_participant().  This method is only called if it was not already
        determined by those primary indicators that the participant is a test participant.
        :param summary: summary data
        :return: dict
        """
        test_participant = 0
        # Test if @example.com is in email address.
        if '@example.com' in (summary.get('email') or ''):
            test_participant = 1
        else:
            # Check for SMS phone number for test participants.  To mirror RDR, the phone number verification
            # has an order of precedence between login_phone_number and phone_number values and only the
            # login_phone_number is verified if it exists
            # See questionnaire_response_dao.py:
            #   # switch account to test account if the phone number starts with 4442
            #   # this is a requirement from PTSC
            #    ph = getattr(participant_summary, 'loginPhoneNumber') or \
            #        getattr(participant_summary, 'phoneNumber') or 'None'
            phone = summary.get('login_phone_number', None) or summary.get('phone_number', None) or 'None'
            if phone and re.sub('[\(|\)|\-|\s]', '', phone).startswith(TEST_LOGIN_PHONE_NUMBER_PREFIX):
                test_participant = 1

        data = {'test_participant': test_participant}
        return data

    @staticmethod
    def get_module_answers(ro_dao, module, p_id, qr_id=None, return_responses=False):
        """
        Retrieve the questionnaire module answers for the given participant id.  This retrieves all responses to
        the module and applies/layers the answers from each response to the final data dict returned.
        :param ro_dao: Readonly ro_dao object
        :param module: Module name
        :param p_id: participant id.
        :param qr_id: questionnaire response id
        :param return_responses:  Return the responses (unlayered) in addition to the processed answer data
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
            WHERE qr.participant_id = :p_id and qc.code_id in (select c1.code_id from code c1 where c1.value = :mod)
                AND qr.classification_type != 1
            ORDER BY qr.created;
        """

        _answers_sql = """
            SELECT qr.questionnaire_id,
                   qra.question_id,
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
            WHERE qr.questionnaire_response_id = :qr_id
                  and (qra.ignore is null or qra.ignore = 0)
            -- Order by question and the calculated answer so duplicates can be caught when results are processed
            ORDER BY qra.question_id, answer
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
                # Save answers into data dict.  Ignore duplicate answers to the same question from the same response
                # (See: questionnaire_response_id 680418686 as an example)
                last_question_code_id = None
                last_answer = None
                skipped_duplicates = 0
                for qnan in qnans:
                    if last_question_code_id == qnan.code_id and last_answer == qnan.answer:
                        skipped_duplicates += 1
                        continue
                    else:
                        last_question_code_id = qnan.code_id
                        last_answer = qnan.answer

                    # For question codes with multiple distinct responses, created comma-separated list of answers
                    if qnan.answer:
                        if qnan.code_name in data:
                            data[qnan.code_name] += f',{qnan.answer}'
                        else:
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
                if skipped_duplicates:
                    logging.warning('Questionnaire response {0} contained {1} duplicate answers. Please investigate' \
                                    .format(row.questionnaire_response_id, skipped_duplicates))

        # Apply answers to data dict, response by response, until we reach the end or the specific response id.
        data = dict()
        unlayered_codes = _unlayered_question_codes_map.get(module, [])
        for questionnaire_response_id, qnans in answers.items():
            # This excludes the layering of prior answers to certain question codes if they do not exist in the more
            # recent response
            for q_code in unlayered_codes:
                if q_code in data.keys() and q_code not in qnans.keys():
                    del data[q_code]

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

    def _find_consent_response(self, response_rec: BQRecord, module: str):
        """
        Look for the participant provided answer value that determines consent status for a module
        :param response_rec: A BQRecord object derived from a QuestionnaireResponse record
        :param module:  The module name (e.g., EHRConsentPII, GROR...)
        :returns: answer_code, consent_status
                  answer_code:  answer code string value
                  consent_status:  BQModuleStatusEnum
        """
        # If a module doesn't have defined consent questions that determine status, it defaults to SUBMITTED status
        if _consent_module_question_map[module] is None:
            return None, BQModuleStatusEnum.SUBMITTED

        answer_code = None

        consent_question_codes = _consent_module_question_map[module]
        code_list = [consent_question_codes, ] if isinstance(consent_question_codes, str) else consent_question_codes
        for code in code_list:
            answer_code = response_rec.get(code, None)
            if answer_code:
                break

        consent_status = _consent_answer_status_map.get(answer_code, None) if answer_code else None
        if not consent_status:
            if module == 'EHRConsentPII':
                # PDR-979:  Match RDR by defaulting to SUBMITTED_NO_CONSENT for (sensitive) EHRConsentPII if
                # there was not a recognized "yes" consent answer
                consent_status = BQModuleStatusEnum.SUBMITTED_NO_CONSENT
            else:
                # For other modules/cases where there wasn't any recognized consent answer found in the payload
                # (not even PMI_SKIP, which maps to UNSET)
                consent_status = BQModuleStatusEnum.SUBMITTED_INVALID

        return answer_code, consent_status

    @staticmethod
    def is_replay(prev_data_dict, prev_answer_hash,
                  curr_data_dict, curr_answer_hash,
                  ignore_keys=[]):
        """
        Compares two module or consent data dictionaries to identify replayed responses
        Replayed/resent responses are usually the result of trying to resolve a data issue, and are basically
        duplicate QuestionnaireResponse payloads except for differing creation timestamps and questionnaire response ids

        :param prev_data_dict: previous response data dictionary to compare
        :param curr_data_dict: current response data dictionary to compare
        :param curr_answer_hash: value from the current response's QuestionnaireResponse.answerHash field
        :param prev_answer_hash: value from the previous response's QuestionnaireResponse.answerHash field
        :param ignore_keys:  List of dict fields that should be excluded from matching.  E.g., created timestamp field
        :return:  Boolean, True if the dictionaries match on everything except the excluded keys
        """
        # Confirm both data dictionaries are populated
        if not bool(prev_data_dict) or not bool(curr_data_dict):
            return False

        # Validate we're comparing "like" dictionaries (disregarding keys to be ignored)
        prev_data_dict_keys = sorted(list(k for k in prev_data_dict.keys() if k not in ignore_keys))
        data_dict_keys = sorted(list(k for k in curr_data_dict.keys() if k not in ignore_keys))
        if prev_data_dict_keys != data_dict_keys:
            return False

        # Remaining key/value pairs must match for the two dictionaries
        for key in data_dict_keys:
            if curr_data_dict[key] != prev_data_dict[key]:
                return False

        # PDR-484:  Still possible for two responses to match on all the key/value pairs we include in the
        # PDR participant module dicts (summary/metadata details), but have distinct response JSON payloads.  Can use
        # the QuestionnaireResponse.answerHash values to confirm non-replays, provided both responses had an answerHash
        # calculated for them
        if prev_answer_hash and curr_answer_hash and (prev_answer_hash != curr_answer_hash):
            return False

        return True

    def find_questionnaire_response_id(self, ro_session, p_id, module,
                        classification_type: QuestionnaireResponseClassificationType, lookup_type: ModuleLookupEnum):
        """
        Find the requested questionnaire response id(s) for the given arguments.
        :param ro_session: Read only sql alchemy session
        :param p_id: Participant ID
        :param module: Survey Module ID, IE: "TheBasics"
        :param classification_type: QuestionnaireResponseClassificationType
        :param lookup_type:
        :return: questionnaire_response_id, list of questionnaire_response_ids or None
        """
        #       Due to the existence of responses with duplicate 'authored' and 'created' timestamps, we
        #       also include 'external_id' in the order by clause.
        sql = """
            select qr.questionnaire_response_id
            from questionnaire_response qr
                inner join questionnaire_concept qc on qr.questionnaire_id = qc.questionnaire_id
                inner join code c on qc.code_id = c.code_id
            where qr.participant_id = :p_id and c.value = :module and qr.classification_type = :class_type
            order by qr.authored, qr.created, qr.external_id"""

        args = { 'p_id': p_id, 'module': module, 'class_type': int(classification_type) }
        results = ro_session.execute(sql, args)
        # Create distinct list of questionnaire_response_ids and preserve order
        qr_ids = list(dict.fromkeys([r.questionnaire_response_id for r in results]))

        if not qr_ids:
            return None
        if lookup_type == ModuleLookupEnum.FIRST:
            return qr_ids[0]
        elif lookup_type == ModuleLookupEnum.LAST:
            return qr_ids[-1]

        return qr_ids

    def _calculate_ubr(self, p_id, summary, ro_session):
        """
        Calculate the UBR values for this participant
        :param p_id: participant id.
        :param summary: summary data
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        data = dict()
        # Return if participant has not yet submitted a primary consent response.
        if summary.get('enrl_status_id', 0) < PDREnrollmentStatusEnum.Participant:
            return data

        #### ConsentPII UBR calculations.
        # ubr_geography
        addresses = summary.get('addresses', [])
        consent_date = summary.get('enrl_participant_time', None)
        if addresses and consent_date:
            zip_code = None
            for addr in addresses:
                if addr['addr_type_id'] == StreetAddressTypeEnum.RESIDENCE.value:
                    zip_code = addr.get('addr_zip', None)
                    break
            data['ubr_geography'] = ubr.ubr_geography(consent_date.date(), zip_code)
        # ubr_age_at_consent
        data['ubr_age_at_consent'] = \
            ubr.ubr_age_at_consent(summary.get('enrl_participant_time', None), summary.get('date_of_birth', None))
        # ubr_overall - This should be calculated here in case there is no TheBasics response available.
        data['ubr_overall'] = ubr.ubr_overall(data)

        #### TheBasics UBR calculations.
        # Note: Due to PDR-484 we can't rely on the summary having a record for each valid submission so we
        #       are going to do our own query to get the first TheBasics submission after consent.
        # As of RDR 1.113.1, can filter on new classification_type to filter on full (COMPLETE) TheBasics surveys
        qr_id = self.find_questionnaire_response_id(
            ro_session, p_id, "TheBasics", QuestionnaireResponseClassificationType.COMPLETE, ModuleLookupEnum.FIRST)
        if not qr_id:
            return data

        qnan = self.get_module_answers(self.ro_dao, 'TheBasics', p_id=p_id, qr_id=qr_id)

        # ubr_sex
        data['ubr_sex'] = ubr.ubr_sex(qnan.get('BiologicalSexAtBirth_SexAtBirth', None))
        # ubr_sexual_orientation
        data['ubr_sexual_orientation'] = ubr.ubr_sexual_orientation(qnan.get('TheBasics_SexualOrientation', None))
        # ubr_gender_identity
        data['ubr_gender_identity'] = ubr.ubr_gender_identity(qnan.get('BiologicalSexAtBirth_SexAtBirth', None),
            qnan.get('Gender_GenderIdentity', None), qnan.get('Gender_CloserGenderDescription', None))
        # ubr_sexual_gender_minority
        data['ubr_sexual_gender_minority'] = \
            ubr.ubr_sexual_gender_minority(data['ubr_sexual_orientation'], data['ubr_gender_identity'])
        # ubr_ethnicity
        data['ubr_ethnicity'] = ubr.ubr_ethnicity(qnan.get('Race_WhatRaceEthnicity', None))
        # ubr_education
        data['ubr_education'] = ubr.ubr_education(qnan.get('EducationLevel_HighestGrade', None))
        # ubr_income
        data['ubr_income'] = ubr.ubr_income(qnan.get('Income_AnnualIncome', None))
        # ubr_disability
        data['ubr_disability'] = ubr.ubr_disability(qnan)
        # ubr_overall
        data['ubr_overall'] = ubr.ubr_overall(data)

        return data

    @staticmethod
    def generate_primary_consent_metrics(p_id, ro_session=None):
        """
        Rebuild the PDR consent metrics records for a participant's primary consent(s).  Invalid DOB/age at consent
        errors need to be checked for resolution when participant data is rebuilt, in case a new ConsentPII with a
        different DOB value has been received.
        :param p_id:  Participant id
        :param ro_session:  Active DB session for running a read-only query (optional)
        """
        dao = None
        if not ro_session:
            dao = ResourceDataDao(backup=True)
            ro_session = dao.session()

        # Only need to regenerate metrics if there are already primary consent PDF validation results (could still be
        # pending if this is a newly consented participant), with certain statuses pertinent to the metrics.  This
        # query is almost always limited to 2 or fewer results (a few dozen outlier pids have more)
        sql = """
             select id from consent_file
             where participant_id = :p_id and type = :consent_type
                   and sync_status in :status_filter
        """
        args = {'p_id': p_id, 'consent_type': int(ConsentType.PRIMARY),
                'status_filter': [int(ConsentSyncStatus.NEEDS_CORRECTING), int(ConsentSyncStatus.READY_FOR_SYNC),
                                      int(ConsentSyncStatus.SYNC_COMPLETE)]}
        results = ro_session.execute(sql, args)

        if results:
            consent_file_ids = [r.id for r in results]
            if len(consent_file_ids):
                res_gen = generators.ConsentMetricGenerator(ro_dao=dao)
                # Transform into consent metrics records for PDR (resource_data table/resource API only, not in BQ)
                validation_results = res_gen.get_consent_validation_records(id_list=consent_file_ids)
                for row in validation_results:
                    res = res_gen.make_resource(row.id, consent_validation_rec=row)
                    res.save()

def rebuild_participant_summary_resource(p_id, res_gen=None, patch_data=None, qc_mode=False):
    """
    Rebuild a resource record for a specific participant
    :param p_id: participant id
    :param res_gen: ParticipantSummaryGenerator object
    :param patch_data: dict of resource values to update/insert.
    :param qc_mode: If True, the resource data will be generated and returned but not saved to the database
    :return:
    """
    # Allow for batch requests to rebuild participant summary data.
    if not res_gen:
        res_gen = ParticipantSummaryGenerator()

    # See if this is a partial update.
    if patch_data:
        if isinstance(patch_data, dict):
            res_gen.patch_resource(p_id, patch_data)
            return patch_data
        else:
            logging.error('Participant Generator: Invalid patch data, nothing done.')

    res = res_gen.make_resource(p_id, qc_mode=qc_mode)
    if not qc_mode:
        res.save()

    return res

def participant_summary_update_resource_task(p_id):
    """
    Cloud task to update the Participant Summary record for the given participant.
    :param p_id: Participant ID
    """
    rebuild_participant_summary_resource(p_id)
