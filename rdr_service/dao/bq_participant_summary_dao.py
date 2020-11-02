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
    CONSENT_COPE_DEFERRED_CODE
)
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_pdr_participant_summary import BQPDRParticipantSummary
from rdr_service.model.bq_participant_summary import BQParticipantSummarySchema, BQStreetAddressTypeEnum, \
    BQModuleStatusEnum, BQParticipantSummary, COHORT_1_CUTOFF, COHORT_2_CUTOFF, BQConsentCohort
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import PhysicalMeasurements, PhysicalMeasurementsStatus
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_cohort_pilot import ParticipantCohortPilot
# TODO:  Using participant_summary as a workaround.  Replace with new participant_profile when it's available
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.participant_enums import EnrollmentStatusV2, WithdrawalStatus, WithdrawalReason, SuspensionStatus, \
    SampleStatus, BiobankOrderStatus, PatientStatusFlag, ParticipantCohortPilotFlag, EhrStatus
from rdr_service.resource.helpers import DateCollection


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
    CONSENT_COPE_DEFERRED_CODE: BQModuleStatusEnum.SUBMITTED_NOT_SURE,
    'ConsentAncestryTraits_Yes': BQModuleStatusEnum.SUBMITTED,
    'ConsentAncestryTraits_No': BQModuleStatusEnum.SUBMITTED_NO_CONSENT,
    'ConsentAncestryTraits_NotSure': BQModuleStatusEnum.SUBMITTED_NOT_SURE
}

class BQParticipantSummaryGenerator(BigQueryGenerator):
    """
    Generate a Participant Summary BQRecord object
    """
    ro_dao = None
    # Retrieve module and sample test lists from config.
    _baseline_modules = [mod.replace('questionnaireOn', '')
                         for mod in config.getSettingList('baseline_ppi_questionnaire_fields')]
    _baseline_sample_test_codes = config.getSettingList('baseline_sample_test_codes')
    _dna_sample_test_codes = config.getSettingList('dna_sample_test_codes')

    def make_bqrecord(self, p_id, convert_to_enum=False):
        """
        Build a Participant Summary BQRecord object for the given participant id.
        :param p_id: participant id
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=True)

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
            # calculate test participant status
            summary = self._merge_schema_dicts(summary, self._calculate_test_participant(summary))

            return BQRecord(schema=BQParticipantSummarySchema, data=summary, convert_to_enum=convert_to_enum)

    def patch_bqrecord(self, p_id, data):
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

        args = {'pid': p_id, 'table_id': 'participant_summary', 'modified': datetime.datetime.utcnow()}
        for k, v in data.items():
            args[f'p_{k}'] = v

        sql = f"""
            update bigquery_sync
                set modified = :modified, resource = json_set(resource, {sql_json_set_values})
               where pk_id = :pid and table_id = :table_id
        """
        dao = BigQuerySyncDao(backup=False)
        with dao.session() as session:
            session.execute(sql, args)

            sql = 'select resource from bigquery_sync where pk_id = :pid and table_id = :table_id limit 1'

            rec = session.execute(sql, args).first()
            if rec:
                return BQRecord(schema=BQParticipantSummarySchema, data=json.loads(rec.resource),
                                convert_to_enum=False)
        return None

    def _prep_participant(self, p_id, ro_session):
        """
        Get the information from the participant record
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        # Note: We need to be careful here, there is a delay from when a participant is inserted in the primary DB
        # and when it shows up in the replica DB instance.
        p = ro_session.query(Participant).filter(Participant.participantId == p_id).first()
        if not p:
            msg = f'Participant lookup for P{p_id} failed.'
            logging.error(msg)
            raise NotFound(msg)

        hpo = ro_session.query(HPO.name).filter(HPO.hpoId == p.hpoId).first()
        organization = ro_session.query(Organization.externalId). \
            filter(Organization.organizationId == p.organizationId).first()

        withdrawal_status = WithdrawalStatus(p.withdrawalStatus)
        withdrawal_reason = WithdrawalReason(p.withdrawalReason if p.withdrawalReason else 0)
        suspension_status = SuspensionStatus(p.suspensionStatus)

        # The cohort_2_pilot_flag field values in participant_summary were set via a one-time backfill based on a
        # list of participant IDs provided by PTSC and archived in the participant_cohort_pilot table.  See:
        # https://precisionmedicineinitiative.atlassian.net/browse/DA-1622
        # TO DO:  A participant_profile table may be implemented as part of the effort to eliminate dependencies on
        # participant_summary.  The cohort_2_pilot_flag could be queried from that new table in the future
        #
        # Note this query assumes participant_cohort_pilot only contains entries for the cohort 2 pilot
        # participants for genomics and has not been used for identifying participants in more recent pilots
        cohort_2_pilot = ro_session.query(ParticipantCohortPilot.participantCohortPilot). \
            filter(ParticipantCohortPilot.participantId == p_id).first()

        cohort_2_pilot_flag = \
            ParticipantCohortPilotFlag.COHORT_2_PILOT if cohort_2_pilot else ParticipantCohortPilotFlag.UNSET

        data = {
            'participant_id': p_id,
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
            'cohort_2_pilot_flag': str(cohort_2_pilot_flag),
            'cohort_2_pilot_flag_id': int(cohort_2_pilot_flag)
        }

        return data

    def _prep_participant_profile(self, p_id, ro_session):
        """
        Get additional participant status fields that were incorporated into the RDR participant_summary
        but can't be derived from information in other RDR tables.  Example is EHR status information which is
        read from a curation dataset by a daily cron job that then updates participant_summary directly.
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
                              ParticipantSummary.enrollmentStatusCoreStoredSampleTime) \
            .filter(ParticipantSummary.participantId == p_id).first()

        if not ps:
            logging.debug(f'No participant_summary record found for {p_id}')
        else:
            # SqlAlchemy may return None for our zero-based NOT_PRESENT EhrStatus Enum, so map None to NOT_PRESENT
            # See rdr_service.model.utils Enum decorator class
            ehr_status = EhrStatus.NOT_PRESENT if ps.ehrStatus is None else ps.ehrStatus
            data = {
                'ehr_status': str(ehr_status),
                'ehr_status_id': int(ehr_status),
                'ehr_receipt': ps.ehrReceiptTime,
                'ehr_update': ps.ehrUpdateTime,
                'enrollment_core_ordered': ps.enrollmentStatusCoreOrderedSampleTime,
                'enrollment_core_stored': ps.enrollmentStatusCoreStoredSampleTime
            }

        return data

    def _prep_consentpii_answers(self, p_id):
        """
        Get participant information from the ConsentPII questionnaire
        :param p_id: participant id
        :return: dict
        """
        qnans = self.get_module_answers(self.ro_dao, 'ConsentPII', p_id)
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
            ]
        }

        return data

    def _prep_modules(self, p_id, ro_session):
        """
        Find all questionnaire modules the participant has completed and loop through them.
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        code_id_query = ro_session.query(func.max(QuestionnaireConcept.codeId)). \
            filter(QuestionnaireResponse.questionnaireId ==
                   QuestionnaireConcept.questionnaireId).label('codeId')
        query = ro_session.query(
            QuestionnaireResponse.questionnaireResponseId, QuestionnaireResponse.authored,
            QuestionnaireResponse.created, QuestionnaireResponse.language, code_id_query). \
            filter(QuestionnaireResponse.participantId == p_id). \
            order_by(QuestionnaireResponse.authored)
        # sql = self.ro_dao.query_to_text(query)
        results = query.all()

        data = {
            'consent_cohort': BQConsentCohort.UNSET.name,
            'consent_cohort_id': BQConsentCohort.UNSET.value
        }
        modules = list()
        consents = list()
        consent_dt = None

        if results:
            for row in results:
                module_name = self._lookup_code_value(row.codeId, ro_session)
                module_data = {
                    'mod_module': module_name,
                    'mod_baseline_module': 1 if module_name in self._baseline_modules else 0,  # Boolean field
                    'mod_authored': row.authored,
                    'mod_created': row.created,
                    'mod_language': row.language,
                }
                # Default status, may be updated based on consent answer
                module_status = BQModuleStatusEnum.SUBMITTED

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
                        qnan = BQRecord(schema=None, data=qnans)
                        consent = {
                            'consent': _consent_module_question_map[module_name],
                            'consent_id': self._lookup_code_id(_consent_module_question_map[module_name], ro_session),
                            'consent_date': parser.parse(qnan['authored']).date() if qnan['authored'] else None,
                            'consent_module': module_name,
                            'consent_module_authored': row.authored,
                            'consent_module_created': row.created,
                        }
                        # Note:  Based on currently available modules when a module has no
                        # associated answer options (like ConsentPII or ProgramUpdate), any submitted response is given
                        # animplicit ConsentPermission_Yes value.   May need adjusting if there are ever modules where
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
                            # Check for a specific submittal status based on the answer value (default to SUBMITTED)
                            module_status = _consent_answer_status_map.get(consent_value, None)
                            if not module_status:
                                logging.warning(
                                   f'Defaulting {module_name} answer {consent_value} to status SUBMITTED (pid: {p_id}) '
                                )
                                # TODO: SUBMITTED is what all module statuses used to default to, so will use the same
                                # default in cases where there was no known value in the map for the answer code.
                                # May want to reconsider if this should be something else (UNSET?)
                                module_status = BQModuleStatusEnum.SUBMITTED

                        consents.append(consent)

                module_data['mod_status'] = module_status.name
                module_data['mod_status_id'] = module_status.value
                modules.append(module_data)


        if len(modules) > 0:
            # remove any duplicate modules and consents because of replayed responses.
            data['modules'] = [dict(t) for t in {tuple(d.items()) for d in modules}]
            if len(consents) > 0:
                data['consents'] = [dict(t) for t in {tuple(d.items()) for d in consents}]
                # keep consents in order if dates need to be checked, sort by 'consent_module_authored' desc.
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
        qnans = self.ro_dao.call_proc('sp_get_questionnaire_answers', args=['TheBasics', p_id])
        if not qnans or len(qnans) == 0:
            return {}

        # get race question answers
        qnan = BQRecord(schema=None, data=qnans[0])  # use only most recent questionnaire.
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

        query = ro_session.query(PhysicalMeasurements.created, PhysicalMeasurements.createdSiteId,
                                 PhysicalMeasurements.final,
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
                'pm_status': str(pm_status),
                'pm_status_id': int(pm_status),
                'pm_created': row.created,
                'pm_created_site': self._lookup_site_name(row.createdSiteId, ro_session),
                'pm_created_site_id': row.createdSiteId,
                'pm_finalized': row.finalized,
                'pm_finalized_site': self._lookup_site_name(row.finalizedSiteId, ro_session),
                'pm_finalized_site_id': row.finalizedSiteId,
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
                'bbs_test': test,
                'bbs_baseline_test': 1 if test in self._baseline_sample_test_codes else 0,  # Boolean field
                'bbs_dna_test': 1 if test in self._dna_sample_test_codes else 0,  # Boolean field
                'bbs_confirmed': stored_confirmed,
                'bbs_status': str(SampleStatus.RECEIVED) if stored_confirmed else None,
                'bbs_status_id': int(SampleStatus.RECEIVED) if stored_confirmed else None,
                'bbs_collected': bos.collected if bos else None,
                'bbs_processed': bos.processed if bos else None,
                'bbs_finalized': bos.finalized if bos else None,
                'bbs_created': bss.created if bss else None,
                'bbs_disposed': bss.disposed if bss else None,
                'bbs_disposed_reason': str(SampleStatus(stored_status)) if stored_status else None,
                'bbs_disposed_reason_id': int(SampleStatus(stored_status)) if stored_status else None,
            }

        # SQL to generate a list of biobank orders associated with a participant
        _biobank_orders_sql = """
           select bo.biobank_order_id, bo.created, bo.order_status,
                   bo.collected_site_id, (select google_group from site where site.site_id = bo.collected_site_id) as collected_site,
                   bo.processed_site_id, (select google_group from site where site.site_id = bo.processed_site_id) as processed_site,
                   bo.finalized_site_id, (select google_group from site where site.site_id = bo.finalized_site_id) as finalized_site,
                   case when exists (
                     select bdo.participant_id from biobank_mail_kit_order bdo
                        where bdo.biobank_order_id = bo.biobank_order_id and bo.participant_id = bdo.participant_id)
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
                'bbo_biobank_order_id': row.biobank_order_id,
                'bbo_created': row.created,
                'bbo_status': str(
                    BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET),
                'bbo_status_id': int(
                    BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET),
                'bbo_dv_order': row.dv_order,
                'bbo_collected_site': row.collected_site,
                'bbo_collected_site_id': row.collected_site_id,
                'bbo_processed_site': row.processed_site,
                'bbo_processed_site_id': row.processed_site_id,
                'bbo_finalized_site': row.finalized_site,
                'bbo_finalized_site_id': row.finalized_site_id,
                'bbo_tests_ordered': len(bos_results),
                'bbo_tests_stored': stored_count,
                'bbo_samples': bbo_samples
            }

            orders.append(order)

        # Add any "orderless" stored samples for this participant.  They will all be associated with a
        # "pseudo" order with an order id of 'UNSET'
        if len(bss_missing_orders):
            orderless_stored_samples = list()
            for bss_row in bss_missing_orders:
                orderless_stored_samples.append(_make_sample_dict_from_row(bss=bss_row, bos=None))

            orders.append({
                'bbo_biobank_order_id': 'UNSET',
                'bbo_tests_stored': len(orderless_stored_samples),
                'bbo_samples': orderless_stored_samples
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
            SELECT psh.created AS created,
                   psh.modified AS modified,
                   psh.authored AS authored,
                   psh.patient_status AS patient_status,
                   psh.hpo_id AS hpo_id,
                   (select t.name from hpo t where t.hpo_id = psh.hpo_id) as hpo_name,
                   psh.organization_id AS organization_id,
                   (select t.external_id from organization t where t.organization_id = psh.organization_id) AS organization_name,
                   psh.site_id AS site_id,
                   (select t.google_group from site t where t.site_id = psh.site_id) AS site_name
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
                    'site_id': row.site_id
                })
            data['patient_statuses'] = status_recs

        return data

    def _calculate_enrollment_status(self, summary):
        """
        Calculate the participant's enrollment status
        :param summary: summary data
        :return: dict:q
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
                if pm['pm_status_id'] == int(PhysicalMeasurementsStatus.COMPLETED) or \
                    (pm['pm_finalized'] and pm['pm_status_id'] != int(PhysicalMeasurementsStatus.CANCELLED)):
                    pm_complete = True
                    physical_measurements_date = \
                        min(physical_measurements_date, pm['pm_finalized'] or datetime.datetime.max)

        baseline_module_count = 0
        latest_baseline_module_completion = datetime.datetime.min
        completed_all_baseline_modules = False
        if 'modules' in summary:
            for module in summary['modules']:
                if module['mod_baseline_module'] == 1:
                    baseline_module_count += 1
                    latest_baseline_module_completion = \
                        max(latest_baseline_module_completion, module['mod_created'] or datetime.datetime.min)
            completed_all_baseline_modules = baseline_module_count >= len(self._baseline_modules)

        dna_sample_count = 0
        first_dna_sample_date = datetime.datetime.max
        bb_orders = summary.get('biobank_orders', list())
        for order in bb_orders:
            for sample in order.get('bbo_samples', list()):
                if sample['bbs_dna_test'] and sample['bbs_confirmed']:
                    dna_sample_count += 1
                    first_dna_sample_date = min(first_dna_sample_date, sample['bbs_created'] or datetime.datetime.max)

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
                    date_overlap = study_consent_date_range \
                        .get_intersection(pm_date_range) \
                        .get_intersection(baseline_modules_date_range) \
                        .get_intersection(dna_date_range) \
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
    #     # Calculate the earliest ordered sample and stored sample times.
    #     ordered_time = stored_time = datetime.datetime.max
    #     stored_sample_times = dict(zip(self._dna_sample_test_codes, [
    #         {
    #             'confirmed': datetime.datetime.min, 'confirmed_count': 0,
    #             'disposed': datetime.datetime.min, 'disposed_count': 0
    #         } for i in range(0, 5)]))  # pylint: disable=unused-variable
    #
    #     for bbo in summary['biobank_orders']:
    #         if not bbo['bbo_samples']:
    #             continue
    #
    #         for bboi in bbo['bbo_samples']:
    #             if bboi['bbs_dna_test'] == 1:
    #                 # See: biobank_order_dao.py:_set_participant_summary_fields()
    #                 #      biobank_order_dao.py:_get_order_status_and_time()
    #                 if ordered_time == datetime.datetime.max:
    #                     ordered_time = (bboi['bbs_finalized'] or bboi['bbs_processed'] or
    #                                    bboi['bbs_collected'] or bbo['bbo_created'] or datetime.datetime.max)
    #                 # See: participant_summary_dao.py:calculate_max_core_sample_time() and
    #                 #       _participant_summary_dao.py:126
    #                 sst = stored_sample_times[bboi['bbs_test']]
    #                 if bboi['bbs_confirmed']:
    #                     sst['confirmed'] = max(sst['confirmed'], bboi['bbs_confirmed'])
    #                     sst['confirmed_count'] += 1
    #                 if bboi['bbs_disposed']:
    #                     sst['disposed'] = max(sst['disposed'], bboi['bbs_disposed'])
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
    #         max(mod['mod_created'] or datetime.datetime.min for mod in summary['modules']
    #             if mod['mod_baseline_module'] == 1),
    #         max(pm['pm_finalized'] or datetime.datetime.min for pm in summary['pm'])
    #         if 'pm' in summary else datetime.datetime.min
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
                if pm['pm_status_id'] != int(PhysicalMeasurementsStatus.CANCELLED) and pm['pm_finalized']:
                    dates.append(datetime_to_date(pm['pm_finalized']))

        if 'biobank_orders' in summary:
            for order in summary['biobank_orders']:
                if order['bbo_biobank_order_id'] != 'UNSET' and \
                   order['bbo_status_id'] != int(BiobankOrderStatus.CANCELLED) and 'bbo_samples' in order:
                    for sample in order['bbo_samples']:
                        if 'bbs_finalized' in sample and sample['bbs_finalized'] and \
                           isinstance(sample['bbs_finalized'], datetime.datetime):
                            dates.append(datetime_to_date(sample['bbs_finalized']))

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
            # Check to see if the participant is in the Test HPO.
            if (summary.get('hpo') or 'None').lower() == 'test':
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
    def get_module_answers(ro_dao, module, p_id, qr_id=None):
        """
        Retrieve the questionnaire module answers for the given participant id.  This retrieves all responses to
        the module and applies/layers the answers from each response to the final data dict returned.
        :param ro_dao: Readonly ro_dao object
        :param module: Module name
        :param p_id: participant id.
        :param qr_id: questionnaire response id
        :return: dict
        """
        _module_info_sql = """
            SELECT DISTINCT qr.questionnaire_id,
                   qr.questionnaire_response_id,
                   qr.created,
                   q.version,
                   qr.authored,
                   qr.language,
                   qr.participant_id
            FROM questionnaire_response qr
                    INNER JOIN questionnaire_concept qc on qr.questionnaire_id = qc.questionnaire_id
                    INNER JOIN questionnaire q on q.questionnaire_id = qc.questionnaire_id
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
            ro_dao = BigQuerySyncDao(backup=True)

        with ro_dao.session() as session:
            results = session.execute(_module_info_sql, {"p_id": p_id, "mod": module})
            if not results:
                return None

            # Query the answers for all responses found.
            for row in results:
                # Save parent record field values into data dict.
                data = ro_dao.to_dict(row, result_proxy=results)
                qnans = session.execute(_answers_sql, {'qr_id': row.questionnaire_response_id})
                # Save answers into data dict.
                for qnan in qnans:
                    data[qnan.code_name] = qnan.answer
                # Insert data dict into answers list.
                answers[row.questionnaire_response_id] = data

        # Apply answers to data dict, response by response, until we reach the end or the specific response id.
        data = dict()
        for questionnaire_response_id, qnans in answers.items():
            data.update(qnans)
            if qr_id and qr_id == questionnaire_response_id:
                break

        return data if data else None


def rebuild_bq_participant(p_id, ps_bqgen=None, pdr_bqgen=None, project_id=None, patch_data=None):
    """
    Rebuild a BQ record for a specific participant
    :param p_id: participant id
    :param ps_bqgen: BQParticipantSummaryGenerator object
    :param pdr_bqgen: BQPDRParticipantSummaryGenerator object
    :param project_id: Project ID override value.
    :param patch_data: dict of resource values to update/insert.
    :return:
    """
    # Allow for batch requests to rebuild participant summary data.
    if not ps_bqgen:
        ps_bqgen = BQParticipantSummaryGenerator()
    if not pdr_bqgen:
        from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
        pdr_bqgen = BQPDRParticipantSummaryGenerator()

    # See if this is a partial update.
    if patch_data and isinstance(patch_data, dict):
        ps_bqr = ps_bqgen.patch_bqrecord(p_id, patch_data)
    else:
        ps_bqr = ps_bqgen.make_bqrecord(p_id)

    # Since the PDR participant summary is primarily a subset of the Participant Summary, call the full
    # Participant Summary generator and take what we need from it.
    pdr_bqr = pdr_bqgen.make_bqrecord(p_id, ps_bqr=ps_bqr)

    w_dao = BigQuerySyncDao()

    with w_dao.session() as w_session:
        # save the participant summary record if this is a full rebuild.
        if not patch_data and isinstance(patch_data, dict):
            ps_bqgen.save_bqrecord(p_id, ps_bqr, bqtable=BQParticipantSummary, w_dao=w_dao, w_session=w_session,
                               project_id=project_id)
        # save the PDR participant summary record
        pdr_bqgen.save_bqrecord(p_id, pdr_bqr, bqtable=BQPDRParticipantSummary, w_dao=w_dao, w_session=w_session,
                                project_id=project_id)
        w_session.flush()

    return ps_bqr


def bq_participant_summary_update_task(p_id):
    """
    Cloud task to update the Participant Summary record for the given participant.
    :param p_id: Participant ID
    """
    rebuild_bq_participant(p_id)
