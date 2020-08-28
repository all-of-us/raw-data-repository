import datetime
import logging
import re

from dateutil import parser, tz
from sqlalchemy import func, desc, exc
from werkzeug.exceptions import NotFound

from rdr_service import config
from rdr_service.code_constants import CONSENT_GROR_YES_CODE, CONSENT_PERMISSION_YES_CODE, CONSENT_PERMISSION_NO_CODE, \
    DVEHR_SHARING_QUESTION_CODE, EHR_CONSENT_QUESTION_CODE, DVEHRSHARING_CONSENT_CODE_YES, GROR_CONSENT_QUESTION_CODE, \
    EHR_CONSENT_EXPIRED_YES, UNKNOWN_BIOBANK_ORDER_ID
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
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.participant_enums import EnrollmentStatusV2, WithdrawalStatus, WithdrawalReason, SuspensionStatus, \
    SampleStatus, BiobankOrderStatus, PatientStatusFlag, ParticipantCohortPilotFlag
from rdr_service.resource.helpers import DateCollection

_consent_module_question_map = {
    # module: question code string
    'ConsentPII': None,
    'DVEHRSharing': 'DVEHRSharing_AreYouInterested',
    'EHRConsentPII': 'EHRConsentPII_ConsentPermission',
    'GROR': 'ResultsConsent_CheckDNA',
    'PrimaryConsentUpdate': 'Reconsent_ReviewConsentAgree'
}

# _consent_expired_question_map must contain every module ID from _consent_module_question_map.
_consent_expired_question_map = {
    'ConsentPII': None,
    'DVEHRSharing': None,
    'EHRConsentPII': 'EHRConsentPII_ConsentExpired',
    'GROR': None,
    'PrimaryConsentUpdate': None
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
            # calculate enrollment status times
            summary = self._merge_schema_dicts(summary, self._calculate_enrollment_timestamps(summary))
            # calculate distinct visits
            summary = self._merge_schema_dicts(summary, self._calculate_distinct_visits(summary))
            # calculate test participant status
            summary = self._merge_schema_dicts(summary, self._calculate_test_participant(summary))

            return BQRecord(schema=BQParticipantSummarySchema, data=summary, convert_to_enum=convert_to_enum)

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

        data = {
            'first_name': qnan.get('PIIName_First'),
            'middle_name': qnan.get('PIIName_Middle'),
            'last_name': qnan.get('PIIName_Last'),
            'date_of_birth': qnan.get('PIIBirthInformation_BirthDate'),
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
                modules.append({
                    'mod_module': module_name,
                    'mod_baseline_module': 1 if module_name in self._baseline_modules else 0,  # Boolean field
                    'mod_authored': row.authored,
                    'mod_created': row.created,
                    'mod_language': row.language,
                    'mod_status': BQModuleStatusEnum.SUBMITTED.name,
                    'mod_status_id': BQModuleStatusEnum.SUBMITTED.value,
                })

                # check if this is a module with consents.
                if module_name not in _consent_module_question_map:
                    continue

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
                    if module_name == 'ConsentPII':
                        consent['consent'] = 'ConsentPII'
                        consent['consent_id'] = self._lookup_code_id('ConsentPII', ro_session)
                        consent['consent_value'] = 'ConsentPermission_Yes'
                        consent['consent_value_id'] = self._lookup_code_id('ConsentPermission_Yes', ro_session)
                    else:
                        consent['consent_value'] = qnan.get(_consent_module_question_map[module_name], None)
                        consent['consent_value_id'] = self._lookup_code_id(
                            qnan.get(_consent_module_question_map[module_name], None), ro_session)
                        consent['consent_expired'] = \
                            qnan.get(_consent_expired_question_map[module_name] or 'None', None)

                    consents.append(consent)

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
        Look up biobank orders / stored samples
        :param p_id: participant id
        :param p_bb_id:  participant's biobank id
        :param ro_session: Readonly DAO session object
        :return:
        """

        def _make_stored_sample_dict_from_row(bss_row, has_order=True):
            """
            Internal helper routine to populate a stored sample dict entry from a biobank_stored_sample
            table query result row
            """
            return {
                'bbs_test': bss_row.test,
                'bbs_baseline_test': 1 if bss_row.test in self._baseline_sample_test_codes else 0,  # Boolean field
                'bbs_dna_test': 1 if bss_row.test in self._dna_sample_test_codes else 0,  # Boolean field
                'bbs_collected': bss_row.collected if has_order else None,
                'bbs_processed': bss_row.processed if has_order else None,
                'bbs_finalized': bss_row.finalized if has_order else None,
                'bbs_confirmed': bss_row.bb_confirmed,
                'bbs_status': str(SampleStatus.RECEIVED) if bss_row.bb_confirmed else None,
                'bbs_status_id': int(SampleStatus.RECEIVED) if bss_row.bb_confirmed else None,
                'bbs_created': bss_row.bb_created,
                'bbs_disposed': bss_row.bb_disposed,
                'bbs_disposed_reason': str(SampleStatus(bss_row.bb_status)) if bss_row.bb_status else None,
                'bbs_disposed_reason_id': int(SampleStatus(bss_row.bb_status)) if bss_row.bb_status else None,
            }

        # SQL to find total number of biobank stored samples associated with the participant
        _stored_samples_count_sql = """
             select count(*) from biobank_stored_sample where biobank_id = :bb_id;
          """

        # SQL to generate a list of biobank orders and counts of ordered and stored samples.
        _biobank_orders_sql = """
           select bo.biobank_order_id, bo.created, bo.order_status,
                   bo.collected_site_id, (select google_group from site where site.site_id = bo.collected_site_id) as collected_site,
                   bo.processed_site_id, (select google_group from site where site.site_id = bo.processed_site_id) as processed_site,
                   bo.finalized_site_id, (select google_group from site where site.site_id = bo.finalized_site_id) as finalized_site,
                   case when exists (
                     select bdo.participant_id from biobank_dv_order bdo
                        where bdo.biobank_order_id = bo.biobank_order_id) then 1 else 0 end as dv_order,
                   (select count(1) from biobank_ordered_sample bos2
                        where bos2.order_id = bo.biobank_order_id) as tests_ordered,
                   (select count(1) from biobank_stored_sample bss2
                        where bss2.biobank_order_identifier = boi.`value`) as tests_stored
             from biobank_order bo left outer join biobank_order_identifier boi on bo.biobank_order_id = boi.biobank_order_id
             where boi.`system` = 'https://www.pmi-ops.org' and bo.participant_id = :pid
             order by bo.created desc;
         """

        # SQL to collect all the ordered samples tests and stored sample tests.
        _biobank_order_samples_sql = """
            select bos.test, bos.collected, bos.processed, bos.finalized, bo.order_status,
                   bss.confirmed as bb_confirmed, bss.created as bb_created, bss.disposed as bb_disposed,
                   bss.status as bb_status
            from biobank_order bo inner join biobank_order_identifier boi on bo.biobank_order_id = boi.biobank_order_id
                 left join biobank_ordered_sample bos on bo.biobank_order_id = bos.order_id
                 left join biobank_stored_sample bss on boi.`value` = bss.biobank_order_identifier and bos.test = bss.test
             where boi.`system` = 'https://www.pmi-ops.org' and bss.biobank_order_identifier = boi.value
                and bo.biobank_order_id = :order_id
        """

        # Used when there are more ordered tests than stored tests.
        _biobank_ordered_samples_sql = """
          select bos.test, bos.collected, bos.processed, bos.finalized, bo.order_status,
                   null as bb_confirmed, null as bb_created, null as bb_disposed, null as bb_status
            from biobank_order bo left join biobank_ordered_sample bos on bo.biobank_order_id = bos.order_id
             where bo.biobank_order_id = :order_id;
        """

        # Used when there are less ordered tests than stored tests.
        _biobank_stored_samples_sql = """
            select
                bss.test, bss.confirmed as bb_confirmed, bss.created as bb_created, bss.disposed as bb_disposed,
                   bss.status as bb_status
            from biobank_order bo inner join biobank_order_identifier boi on bo.biobank_order_id = boi.biobank_order_id
                 left join biobank_stored_sample bss on boi.`value` = bss.biobank_order_identifier
             where boi.`system` = 'https://www.pmi-ops.org' and bo.biobank_order_id = :order_id;
        """

        # SQL to find stored samples for the participant that are not associated with a biobank order
        # See: https://precisionmedicineinitiative.atlassian.net/browse/PDR-89. This will only be executed in
        # a small number of cases where a participant has "unknown order" samples
        _samples_without_biobank_order_sql = """
                select bss.test, bss.confirmed as bb_confirmed, bss.created as bb_created, bss.disposed as bb_disposed,
                       bss.status as bb_status, bo.biobank_order_id as bbo_id
                  from biobank_stored_sample bss
                  left outer join biobank_order_identifier boi on bss.biobank_order_identifier = boi.`value`
                  left outer join biobank_order bo on boi.biobank_order_id = bo.biobank_order_id
                where boi.`system` = 'https://www.pmi-ops.org'
                    and bss.biobank_id = :bb_id and bo.biobank_order_id is null;
             """

        data = {}
        orders = list()
        stored_samples_added = 0

        # Find known biobank orders associated with this participant
        cursor = ro_session.execute(_biobank_orders_sql, {'pid': p_id})
        results = [r for r in cursor]
        # loop through results and create one order record for each biobank_order_id value.
        for row in results:
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
                'bbo_tests_ordered': row.tests_ordered,
                'bbo_tests_stored': row.tests_stored,
                'bbo_samples': list()
            }

            # Query for all samples that have a matching ordered sample test and stored sample test.
            cursor = ro_session.execute(_biobank_order_samples_sql, {'order_id': row.biobank_order_id})
            s_results = [r for r in cursor]
            for s_row in s_results:
                order['bbo_samples'].append(_make_stored_sample_dict_from_row(s_row, has_order=True))
                stored_samples_added += 1

            orders.append(order)

        # Check if this participant has additional stored samples that were not added to the data dict above
        # Occurs when the results for the biobank orders query (above) misses samples without an associated order
        # See https://precisionmedicineinitiative.atlassian.net/browse/PDR-89
        if stored_samples_added < ro_session.execute(_stored_samples_count_sql, {'bb_id': p_bb_id}).scalar():
            # Include any "unknown order" samples associated with this participant
            cursor = ro_session.execute(_samples_without_biobank_order_sql, {'bb_id': p_bb_id})
            samples = [s for s in cursor]
            if len(samples):
                orders.append({'bbo_biobank_order_id': UNKNOWN_BIOBANK_ORDER_ID, 'bbo_samples': list()})
                for row in samples:
                    orders[-1]['bbo_samples'].append(_make_stored_sample_dict_from_row(row, has_order=False))

        # Check to see that we have captured all of the stored samples for the orders.
        # About 20% of the biobank orders have mis-matched ordered and stored sample counts.
        for order in orders:
            if order['bbo_tests_stored'] == 0 or order['bbo_tests_ordered'] == order['bbo_tests_stored']:
                continue
            # Fill in any missing ordered sample test records.
            if order['bbo_tests_ordered'] > order['bbo_tests_stored'] and \
                        len(order['bbo_samples']) < order['bbo_tests_ordered']:
                cursor = ro_session.execute(_biobank_ordered_samples_sql, {'order_id': order['bbo_biobank_order_id']})
                results = [r for r in cursor]
                existing_tests = [sample['bbs_test'] for sample in order['bbo_samples']]
                for row in results:
                    if row.test in existing_tests:
                        continue
                    order['bbo_samples'].append(_make_stored_sample_dict_from_row(row, has_order=True))
            # Fill in any missing stored sample test records.
            if order['bbo_tests_ordered'] < order['bbo_tests_stored']:
                cursor = ro_session.execute(_biobank_stored_samples_sql, {'order_id': order['bbo_biobank_order_id']})
                results = [r for r in cursor]
                existing_tests = [sample['bbs_test'] for sample in order['bbo_samples']]
                for row in results:
                    if row.test in existing_tests:
                        continue
                    order['bbo_samples'].append(_make_stored_sample_dict_from_row(row, has_order=False))

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

        data['enrollment_status'] = str(status)
        data['enrollment_status_id'] = int(status)
        if status > EnrollmentStatusV2.REGISTERED:
            data['enrollment_member'] = \
                enrollment_member_time if enrollment_member_time != datetime.datetime.max else None

        return data

    def _calculate_enrollment_timestamps(self, summary):
        """
        Calculate all enrollment status timestamps, based on calculate_max_core_sample_time() method in
        participant summary dao.
        :param summary: summary data
        :return: dict
        """
        if summary['enrollment_status_id'] != int(EnrollmentStatusV2.CORE_PARTICIPANT):
            return {}

        # Calculate the earliest ordered sample and stored sample times.
        ordered_time = stored_time = datetime.datetime.max
        stored_sample_times = dict(zip(self._dna_sample_test_codes, [
            {
                'confirmed': datetime.datetime.min, 'confirmed_count': 0,
                'disposed': datetime.datetime.min, 'disposed_count': 0
            } for i in range(0, 5)]))  # pylint: disable=unused-variable

        for bbo in summary['biobank_orders']:
            if not bbo['bbo_samples']:
                continue

            for bboi in bbo['bbo_samples']:
                if bboi['bbs_dna_test'] == 1:
                    ordered_time = min(ordered_time, bboi['bbs_finalized'] or datetime.datetime.max)
                    # See: participant_summary_dao.py:calculate_max_core_sample_time() and
                    #       _participant_summary_dao.py:126
                    sst = stored_sample_times[bboi['bbs_test']]
                    if bboi['bbs_confirmed']:
                        sst['confirmed'] = max(sst['confirmed'], bboi['bbs_confirmed'])
                        sst['confirmed_count'] += 1
                    if bboi['bbs_disposed']:
                        sst['disposed'] = max(sst['disposed'], bboi['bbs_disposed'])
                        sst['disposed_count'] += 1

        sstl = list()
        for k, v in stored_sample_times.items():  # pylint: disable=unused-variable
            if v['confirmed_count'] != v['disposed_count']:
                ts = v['confirmed']
            else:
                ts = v['disposed']
            if ts != datetime.datetime.min:
                sstl.append(ts)

        if sstl:
            stored_time = min(sstl)

        data = {
            'enrollment_core_ordered': ordered_time if ordered_time != datetime.datetime.max else None,
            'enrollment_core_stored': stored_time if stored_time != datetime.datetime.max else None
        }
        if ordered_time == datetime.datetime.max and stored_time == datetime.datetime.max:
            return data

        # If we have ordered or stored sample times, ensure that it is not before the alt_time value.
        alt_time = max(
            summary.get('enrollment_member', datetime.datetime.min),
            max(mod['mod_created'] or datetime.datetime.min for mod in summary['modules']
                if mod['mod_baseline_module'] == 1),
            max(pm['pm_finalized'] or datetime.datetime.min for pm in summary['pm'])
            if 'pm' in summary else datetime.datetime.min
        )

        if data['enrollment_core_ordered']:
            data['enrollment_core_ordered'] = max(ordered_time, alt_time)
        if data['enrollment_core_stored']:
            data['enrollment_core_stored'] = max(stored_time, alt_time)

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
                if pm['pm_status_id'] != int(PhysicalMeasurementsStatus.CANCELLED) and pm['pm_finalized']:
                    dates.append(datetime_to_date(pm['pm_finalized']))

        if 'biobank_orders' in summary:
            for order in summary['biobank_orders']:
                if order['bbo_biobank_order_id'] != UNKNOWN_BIOBANK_ORDER_ID and \
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
        Retrieve the most recent questionnaire module answers for the given participant id.
        :param ro_dao: Readonly ro_dao object
        :param module: Module name
        :param p_id: participant id.
        :param qr_id: questionnaire response id
        :return: dict
        """
        _module_info_sql = """
                        SELECT qr.questionnaire_id,
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
                        ORDER BY qr.created DESC;
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

        if not ro_dao:
            ro_dao = BigQuerySyncDao(backup=True)

        with ro_dao.session() as session:
            results = session.execute(_module_info_sql, {"p_id": p_id, "mod": module})
            if not results:
                return None

            # Match the specific questionnaire response id otherwise return answers for the most recent response.
            for row in results:
                if qr_id and row.questionnaire_response_id != qr_id:
                    continue

                data = ro_dao.to_dict(row, result_proxy=results)
                answers = session.execute(_answers_sql, {'qr_id': row.questionnaire_response_id})

                for answer in answers:
                    data[answer.code_name] = answer.answer
                return data

        return None


def rebuild_bq_participant(p_id, ps_bqgen=None, pdr_bqgen=None, project_id=None):
    """
    Rebuild a BQ record for a specific participant
    :param p_id: participant id
    :param ps_bqgen: BQParticipantSummaryGenerator object
    :param pdr_bqgen: BQPDRParticipantSummaryGenerator object
    :param project_id: Project ID override value.
    :return:
    """
    # Allow for batch requests to rebuild participant summary data.
    if not ps_bqgen:
        ps_bqgen = BQParticipantSummaryGenerator()
    if not pdr_bqgen:
        from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
        pdr_bqgen = BQPDRParticipantSummaryGenerator()

    ps_bqr = ps_bqgen.make_bqrecord(p_id)

    # Since the PDR participant summary is primarily a subset of the Participant Summary, call the full
    # Participant Summary generator and take what we need from it.
    pdr_bqr = pdr_bqgen.make_bqrecord(p_id, ps_bqr=ps_bqr)

    w_dao = BigQuerySyncDao()

    with w_dao.session() as w_session:

        # save the participant summary record.
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
