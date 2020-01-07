import datetime
import logging

from dateutil import parser, tz
from sqlalchemy import func, desc
from werkzeug.exceptions import NotFound

from rdr_service import config
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_pdr_participant_summary import BQPDRParticipantSummary
from rdr_service.model.bq_participant_summary import BQParticipantSummarySchema, BQStreetAddressTypeEnum, \
    BQModuleStatusEnum, BQParticipantSummary
from rdr_service.model.hpo import HPO
from rdr_service.model.measurements import PhysicalMeasurements, PhysicalMeasurementsStatus
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.participant_enums import EnrollmentStatus, WithdrawalStatus, WithdrawalReason, SuspensionStatus, \
    SampleStatus, BiobankOrderStatus


class BQParticipantSummaryGenerator(BigQueryGenerator):
    """
    Generate a Participant Summary BQRecord object
    """
    ro_dao = None
    _baseline_modules = ['TheBasics', 'OverallHealth', 'Lifestyle']
    _baseline_sample_test_codes = ["1ED04", "1ED10", "1HEP4", "1PST8", "2PST8", "1SST8", "2SST8",
                                    "1PS08", "1SS08", "1UR10", "1CFD9", "1PXR2", "1UR90", "2ED10"]
    _dna_sample_test_codes = ["1ED10", "2ED10", "1ED04", "1SAL", "1SAL2"]

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
            summary = self._merge_schema_dicts(summary, self._prep_consentpii_answers(p_id, ro_session))
            # prep questionnaire modules information, includes gathering extra consents.
            summary = self._merge_schema_dicts(summary, self._prep_modules(p_id, ro_session))
            # prep physical measurements
            summary = self._merge_schema_dicts(summary, self._prep_physical_measurements(p_id, ro_session))
            # prep race and gender
            summary = self._merge_schema_dicts(summary, self._prep_the_basics(p_id, ro_session))
            # prep biobank orders and samples
            summary = self._merge_schema_dicts(summary, self._prep_biobank_info(p_id, ro_session))
            # calculate enrollment status for participant
            summary = self._merge_schema_dicts(summary, self._calculate_enrollment_status(p_id, ro_session, summary))
            # calculate distinct visits
            summary = self._merge_schema_dicts(summary, self._calculate_distinct_visits(summary))

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
            'is_ghost_id': 1 if p.isGhostId is True else 0
        }

        return data

    def _prep_consentpii_answers(self, p_id, ro_session):
        """
        Get participant information from the ConsentPII questionnaire
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return: dict
        """
        # qnans = self.ro_dao.call_proc('sp_get_questionnaire_answers', args=['ConsentPII', p_id])
        qnans = self._get_module_answers('ConsentPII', p_id)
        if not qnans:
            # return the minimum data required when we don't have the questionnaire data.
            return {'email': None, 'is_ghost_id': 0}
        qnan = BQRecord(schema=None, data=qnans)  # use only most recent response.
        # TODO: We may need to use the first response to set consent dates,
        #  unless the consent value changed across response records.

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
            ],
            'consents': [
                {
                    'consent': 'ConsentPII',
                    'consent_id': self._lookup_code_id('ConsentPII', ro_session),
                    'consent_date': parser.parse(qnan.get('authored')).date() if qnan.get('authored') else None,
                    'consent_value': 'ConsentPermission_Yes',
                    'consent_value_id': self._lookup_code_id('ConsentPermission_Yes', ro_session),
                },
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
            order_by(QuestionnaireResponse.questionnaireResponseId)
        # sql = self.ro_dao.query_to_text(query)
        results = query.all()

        data = dict()
        modules = list()
        consents = list()

        consent_modules = {
            # module: question code string
            'DVEHRSharing': 'DVEHRSharing_AreYouInterested',
            'EHRConsentPII': 'EHRConsentPII_ConsentPermission',
        }

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
                if module_name not in consent_modules:
                    continue

                qnans = self._get_module_answers(module_name, p_id)
                if qnans:
                    qnan = BQRecord(schema=None, data=qnans)  # use only most recent questionnaire.
                    consents.append({
                        'consent': consent_modules[module_name],
                        'consent_id': self._lookup_code_id(consent_modules[module_name], ro_session),
                        'consent_date': parser.parse(qnan['authored']).date() if qnan['authored'] else None,
                        'consent_value': qnan.get(consent_modules[module_name], None),
                        'consent_value_id':
                            self._lookup_code_id(qnan.get(consent_modules[module_name], None), ro_session),
                    })

        if len(modules) > 0:
            data['modules'] = modules
            if len(consents) > 0:
                data['consents'] = consents

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

    def _prep_biobank_info(self, p_id, ro_session):
        """
        Look up biobank orders
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :return:
        """
        data = {}
        orders = list()

        sql = """
          select bo.biobank_order_id, bo.created, bo.collected_site_id, bo.processed_site_id, bo.finalized_site_id, 
                  bos.test, bos.collected, bos.processed, bos.finalized, bo.order_status,
                  bss.confirmed as bb_confirmed, bss.created as bb_created, bss.disposed as bb_disposed, 
                  bss.status as bb_status, (
                    select count(1) from biobank_dv_order bdo where bdo.biobank_order_id = bo.biobank_order_id
                  ) as dv_order
            from biobank_order bo inner join biobank_ordered_sample bos on bo.biobank_order_id = bos.order_id
                    inner join biobank_order_identifier boi on bo.biobank_order_id = boi.biobank_order_id
                    left outer join 
                      biobank_stored_sample bss on boi.`value` = bss.biobank_order_identifier and bos.test = bss.test
            where boi.`system` = 'https://www.pmi-ops.org' and bo.participant_id = :pid
            order by bo.biobank_order_id, bos.test;
        """

        cursor = ro_session.execute(sql, {'pid': p_id})
        results = [r for r in cursor]
        # loop through results and create one order record for each biobank_order_id value.
        for row in results:
            if not list(filter(lambda order: order['bbo_biobank_order_id'] == row.biobank_order_id, orders)):
                orders.append({
                    'bbo_biobank_order_id': row.biobank_order_id,
                    'bbo_created': row.created,
                    'bbo_status': str(
                        BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET),
                    'bbo_status_id': int(
                        BiobankOrderStatus(row.order_status) if row.order_status else BiobankOrderStatus.UNSET),
                    'bbo_dv_order': 0 if row.dv_order == 0 else 1,  # Boolean field
                    'bbo_collected_site': self._lookup_site_name(row.collected_site_id, ro_session),
                    'bbo_collected_site_id': row.collected_site_id,
                    'bbo_processed_site': self._lookup_site_name(row.processed_site_id, ro_session),
                    'bbo_processed_site_id': row.processed_site_id,
                    'bbo_finalized_site': self._lookup_site_name(row.finalized_site_id, ro_session),
                    'bbo_finalized_site_id': row.finalized_site_id,
                })
        # loop through results again and add each sample to it's order.
        for row in results:
            # get the order list index for this sample record
            try:
                idx = orders.index(
                        list(filter(lambda order: order['bbo_biobank_order_id'] == row.biobank_order_id, orders))[0])
            except IndexError:
                continue
            # if we haven't added any samples to this order, create an empty list.
            if 'bbo_samples' not in orders[idx]:
                orders[idx]['bbo_samples'] = list()
            # append the sample to the order
            orders[idx]['bbo_samples'].append({
                'bbs_test': row.test,
                'bbs_baseline_test': 1 if row.test in self._baseline_sample_test_codes else 0,  # Boolean field
                'bbs_dna_test': 1 if row.test in self._dna_sample_test_codes else 0,  # Boolean field
                'bbs_collected': row.collected,
                'bbs_processed': row.processed,
                'bbs_finalized': row.finalized,
                'bbs_confirmed': row.bb_confirmed,
                'bbs_status': str(SampleStatus.RECEIVED) if row.bb_confirmed else None,
                'bbs_status_id': int(SampleStatus.RECEIVED) if row.bb_confirmed else None,
                'bbs_created': row.bb_created,
                'bbs_disposed': row.bb_disposed,
                'bbs_disposed_reason': str(SampleStatus(row.bb_status)) if row.bb_status else None,
                'bbs_disposed_reason_id': int(SampleStatus(row.bb_status)) if row.bb_status else None,
            })

        if len(orders) > 0:
            data['biobank_orders'] = orders
        return data

    def _calculate_enrollment_status(self, p_id, ro_session, ro_summary):
        """
        Calculate the participant's enrollment status
        :param p_id: participant id
        :param ro_session: Readonly DAO session object
        :param ro_summary: summary data
        :return: dict
        """
        if 'consents' not in ro_summary:
            return {}

        study_consent = ehr_consent = dvehr_consent = pm_complete = False
        status = None
        # iterate over consents
        for consent in ro_summary['consents']:
            if consent['consent'] == 'ConsentPII':
                study_consent = True
            if consent['consent'] == 'EHRConsentPII_ConsentPermission' and \
                consent['consent_value'] == 'ConsentPermission_Yes':
                ehr_consent = True
            if consent['consent'] == 'DVEHRSharing_AreYouInterested' and \
                consent['consent_value'] == 'DVEHRSharing_Yes':
                dvehr_consent = True

        # check physical measurements
        if 'pm' in ro_summary:
            for pm in ro_summary['pm']:
                if pm['pm_status_id'] == int(PhysicalMeasurementsStatus.COMPLETED) or \
                    (pm['pm_finalized'] and pm['pm_status_id'] != int(PhysicalMeasurementsStatus.CANCELLED)):
                    pm_complete = True

        baseline_module_count = dna_sample_count = 0
        if 'modules' in ro_summary:
            baseline_module_count = len(
                list(filter(lambda module: module['mod_baseline_module'] == 1, ro_summary['modules'])))

        # It seems we have around 100 participants that BioBank has received and processed samples for
        # and RDR knows about them, but RDR has no record of the orders or which tests were ordered.
        # These participants can still count as Full/Core Participants, so we need to look at only what
        # is in the `biobank_stored_sample` table to calculate the enrollment status.
        # https://precisionmedicineinitiative.atlassian.net/browse/DA-812
        sql = """select bss.test from biobank_stored_sample bss
                    inner join participant p on bss.biobank_id = p.biobank_id
                    where p.participant_id = :pid"""

        cursor = ro_session.execute(sql, {'pid': p_id})
        results = [r for r in cursor]
        dna_sample_count = len(list(filter(lambda test: test[0] in self._baseline_sample_test_codes, results)))

        if study_consent:
            status = EnrollmentStatus.INTERESTED
        if ehr_consent or dvehr_consent:
            status = EnrollmentStatus.MEMBER
        if pm_complete and 'modules' in ro_summary and baseline_module_count >= len(self._baseline_modules) and \
            dna_sample_count > 0:
            status = EnrollmentStatus.FULL_PARTICIPANT

        # TODO: Get Enrollment dates for additional fields -> participant_summary_dao.py:499

        # TODO: Calculate EHR status and dates -> participant_summary_dao.py:707

        data = {
            'enrollment_status': str(status) if status else None,
            'enrollment_status_id': int(status) if status else None,
        }
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
                if order['bbo_status_id'] != int(BiobankOrderStatus.CANCELLED) and 'bbo_samples' in order:
                    for sample in order['bbo_samples']:
                        if 'bbs_finalized' in sample and sample['bbs_finalized'] and \
                            isinstance(sample['bbs_finalized'], datetime.datetime):
                            dates.append(datetime_to_date(sample['bbs_finalized']))

        dates = list(set(dates))  # de-dup list
        data['distinct_visits'] = len(dates)
        return data

    def _get_module_answers(self, module, p_id):
        """

        :param module:
        :param p_id:
        :return:
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
                    WHERE qr.participant_id = :p_id and qc.code_id = (select c1.code_id from code c1 where c1.value = :mod)
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

        self.ro_dao = BigQuerySyncDao(backup=True)
        with self.ro_dao.session() as session:
            results = session.execute(_module_info_sql, {"p_id": p_id, "mod": module})
            if not results:
                return None

            for row in results:
                data = self.ro_dao.to_dict(row, result_proxy=results)

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

    try:
        app_id = config.GAE_PROJECT
    except AttributeError:
        app_id = 'localhost'

    ps_bqr = ps_bqgen.make_bqrecord(p_id)

    # filter test or ghost participants if production
    if app_id == 'all-of-us-rdr-prod':  # or app_id == 'localhost':
        if ps_bqr.is_ghost_id == 1 or ps_bqr.hpo == 'TEST':
            return None

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
