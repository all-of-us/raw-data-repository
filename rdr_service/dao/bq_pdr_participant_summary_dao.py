import os

from rdr_service.model.bq_participant_summary import BQStreetAddressTypeEnum
from rdr_service.dao.bigquery_sync_dao import BigQueryGenerator, BigQuerySyncDao
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_pdr_participant_summary import BQPDRParticipantSummarySchema
from rdr_service.participant_enums import OrderStatus


class BQPDRParticipantSummaryGenerator(BigQueryGenerator):
    """
    Generate a PDR Participant Summary BQRecord object.
    This is a Participant Summary record without PII.
    Note: Logic to create a PDR Participant Summary is in bq_participant_summary_dao:rebuild_bq_participant.
    """
    ro_dao = None
    rural_zipcodes = None

    def make_bqrecord(self, p_id, convert_to_enum=False, ps_bqr=None):
        """
        Build a Participant Summary BQRecord object for the given participant id.
        :param p_id: participant id
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param ps_bqr: A BQParticipantSummary BQRecord object.
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=True)

        # Since we are primarily a subset of the Participant Summary, call the full Participant Summary generator
        # and take what we need from it.
        if not ps_bqr:
            ps_bqr = BQParticipantSummaryGenerator().make_bqrecord(p_id, convert_to_enum=convert_to_enum)
        bqr = BQRecord(schema=BQPDRParticipantSummarySchema, data=ps_bqr.to_dict(), convert_to_enum=convert_to_enum)

        if hasattr(bqr, 'addr_zip') and getattr(bqr, 'addr_zip'):
            setattr(bqr, 'addr_zip', getattr(bqr, 'addr_zip')[:3])

        summary = bqr.to_dict()
        # Populate BQPDRBiospecimenSchema if there are biobank orders.
        # TODO:  Deprecate this BQPDRBiospecimenSchema and transition PDR users to utilize BQBiobankOrderSchema data
        if hasattr(ps_bqr, 'biobank_orders'):
            data = {'biospec': list()}
            for order in ps_bqr.biobank_orders:
                # Count the number of DNA and Baseline tests in this order.
                dna_tests = 0
                dna_tests_confirmed = 0
                baseline_tests = 0
                baseline_tests_confirmed = 0
                for test in order.get('bbo_samples', list()):
                    if test['bbs_dna_test'] == 1:
                        dna_tests += 1
                        if test['bbs_confirmed']:
                            dna_tests_confirmed += 1
                    # PDR-134:  Add baseline tests counts
                    if test['bbs_baseline_test'] == 1:
                        baseline_tests += 1
                        if test['bbs_confirmed']:
                            baseline_tests_confirmed += 1

                # PDR-243:  Use an OrderStatus (not the order's BiobankOrderStatus value)
                # to align with the RDR biospecimen fields in participant summary.  TODO:  the BQPDRBiospecimenSchema
                # will be deprecated once PDR users migrate to using the BQBiobankOrderSchema data for queries
                data['biospec'].append({
                    'biosp_status': str(OrderStatus(order.get('bbo_finalized_status', OrderStatus.UNSET))),
                    'biosp_status_id': int(OrderStatus(order.get('bbo_finalized_status_id', OrderStatus.UNSET))),
                    'biosp_order_time': order.get('bbo_created', None),
                    'biosp_isolate_dna': dna_tests,
                    'biosp_isolate_dna_confirmed': dna_tests_confirmed,
                    'biosp_baseline_tests': baseline_tests,
                    'biosp_baseline_tests_confirmed': baseline_tests_confirmed,
                })


            summary = self._merge_schema_dicts(summary, data)

        # Calculate contact information
        summary = self._merge_schema_dicts(summary, self._set_contact_flags(ps_bqr))
        # Calculate UBR
        summary = self._merge_schema_dicts(summary, self._calculate_ubr(ps_bqr, p_id))

        bqr = BQRecord(schema=BQPDRParticipantSummarySchema, data=summary, convert_to_enum=convert_to_enum)
        return bqr

    def _import_rural_zipcodes(self):
        """
        Load the file app_data/rural_zipcodes.txt
        """
        self.rural_zipcodes = list()
        paths = ('app_data', 'rdr_service/app_data', 'rest-api/app_data')

        for path in paths:
            if os.path.exists(os.path.join(path, 'rural_zipcodes.txt')):
                with open(os.path.join(path, 'rural_zipcodes.txt')) as handle:
                    # pylint: disable=unused-variable
                    for count, line in enumerate(handle):
                        self.rural_zipcodes.append(line.split(',')[1].strip())
                break

    def _set_contact_flags(self, ps_bqr):
        """
        Determine if an email or phone number is available.
        :param ps_bqr: A BQParticipantSummary BQRecord object
        :return: dict
        """
        data = {
            'email_available': 1 if ps_bqr.email else 0,
            'phone_number_available': 1 if (getattr(ps_bqr, 'login_phone_number', None) or
                                            getattr(ps_bqr, 'phone_number', None)) else 0
        }
        return data

    def _calculate_ubr(self, ps_bqr, p_id):
        """
        Calculate the UBR values for this participant
        :param ps_bqr: A BQParticipantSummary BQRecord object.
        :param p_id: Participant ID
        :return: dict
        """
        # setup default values, all UBR values must be 0 or 1.
        data = {
            'ubr_sex': 0,
            'ubr_sexual_orientation': 0,
            'ubr_gender_identity': 0,
            'ubr_ethnicity': 0,
            'ubr_geography': 0,
            'ubr_education': 0,
            'ubr_income': 0,
            'ubr_sexual_gender_minority': 0,
            'ubr_age_at_consent': 0,
            'ubr_overall': 0,
        }
        birth_sex = 'unknown'

        # ubr_sex
        if hasattr(ps_bqr, 'sex') and ps_bqr.sex:
            birth_sex = ps_bqr.sex
            if ps_bqr.sex in ('SexAtBirth_SexAtBirthNoneOfThese', 'SexAtBirth_Intersex'):
                data['ubr_sex'] = 1

        # ubr_sexual_orientation
        if hasattr(ps_bqr, 'sexual_orientation') and ps_bqr.sexual_orientation:
            if ps_bqr.sexual_orientation not in ['SexualOrientation_Straight', 'PMI_PreferNotToAnswer']:
                data['ubr_sexual_orientation'] = 1

        # ubr_gender_identity
        if hasattr(ps_bqr, 'genders') and isinstance(ps_bqr.genders, list):
            data['ubr_gender_identity'] = 1  # easier to default to 1.
            if len(ps_bqr.genders) == 1 and (
                (ps_bqr.genders[0]['gender'] == 'GenderIdentity_Man' and birth_sex == 'SexAtBirth_Male') or
                (ps_bqr.genders[0]['gender'] == 'GenderIdentity_Woman' and birth_sex == 'SexAtBirth_Female') or
                ps_bqr.genders[0]['gender'] in ('PMI_Skip', 'PMI_PreferNotToAnswer')):
                data['ubr_gender_identity'] = 0

        # ubr_ethnicity
        if hasattr(ps_bqr, 'races') and ps_bqr.races:
            data['ubr_ethnicity'] = 1  # easier to default to 1.
            if len(ps_bqr.races) == 1 and \
                ps_bqr.races[0]['race'] in ('WhatRaceEthnicity_White', 'PMI_Skip', 'PMI_PreferNotToAnswer'):
                data['ubr_ethnicity'] = 0

        # ubr_geography
        if hasattr(ps_bqr, 'addresses') and isinstance(ps_bqr.addresses, list):
            for addr in ps_bqr.addresses:
                if addr['addr_type_id'] == BQStreetAddressTypeEnum.RESIDENCE.value:
                    data['addr_city'] = addr['addr_city']
                    data['addr_state'] = addr['addr_state']
                    data['addr_zip'] = addr['addr_zip'][:3] if addr['addr_zip'] else addr['addr_zip']
                    zipcode = addr['addr_zip']

                    # See if we need to import the rural zip code list.
                    if not self.rural_zipcodes:
                        self._import_rural_zipcodes()
                    if zipcode in self.rural_zipcodes:
                        data['ubr_geography'] = 1

        # ubr_education
        if hasattr(ps_bqr, 'education') and ps_bqr.education:
            if ps_bqr.education in (
                'HighestGrade_NeverAttended', 'HighestGrade_OneThroughFour', 'HighestGrade_NineThroughEleven',
                'HighestGrade_FiveThroughEight'):
                data['ubr_education'] = 1

        # ubr_income
        if hasattr(ps_bqr, 'income') and ps_bqr.income:
            if ps_bqr.income in ('AnnualIncome_less10k', 'AnnualIncome_10k25k'):
                data['ubr_income'] = 1

        # ubr_sexual_gender_minority
        if data['ubr_sex'] == 1 or data['ubr_gender_identity'] == 1:
            data['ubr_sexual_gender_minority'] = 1

        # ubr_disability
        qnans = BQParticipantSummaryGenerator.get_module_answers(self.ro_dao, 'TheBasics', p_id)
        data['ubr_disability'] = 0
        if qnans:
            if qnans.get('Employment_EmploymentStatus') == 'EmploymentStatus_UnableToWork' or \
                    qnans.get('Disability_Blind') == 'Blind_Yes' or \
                    qnans.get('Disability_WalkingClimbing') == 'WalkingClimbing_Yes' or \
                    qnans.get('Disability_DressingBathing') == 'DressingBathing_Yes' or \
                    qnans.get('Disability_ErrandsAlone') == 'ErrandsAlone_Yes' or \
                    qnans.get('Disability_Deaf') == 'Deaf_Yes' or \
                    qnans.get('Disability_DifficultyConcentrating') == 'DifficultyConcentrating_Yes':
                data['ubr_disability'] = 1

        # ubr_age_at_consent
        if hasattr(ps_bqr, 'date_of_birth') and ps_bqr.date_of_birth and \
                ps_bqr.consents and len(ps_bqr.consents) > 0:

            consent_date = None
            for consent_type in ['ConsentPII', 'EHRConsentPII_ConsentPermission', 'DVEHRSharing_AreYouInterested']:
                if consent_date:
                    break
                for consent in ps_bqr.consents:
                    if consent['consent'] == consent_type and \
                                consent['consent_value'] in ['ConsentPermission_Yes', 'DVEHRSharing_Yes']:
                        consent_date = consent['consent_date']
                        break

            if consent_date:
                # PDR-261:  Should not use years (integer) alone;  anyone older than 65 by even a day should be
                # flagged as UBR, so use float
                age = (consent_date - ps_bqr.date_of_birth).days / 365
                if not 18.0 <= age <= 65.0:
                    data['ubr_age_at_consent'] = 1

        # pylint: disable=unused-variable
        for key, value in data.items():
            if value == 1:
                data['ubr_overall'] = 1
                break

        return data
