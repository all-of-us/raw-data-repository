#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import os

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.model.bq_base import BQRecord
from rdr_service.resource import generators, schemas
from rdr_service.resource.generators.participant import ParticipantSummaryGenerator
from resource.schemas.participant import StreetAddressTypeEnum


class PDRParticipantSummaryGenerator(generators.BaseGenerator):
    """
    Generate a Participant Summary Resource object
    """
    ro_dao = None
    rural_zipcodes = None

    def make_resource(self, p_id, ps_res=None):
        """
        Build a Participant Summary BQRecord object for the given participant id.
        :param p_id: participant id        
        :param ps_res: A BQParticipantSummary BQRecord object.
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=True)

        # Since we are primarily a subset of the Participant Summary, call the full Participant Summary generator
        # and take what we need from it.
        if not ps_res:
            ps_res = ParticipantSummaryGenerator().make_resource(p_id)
        res = BQRecord(schema=None, data=ps_res.get_data())

        if hasattr(res, 'addresses') and res.addresses:
            for address in res.addresses:
                if address['addr_type_id'] == StreetAddressTypeEnum.RESIDENCE.value:
                    setattr(res, 'addr_state', address['addr_state'])
                    if address['addr_zip']:
                        setattr(res, 'addr_zip', address['addr_zip'][:3])

        summary = res.to_dict()
        # Populate BQAnalyticsBiospecimenSchema if there are biobank orders.
        if hasattr(ps_res, 'biobank_orders'):
            data = {'biospec': list()}
            for order in ps_res.biobank_orders:
                # Count the number of DNA and Baseline tests in this order.
                dna_tests = 0
                dna_tests_confirmed = 0
                baseline_tests = 0
                baseline_tests_confirmed = 0
                for test in order.get('samples', list()):
                    if test['dna_test'] == 1:
                        dna_tests += 1
                        if test['confirmed']:
                            dna_tests_confirmed += 1
                    # PDR-134:  Add baseline tests counts
                    if test['baseline_test'] == 1:
                        baseline_tests += 1
                        if test['confirmed']:
                            baseline_tests_confirmed += 1

                data['biospec'].append({
                    'status': order.get('status', None),
                    'status_id': order.get('status_id', None),
                    'order_time': order.get('created', None),
                    'isolate_dna': dna_tests,
                    'isolate_dna_confirmed': dna_tests_confirmed,
                    'baseline_tests': baseline_tests,
                    'baseline_tests_confirmed': baseline_tests_confirmed
                })

            summary = self._merge_schema_dicts(summary, data)

        # Calculate contact information
        summary = self._merge_schema_dicts(summary, self._set_contact_flags(res))
        # Calculate UBR
        summary = self._merge_schema_dicts(summary, self._calculate_ubr(res, p_id))

        return generators.ResourceRecordSet(schemas.PDRParticipantSummarySchema, summary)


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

    def _set_contact_flags(self, res):
        """
        Determine if an email or phone number is available.
        :param res: A BQParticipantSummary BQRecord object
        :return: dict
        """
        data = {
            'email_available': 1 if getattr(res, 'email', None) else 0,
            'phone_number_available': 1 if (getattr(res, 'login_phone_number', None) or
                                            getattr(res, 'phone_number', None)) else 0
        }
        return data

    def _calculate_ubr(self, res, p_id):
        """
        Calculate the UBR values for this participant
        :param res: A BQParticipantSummary BQRecord object.
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
        if hasattr(res, 'sex') and res.sex:
            birth_sex = res.sex
            if res.sex in ('SexAtBirth_SexAtBirthNoneOfThese', 'SexAtBirth_Intersex'):
                data['ubr_sex'] = 1

        # ubr_sexual_orientation
        if hasattr(res, 'sexual_orientation') and res.sexual_orientation:
            if res.sexual_orientation not in ['SexualOrientation_Straight', 'PMI_PreferNotToAnswer']:
                data['ubr_sexual_orientation'] = 1

        # ubr_gender_identity
        if hasattr(res, 'genders') and isinstance(res.genders, list):
            data['ubr_gender_identity'] = 1  # easier to default to 1.
            if len(res.genders) == 1 and (
                (res.genders[0]['gender'] == 'GenderIdentity_Man' and birth_sex == 'SexAtBirth_Male') or
                (res.genders[0]['gender'] == 'GenderIdentity_Woman' and birth_sex == 'SexAtBirth_Female') or
                res.genders[0]['gender'] in ('PMI_Skip', 'PMI_PreferNotToAnswer')):
                data['ubr_gender_identity'] = 0

        # ubr_ethnicity
        if hasattr(res, 'races') and res.races:
            data['ubr_ethnicity'] = 1  # easier to default to 1.
            if len(res.races) == 1 and \
                res.races[0]['race'] in ('WhatRaceEthnicity_White', 'PMI_Skip', 'PMI_PreferNotToAnswer'):
                data['ubr_ethnicity'] = 0

        # ubr_geography
        if hasattr(res, 'addresses') and isinstance(res.addresses, list):
            for addr in res.addresses:
                if addr['addr_type_id'] == StreetAddressTypeEnum.RESIDENCE.value:
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
        if hasattr(res, 'education') and res.education:
            if res.education in (
                'HighestGrade_NeverAttended', 'HighestGrade_OneThroughFour', 'HighestGrade_NineThroughEleven',
                'HighestGrade_FiveThroughEight'):
                data['ubr_education'] = 1

        # ubr_income
        if hasattr(res, 'income') and res.income:
            if res.income in ('AnnualIncome_less10k', 'AnnualIncome_10k25k'):
                data['ubr_income'] = 1

        # ubr_sexual_gender_minority
        if data['ubr_sex'] == 1 or data['ubr_gender_identity'] == 1:
            data['ubr_sexual_gender_minority'] = 1

        # ubr_disability
        qnans = ParticipantSummaryGenerator.get_module_answers(self.ro_dao, 'TheBasics', p_id)
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
        if hasattr(res, 'date_of_birth') and res.date_of_birth and \
            res.consents and len(res.consents) > 0:

            consent_date = None
            for consent_type in ['ConsentPII', 'EHRConsentPII_ConsentPermission', 'DVEHRSharing_AreYouInterested']:
                if consent_date:
                    break
                for consent in res.consents:
                    if consent['consent'] == consent_type and \
                        consent['consent_value'] in ['ConsentPermission_Yes', 'DVEHRSharing_Yes']:
                        consent_date = consent['consent_date']
                        break

            if consent_date:
                age = int((consent_date - res.date_of_birth).days / 365)
                if not 18 <= age <= 65:
                    data['ubr_age_at_consent'] = 1

        # pylint: disable=unused-variable
        for key, value in data.items():
            if value == 1:
                data['ubr_overall'] = 1
                break

        return data
