#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# Calculator for calculating Participant UBR values.
#
import re

from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from enum import IntEnum

from rdr_service.resource.helpers import RURAL_ZIPCODES, RURAL_2020_CUTOFF


class UBRValueEnum(IntEnum):

    RBR = 0
    UBR = 1
    NotAnswer_Skip = 2  # PMI_PreferNotToAnswer or PMI_Skip value.


class ParticipantUBRCalculator:
    """
    Calculate various UBR values from participant answers.
    Note: Currently UBR should be calculated from the first participant response after consent. Do not use the most
          recent responses.
    """
    @staticmethod
    def ubr_sex(answer: (str, None)):
        """
        Calculate the sex UBR value.
        :param answer: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer is None or answer == 'PMI_Skip':
            return UBRValueEnum.NotAnswer_Skip
        if answer in ('SexAtBirth_SexAtBirthNoneOfThese', 'SexAtBirth_Intersex'):
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_sexual_orientation(answer: (str, None)):
        """
        Calculate the sexual orientation UBR value. Value can be a comma delimited list of multiple choice values.
        :param answer: Answer code to TheBasics_SexualOrientation question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer is None or answer == 'PMI_Skip':
            return UBRValueEnum.NotAnswer_Skip
        if answer not in ['SexualOrientation_Straight', 'PMI_PreferNotToAnswer']:
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_gender_identity(birth_sex, gender_ident, gender_ident_closer):
        """
        Calculate the  UBR value.
        :param birth_sex: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey.
        :param gender_ident: Comma delimited str of answers codes to Gender_GenderIdentity question in "TheBasics"
                        survey.
        :param gender_ident_closer: Comma delimited str of answer codes to GenderIdentity_SexualityCloserDescription
                        question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if gender_ident is None and gender_ident_closer is None:
            return UBRValueEnum.NotAnswer_Skip
        # 'gender_ident' can be null, but 'gender_ident_other' can contain a valid answer.
        if not gender_ident:
            gender_ident = ''
        if not gender_ident_closer:
            gender_ident_closer = ''
        # Both 'gender_ident' and 'gender_ident_closer' can be set to 'PMI_Skip', we only want one 'PMI_Skip' in
        # our final answer code value list. There are cases where 'gender_ident' is null, so only remove 'PMI_Skip'
        # in 'gender_ident_closer' when 'gender_ident' has a value.
        if gender_ident and gender_ident_closer == 'PMI_Skip':
            gender_ident_closer = ''
        # Convert answers to a list of individual answer codes and clean out the empty values.
        tmp_list = gender_ident.split(',') + gender_ident_closer.split(',')
        answers = [i for i in tmp_list if i]
        if not answers:
            return UBRValueEnum.RBR

        # Note: assume ubr by default and check for exclusion criteria.
        if len(answers) == 1:
            answer = answers[0]
            if answer is None or answer == 'PMI_Skip':
                return UBRValueEnum.NotAnswer_Skip
            if (answer == 'PMI_PreferNotToAnswer' or answer == 'GenderIdentity_PreferNotToAnswer') or \
                    (answer == 'GenderIdentity_Man' and birth_sex in ['SexAtBirth_Male', 'PMI_Skip', None]) or \
                    (answer == 'GenderIdentity_Woman' and birth_sex in ['SexAtBirth_Female', 'PMI_Skip', None]):
                return UBRValueEnum.RBR
        return UBRValueEnum.UBR

    @staticmethod
    def ubr_sexual_gender_minority(ubr_sexual_orientation, ubr_gender_identity):
        """
        Calculate the "sexual gender minority" UBR value. If only one of the two args is NullSkip, we convert the
        arg NullSkip value to RBR so we can ignore it.
        :param ubr_sexual_orientation: Value returned from self.ubr_sexual_orientation().
        :param ubr_gender_identity: Value returned from self.ubr_gender_identity().
        :return: UBRValueEnum
        """
        # If both are NullSkip, return NullSkip.
        if ubr_sexual_orientation == UBRValueEnum.NotAnswer_Skip and ubr_gender_identity == UBRValueEnum.NotAnswer_Skip:
            return UBRValueEnum.NotAnswer_Skip
        # If either are NullSkip, convert them to RBR.
        if ubr_sexual_orientation == UBRValueEnum.NotAnswer_Skip:
            ubr_sexual_orientation = UBRValueEnum.RBR
        if ubr_gender_identity == UBRValueEnum.NotAnswer_Skip:
            ubr_gender_identity = UBRValueEnum.RBR
        return max([ubr_sexual_orientation, ubr_gender_identity])

    @staticmethod
    def ubr_ethnicity(answers):
        """
        Calculate the ethnicity UBR value.
        :param answers: Comma delimited str of answer codes to Race_WhatRaceEthnicity questions in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answers is None:
            return UBRValueEnum.NotAnswer_Skip
        # Note: assume UBR by default and check for exclusion criteria.
        answers = answers.split(',')
        if len(answers) == 1:
            answer = answers[0]
            if answer == 'PMI_Skip':
                return UBRValueEnum.NotAnswer_Skip
            if answer in ('WhatRaceEthnicity_White', 'PMI_PreferNotToAnswer'):
                return UBRValueEnum.RBR
        return UBRValueEnum.UBR

    @staticmethod
    def ubr_geography(consent_date, answer: (str, None)):
        """
        Calculate the geography UBR value.
        :param consent_date: Date original consent was submitted.
        :param answer: Answer code to StreetAddress_PIIZIP question in "ConsentPII" survey.
        :return: UBRValueEnum
        """
        # Remove non-numeric characters from string.
        if isinstance(answer, str):
            answer = re.sub('[^0-9]', '', answer)
        if answer is None:
            return UBRValueEnum.NotAnswer_Skip
        # Some participants provide ZIP+4 format.  Use 5-digit zipcode to check for rural zipcode match
        if len(answer) > 5:
            answer = answer.strip()[:5]
        if answer in RURAL_ZIPCODES['2014' if consent_date < RURAL_2020_CUTOFF else '2020']:
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_education(answer: (str, None)):
        """
        Calculate the education UBR value.
        :param answer: Answer code to EducationLevel_HighestGrade question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer is None or answer == 'PMI_Skip':
            return UBRValueEnum.NotAnswer_Skip
        if answer in (
                'HighestGrade_NeverAttended',
                'HighestGrade_OneThroughFour',
                'HighestGrade_FiveThroughEight',
                'HighestGrade_NineThroughEleven'):
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_income(answer: (str, None)):
        """
        Calculate the income UBR value.
        :param answer: Answer code to Income_AnnualIncome question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer is None or answer == 'PMI_Skip':
            return UBRValueEnum.NotAnswer_Skip
        if answer in (
                'AnnualIncome_less10k',
                'AnnualIncome_10k25k'):
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_disability(answers: dict):
        """
        Calculate the disability UBR value.
        :param answers: Dict with keys and answer codes for 'Disability_Blind',
                        'Disability_WalkingClimbing', 'Disability_DressingBathing', 'Disability_ErrandsAlone',
                        'Disability_Deaf' and 'Disability_DifficultyConcentrating' from "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answers:
            # PDR-658:  The Employment_EmploymentStatus/EmploymentStatus_UnableToWork answer check was removed from the
            # UBR Disability calculations per program decision
            if answers.get('Disability_Blind', None) == 'Blind_Yes' or \
                    answers.get('Disability_WalkingClimbing', None) == 'WalkingClimbing_Yes' or \
                    answers.get('Disability_DressingBathing', None) == 'DressingBathing_Yes' or \
                    answers.get('Disability_ErrandsAlone', None) == 'ErrandsAlone_Yes' or \
                    answers.get('Disability_Deaf', None) == 'Deaf_Yes' or \
                    answers.get('Disability_DifficultyConcentrating', None) == 'DifficultyConcentrating_Yes':
                return UBRValueEnum.UBR
        # Check and see if all question answers are either Null or PMI_Skip.
        null_skip = True
        for k in [ 'Disability_Blind', 'Disability_WalkingClimbing', 'Disability_DressingBathing',
                   'Disability_ErrandsAlone', 'Disability_Deaf' and 'Disability_DifficultyConcentrating']:
            if answers.get(k, None) not in (None, 'PMI_Skip'):
                null_skip = False
                break
        if null_skip is True:
            return UBRValueEnum.NotAnswer_Skip
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_age_at_consent(consent_time, answer: (str, None)):
        """
        Calculate the "age at consent" UBR value.
        :param consent_time: Timestamp of primary consent.
        :param answer: Answer to the PIIBirthInformation_BirthDate question in the ConsentPII survey.
        :return: UBRValueEnum
        """
        if answer is None or answer == 'PMI_Skip':
            return UBRValueEnum.NotAnswer_Skip
        if not consent_time:
            return UBRValueEnum.RBR
        # Convert date string to date object if needed.
        if isinstance(answer, str):
            answer = parse(answer)

        rd = relativedelta(consent_time, answer)
        if not 18 <= rd.years < 65:
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_overall(data: dict):
        """
        Calculate the UBR overall value from a dictionary of UBR values.
        :param data:
        :return: 0 or 1
        """
        result = UBRValueEnum.RBR
        if data is None:
            return result
        # Test each UBR value for a UBR status.
        for k, v in data.items():
            if k.startswith('ubr_') and v == UBRValueEnum.UBR:
                return UBRValueEnum.UBR
        return result
