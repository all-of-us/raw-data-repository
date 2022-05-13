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
    # NotAnswer_Skip: Answer is Null (only if a TheBasics has been submitted), PMI_PreferNotToAnswer or PMI_Skip value.
    NotAnswer_Skip = 2


class ParticipantUBRCalculator:
    """
    Calculate various UBR values from participant answers.
    Note: Currently UBR should be calculated from the first participant response after consent. Do not use the most
          recent responses.
    """
    @staticmethod
    def ubr_sex(answer: (str, None)):
        """
        Diversity Category #3 - Biological Sex at Birth
        Calculate the sex UBR value.
        :param answer: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer']:
            return UBRValueEnum.NotAnswer_Skip
        if answer in ('SexAtBirth_Male', 'SexAtBirth_Female'):
            return UBRValueEnum.RBR
        return UBRValueEnum.UBR

    @staticmethod
    def ubr_sexual_orientation(answer: (str, None)):
        """
        Diversity Category #4 part (SO) : Sexual Orientation
        Calculate the sexual orientation UBR value. Value can be a comma delimited list of multiple choice values.
        :param answer: Answer code to TheBasics_SexualOrientation question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        # NOTE:
        # Analysis of RDR data shows it has been possible for participants to submit surveys where they answered
        # the child GenderIdentity_SexualityCloserDescription question from the sexual orientation questions even if
        # they did not answer the expected SexualOrientation_None response to the parent TheBasics_SexualOrientation
        # question first.  This is not  consistent with the documented survey branching logic.  Therefore, UBR/RBR
        # calculations are still based only on the response to the TheBasics_SexualOrientation question alone
        if answer in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer']:
            return UBRValueEnum.NotAnswer_Skip
        if answer == 'SexualOrientation_Straight':
            return UBRValueEnum.RBR
        return UBRValueEnum.UBR

    @staticmethod
    def ubr_gender_identity(birth_sex, gender_ident, gender_ident_closer):
        """
        Diversity Category #4 part (GI) : Gender Identity
        Calculate the  UBR value.
        :param birth_sex: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey.
        :param gender_ident: Comma delimited str of answers codes to Gender_GenderIdentity question in "TheBasics"
                        survey.
        :param gender_ident_closer: Comma delimited str of answer codes to Gender_CloserGenderDescription
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

        # Assume ubr by default and check for exclusion criteria. If there is more than one answer in the
        # list, assume UBR.
        if len(answers) == 1:
            answer = answers[0]  # Get the one answer in the list object.
            if answer in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer', 'GenderIdentity_PreferNotToAnswer']:
                return UBRValueEnum.NotAnswer_Skip
            if (answer == 'GenderIdentity_Man' and
                        birth_sex in ['SexAtBirth_Male', 'PMI_Skip', 'PMI_PreferNotToAnswer', None]) or \
                    (answer == 'GenderIdentity_Woman' and
                        birth_sex in ['SexAtBirth_Female', 'PMI_Skip', 'PMI_PreferNotToAnswer', None]):
                return UBRValueEnum.RBR
        return UBRValueEnum.UBR

    @staticmethod
    def ubr_sexual_gender_minority(ubr_sexual_orientation, ubr_gender_identity):
        """
        Diversity Category #4 : Sexual Gender Minority
        Calculate the "sexual gender minority" UBR value. If only one of the two args is NotAnswer_Skip, we convert the
        arg NotAnswer_Skip value to RBR so we can ignore it.
        :param ubr_sexual_orientation: Value returned from self.ubr_sexual_orientation().
        :param ubr_gender_identity: Value returned from self.ubr_gender_identity().
        :return: UBRValueEnum
        """
        # Both categories must be RBR to be SGM RBR
        if ubr_sexual_orientation == UBRValueEnum.RBR and ubr_gender_identity == UBRValueEnum.RBR:
            return UBRValueEnum.RBR
        # If one or both categories are UBR, return UBR
        if ubr_sexual_orientation == UBRValueEnum.UBR or ubr_gender_identity == UBRValueEnum.UBR:
            return UBRValueEnum.UBR
        # If both of the categories (SO and GI) are Null, the participant is SGM Null.
        # If one of the categories is Null and the other category is RBR, the participant is SGM Null.
        return UBRValueEnum.NotAnswer_Skip

    @staticmethod
    def ubr_ethnicity(answers):
        """
        Diversity Category #1 - Age
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
            if answer in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer']:
                return UBRValueEnum.NotAnswer_Skip
            if answer == 'WhatRaceEthnicity_White':
                return UBRValueEnum.RBR
        return UBRValueEnum.UBR

    @staticmethod
    def ubr_geography(consent_date, answer: (str, None)):
        """
        Diversity Category #7 - Geography
        Calculate the geography UBR value.
        :param consent_date: Date original consent was submitted.
        :param answer: Answer code to StreetAddress_PIIZIP question in "ConsentPII" survey.
        :return: UBRValueEnum
        """
        if answer is None:
            return UBRValueEnum.NotAnswer_Skip
        # Remove non-numeric characters from string.
        if isinstance(answer, str):
            answer = re.sub('[^0-9]', '', answer)
        # Some participants provide ZIP+4 format.  Use 5-digit zipcode to check for rural zipcode match
        if len(answer) > 5:
            answer = answer.strip()[:5]
        if answer in RURAL_ZIPCODES['2014' if consent_date < RURAL_2020_CUTOFF else '2020']:
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_education(answer: (str, None)):
        """
        Diversity Category #6 - Educational Attainment
        Calculate the education UBR value.
        :param answer: Answer code to EducationLevel_HighestGrade question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer']:
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
        Diversity Category #5 - Income
        Calculate the income UBR value.
        :param answer: Answer code to Income_AnnualIncome question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        if answer in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer']:
            return UBRValueEnum.NotAnswer_Skip
        if answer in (
                'AnnualIncome_less10k',
                'AnnualIncome_10k25k'):
            return UBRValueEnum.UBR
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_disability(answers: dict):
        """
        Diversity Category #9 - Disability
        Calculate the disability UBR value.
        :param answers: Dict with keys and answer codes for 'Disability_Blind',
                        'Disability_WalkingClimbing', 'Disability_DressingBathing', 'Disability_ErrandsAlone',
                        'Disability_Deaf' and 'Disability_DifficultyConcentrating' from "TheBasics" survey.
        # List of "Prefer Not To Answer" answer values for each question:
         'Blind_PreferNotToAnswer', 'WalkingClimbing_PreferNotToAnswer', 'DressingBathing_PreferNotToAnswer',
         'ErrandsAlone_PreferNotToAnswer', 'Deaf_PreferNotToAnswer', 'DifficultyConcentrating_PreferNotToAnswer'
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
        # Check and see if all question answers are either Null, 'Prefer Not To Answer' or PMI_Skip.
        null_skip = True
        for k in ['Disability_Blind', 'Disability_WalkingClimbing', 'Disability_DressingBathing',
                   'Disability_ErrandsAlone', 'Disability_Deaf' and 'Disability_DifficultyConcentrating']:
            if answers.get(k, None) not in \
                    [None, 'PMI_Skip', 'Blind_PreferNotToAnswer', 'WalkingClimbing_PreferNotToAnswer',
                     'DressingBathing_PreferNotToAnswer', 'ErrandsAlone_PreferNotToAnswer',
                     'Deaf_PreferNotToAnswer', 'DifficultyConcentrating_PreferNotToAnswer']:
                null_skip = False
                break
        if null_skip is True:
            return UBRValueEnum.NotAnswer_Skip
        return UBRValueEnum.RBR

    @staticmethod
    def ubr_age_at_consent(consent_time, answer: (str, None)):
        """
        Diversity Category #2 - Age
        Calculate the "age at consent" UBR value.
        :param consent_time: Timestamp of primary consent.
        :param answer: Answer to the PIIBirthInformation_BirthDate question in the ConsentPII survey.
        :return: UBRValueEnum
        """
        if answer in [None, 'PMI_Skip']:
            return UBRValueEnum.NotAnswer_Skip
        if not consent_time:
            return UBRValueEnum.RBR
        # Convert date string to date object if needed.
        if isinstance(answer, str):
            answer = parse(answer)
        # When calculating 'Age At Consent', ensure that leap years are accounted for. A date subtraction
        # function that is "leap year" aware must be used.
        rd = relativedelta(consent_time, answer)
        # RBR if age is between 18 and 64, else UBR.
        if 18 <= rd.years < 65:
            return UBRValueEnum.RBR
        return UBRValueEnum.UBR

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
