#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# Calculator for calculating Participant UBR values.
#
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

from rdr_service.resource.helpers import RURAL_ZIPCODES


class ParticipantUBRCalculator:
    """
    Calculate various UBR values from participant answers.
    Note: Currently UBR should be calculated from the first participant response after consent. Do not use the most
          recent responses.
    """
    @staticmethod
    def ubr_sex(answer: str):
        """
        Calculate the sex UBR value.
        :param answer: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey.
        :return: 1 if UBR else 0
        """
        if answer and answer in ('SexAtBirth_SexAtBirthNoneOfThese', 'SexAtBirth_Intersex'):
            return 1
        return 0

    @staticmethod
    def ubr_sexual_orientation(answer: str):
        """
        Calculate the sexual orientation UBR value.
        :param answer: Answer code to TheBasics_SexualOrientation question in "TheBasics" survey.
        :return: 1 if UBR else 0
        """
        if answer not in ['SexualOrientation_Straight', 'PMI_PreferNotToAnswer', 'PMI_Skip', None]:
            return 1
        return 0

    @staticmethod
    def ubr_gender_identity(birth_sex, gender_ident, gender_ident_closer):
        """
        Calculate the  UBR value.
        :param birth_sex: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey.
        :param gender_ident: Comma delimited str of answers codes to Gender_GenderIdentity question in "TheBasics"
                        survey.
        :param gender_ident_closer: Comma delimited str of answer codes to GenderIdentity_SexualityCloserDescription
                        question in "TheBasics" survey.
        :return: 1 if UBR else 0
        """
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
            return 0

        # Note: assume ubr by default and check for exclusion criteria.
        if len(answers) == 1:
            answer = answers[0]
            if answer in ('PMI_Skip', 'PMI_PreferNotToAnswer', None) or \
                    (answer == 'GenderIdentity_Man' and birth_sex in ['SexAtBirth_Male', 'PMI_Skip', None]) or \
                    (answer == 'GenderIdentity_Woman' and birth_sex in ['SexAtBirth_Female', 'PMI_Skip', None]):
                return 0
        return 1

    @staticmethod
    def ubr_sexual_gender_minority(ubr_sexual_orientation, ubr_gender_identity):
        """
        Calculate the "sexual gender minority" UBR value.
        :param ubr_sexual_orientation: Value returned from self.ubr_sexual_orientation().
        :param ubr_gender_identity: Value returned from self.ubr_gender_identity().
        :return: 1 if UBR else 0
        """
        return ubr_sexual_orientation or ubr_gender_identity

    @staticmethod
    def ubr_ethnicity(answers):
        """
        Calculate the ethnicity UBR value.
        :param answers: Comma delimited str of answer codes to Race_WhatRaceEthnicity questions in "TheBasics" survey.
        :return: 1 if UBR else 0
        """
        # Note: assume ubr by default and check for exclusion criteria.
        if answers:
            answers = answers.split(',')
            if len(answers) == 1:
                answer = answers[0]
                if answer in ('WhatRaceEthnicity_White', 'PMI_Skip', 'PMI_PreferNotToAnswer', None):
                    return 0
        return 1

    @staticmethod
    def ubr_geography(answer):
        """
        Calculate the geography UBR value.
        :param answer: Answer code to StreetAddress_PIIZIP question in "ConsentPII" survey.
        :return: 1 if UBR else 0
        """
        if not answer:
            return 0
        # Some participants provide ZIP+4 format.  Use 5-digit zipcode to check for rural zipcode match
        if len(answer) > 5:
            answer = answer.strip()[:5]
        if answer in RURAL_ZIPCODES:
            return 1
        return 0

    @staticmethod
    def ubr_education(answer):
        """
        Calculate the education UBR value.
        :param answer: Answer code to EducationLevel_HighestGrade question in "TheBasics" survey.
        :return: 1 if UBR else 0
        """
        if answer in (
                'HighestGrade_NeverAttended',
                'HighestGrade_OneThroughFour',
                'HighestGrade_NineThroughEleven',
                'HighestGrade_FiveThroughEight'):
            return 1
        return 0

    @staticmethod
    def ubr_income(answer):
        """
        Calculate the income UBR value.
        :param answer: Answer code to Income_AnnualIncome question in "TheBasics" survey.
        :return: 1 if UBR else 0
        """
        if answer in (
                'AnnualIncome_less10k',
                'AnnualIncome_10k25k'):
            return 1
        return 0

    @staticmethod
    def ubr_disability(answers: dict):
        """
        Calculate the disability UBR value.
        :param answers: Dict with keys and answer codes for 'Employment_EmploymentStatus', 'Disability_Blind',
                        'Disability_WalkingClimbing', 'Disability_DressingBathing', 'Disability_ErrandsAlone',
                        'Disability_Deaf' and 'Disability_DifficultyConcentrating' from "TheBasics" survey.
        :return: 1 if UBR else 0
        """
        if answers:
            if answers.get('Employment_EmploymentStatus', None) == 'EmploymentStatus_UnableToWork' or \
                    answers.get('Disability_Blind', None) == 'Blind_Yes' or \
                    answers.get('Disability_WalkingClimbing', None) == 'WalkingClimbing_Yes' or \
                    answers.get('Disability_DressingBathing', None) == 'DressingBathing_Yes' or \
                    answers.get('Disability_ErrandsAlone', None) == 'ErrandsAlone_Yes' or \
                    answers.get('Disability_Deaf', None) == 'Deaf_Yes' or \
                    answers.get('Disability_DifficultyConcentrating', None) == 'DifficultyConcentrating_Yes':
                return 1
        return 0

    @staticmethod
    def ubr_age_at_consent(consent_time, answer):
        """
        Calculate the "age at consent" UBR value.
        :param consent_time: Timestamp of primary consent.
        :param answer: Answer to the PIIBirthInformation_BirthDate question in the ConsentPII survey.
        :return: 1 if UBR else 0
        """
        if not consent_time or not answer:
            return 0
        # Convert date string to date object if needed.
        if isinstance(answer, str):
            answer = parse(answer)

        rd = relativedelta(consent_time, answer)
        if not 18 <= rd.years < 65:
            return 1
        return 0
