#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# Calculator for calculating Participant UBR values.
#
# Note: This UBR code is no longer used in the RDR.  This code is for research purposes only.
#
import math
import re

from enum import IntEnum
from typing import Dict, List
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

from rdr_service.resource.helpers import RURAL_ZIPCODES, RURAL_2020_CUTOFF


class UBRValueEnum(IntEnum):

    RBR = 0
    UBR = 1
    # NotAnswer_Skip: Answer is Null (only if a TheBasics has been submitted), PMI_PreferNotToAnswer or PMI_Skip value.
    NotAnswer_Skip = 2


class ParticipantUBRCalculator:
    """
    Calculate various UBR values from participant answers.  Some methods may be overridden by the child
    PedParticipantUBRCalculator class.

    Note: Currently UBR should be calculated from the first participant response after consent. Do not use the most
          recent responses.
    """

    @staticmethod
    def ubr_sex(answer: (str, None)):
        """
        Diversity Category #3 - Biological Sex at Birth
        Calculate the sex UBR value.
        :param answer: Answer code to BiologicalSexAtBirth_SexAtBirth question in "TheBasics" survey,
                       or biologicalsexatbirth_sexatbirth_ped for pediatric ped_basics survey.  The answer code
                       options are the same for both the adult and pediatric question codes
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
        Diversity Category #4 part (SO) : Sexual Orientation (does not apply to pediatric 0-6 participants)
        Calculate the sexual orientation UBR value. Value can be a comma delimited list of multiple choice values.
        :param answer: Answer code to TheBasics_SexualOrientation question in "TheBasics" survey.
        :return: UBRValueEnum
        """
        # NOTE:
        # Analysis of RDR data shows it has been possible for participants to submit surveys where they answered
        # the branched GenderIdentity_SexualityCloserDescription question even if they did not answer the
        # expected SexualOrientation_None response to the initial TheBasics_SexualOrientation
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
        Diversity Category #4 part (GI) : Gender Identity (does not apply to pediatric 0-6 participants)
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
        # If the gender_ident_closer list is empty, remove the 'GenderIdentity_AdditionalOptions' answer if needed.
        if not gender_ident_closer and 'GenderIdentity_AdditionalOptions' in answers:
            answers.remove('GenderIdentity_AdditionalOptions')

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
        Diversity Category #4 : Sexual Gender Minority (does not apply to pediatric 0-6 participants)
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
        Diversity Category: Ethnicity
        Calculate the ethnicity UBR value.
        :param answers: Comma delimited str of answer codes to Race_WhatRaceEthnicity questions in "TheBasics" survey
                        (or race_whatraceethnicity_ped for pediatric ped_basics survey).  The answer code options
                        are the same for both the adult and pediatric question code.
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
        :param consent_date: Date original consent was submitted
        :param answer: Answer for the participant StreetAddressPIIZipCode question in the adult ConsentPII or
                       childpermission_info_zipcode question in "consentpii_0to6" survey
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
        Diversity Category #6 - Educational Attainment (does not apply to pediatric 0-6 participants)
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
    def ubr_income(howmanypeople, income, addr_state, year_completed_thebasics, income_mapping):
        """
        Diversity Category #5 - Income
        Calculate the income UBR value.

        Example of config income mapping structure:


        {
          "AK": {
              "2023_2024": {
                  "1_2": ["less10k", "10k25k", "25k35k", "35k50k"],
                  "3": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k"],
                  "4_5": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k"],
                  "6_7_8_9": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k", "100k150k"],
                  "10_11": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k", "100k150k", "150k200k"]
              }
          },
          "HI": {
              "2023_2024": {
                  "1": ["less10k", "10k25k", "25k35k"],
                  "2": ["less10k", "10k25k", "25k35k", "35k50k"],
                  "3_4": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k"],
                  "5_6": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k"],
                  "7_8_9_10": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k", "100k150k"],
                  "11": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k", "100k150k", "150k200k"]
              }
          },
          "Other": {
              "2023_2024": {
                  "1": ["less10k", "10k25k", "25k35k"],
                  "2_3": ["less10k", "10k25k", "25k35k", "35k50k"],
                  "4_5": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k"],
                  "6_7": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k"],
                  "7_8_9_10_11": ["less10k", "10k25k", "25k35k", "35k50k", "50k75k", "75k100k", "100k150k", "150k200k"]
              }
        }
        :param howmanypeople: Answer for livingsituation_howmanypeople to know number of people in the household
        :param income: Answer for income_annualincome
        :param addr_state: State of residence from ConsentPii
        :param year_completed_thebasics: The authored date from theBasics (can also be the created date in lower envs)
        :param income_mapping: dict containing income ubr guidelines
        :return: UBRValueEnum
        """

        if income in ('PMI_PreferNotToAnswer', 'PMI_Skip', None):
            return UBRValueEnum.NotAnswer_Skip

        income = income.replace('AnnualIncome_', '')

        # ensure howmanypeople is numeric or PMI_Skip or PMI_PreferNotToAnswer
        howmanypeople = howmanypeople or '0'
        # Number of people should be the answer + 1 (for survey taker). If non-numeric answer default to 1
        people = str(math.ceil(abs(float(howmanypeople))) + 1) if howmanypeople.isdigit() else '1'
        # If number of people is more than 10 default value to 11
        people = '11' if int(people) > 10 else people

        state = addr_state if addr_state in income_mapping.keys() else 'Other'
        year = next((y for y in income_mapping[state].keys() if str(year_completed_thebasics) in y), None)
        if year is None:
            raise ValueError
        num_people = next(n for n in income_mapping[state][year].keys() if people in n)

        if income not in income_mapping[state][year][num_people]:
            return UBRValueEnum.RBR
        else:
            return UBRValueEnum.UBR

    @staticmethod
    def ubr_disability(survey_responses: List[Dict]):
        """
        Diversity Category #9 - Disability
        Calculate the disability UBR value.   Overriden in PedParticipantUBRCalculator class
        :param survey_responses: Ordered list of Dicts with keys and answer codes for 'Disability_Blind',
                        'Disability_WalkingClimbing', 'Disability_DressingBathing', 'Disability_ErrandsAlone',
                        'Disability_Deaf' and 'Disability_DifficultyConcentrating' from "TheBasics" and/or "lfs"
                        (Life Functioning Survey) surveys.  If list has both TheBasics and lfs responses, lfs answers
                        should be last in the ordered list
        # List of "Prefer Not To Answer" answer values for each question:
         'Blind_PreferNotToAnswer', 'WalkingClimbing_PreferNotToAnswer', 'DressingBathing_PreferNotToAnswer',
         'ErrandsAlone_PreferNotToAnswer', 'Deaf_PreferNotToAnswer', 'DifficultyConcentrating_PreferNotToAnswer'
        :return: UBRValueEnum
        """
        ret = UBRValueEnum.RBR  # Default return value
        for answers in survey_responses:
            # PDR-658:  The Employment_EmploymentStatus/EmploymentStatus_UnableToWork answer check was removed from
            # the UBR Disability calculations per program decision
            if answers.get('Disability_Blind', None) == 'Blind_Yes' or \
                    answers.get('Disability_WalkingClimbing', None) == 'WalkingClimbing_Yes' or \
                    answers.get('Disability_DressingBathing', None) == 'DressingBathing_Yes' or \
                    answers.get('Disability_ErrandsAlone', None) == 'ErrandsAlone_Yes' or \
                    answers.get('Disability_Deaf', None) == 'Deaf_Yes' or \
                    answers.get('Disability_DifficultyConcentrating', None) == 'DifficultyConcentrating_Yes':
                # Affirmative UBR answers from TheBasics are always honored, do not need to continue processing
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
                # This may be updated on subsequent pass, when/if processing additional lfs response data
                ret = UBRValueEnum.NotAnswer_Skip
            else:
                ret = UBRValueEnum.RBR

        return ret

    @staticmethod
    def ubr_age_at_consent(consent_time, answer: (str, None)):
        """
        Diversity Category #2 - Age
        Calculate the "age at consent" UBR value (does not apply to pediatric participants)
        :param consent_time: Timestamp of primary consent.
        :param answer: Answer to the PIIBirthInformation_BirthDate question in the ConsentPII survey.
        :return: UBRValueEnum
        """
        if answer in [None, 'PMI_Skip']:
            return UBRValueEnum.NotAnswer_Skip
        if not consent_time:
            return None
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

    @staticmethod
    def ubr_access_to_care(answers):
        """
        Diversity Category - Access to Care
        Calculate the Access to Care UBR value for participants
        This can be calculated on the response to the Basics 'insurance_healthinsurance' question alone
        :param answers: Dict with keys and answer codes for thebasics and access to care questions
        :return: UBRValueEnum
        """
        # if answers or True:
        #     return None

        # Questions on access to care.
        if answers.get('insurance_healthinsurance', None) == 'HealthInsurance_No':
            return UBRValueEnum.UBR
        if answers.get('healthadvice_placeforhealthadvice', None) == 'PlaceforHealthAdvice_No':
            return UBRValueEnum.UBR

        if answers.get('healthadvice_whatkindofplace', None) == 'WhatKindOfPlace_EmergencyRoom':
            return UBRValueEnum.UBR

        if (answers.get('delayedmedicalcare_cantaffordcopay', None) == 'CantAffordCoPay_Yes'
                or answers.get('delayedmedicalcare_deductibletoohigh', None) == 'DeductibleTooHigh_Yes'
                or answers.get('delayedmedicalcare_hadtopayoutofpocket', None) == 'HadToPayOutOfPocket_Yes'
                or answers.get('delayedmedicalcare_ruralarea', None) == 'RuralArea_Yes'):
            return UBRValueEnum.UBR

        if (answers.get('cantaffordcare_prescriptionmedicines', None) == 'PrescriptionMedicines_Yes'
                or answers.get('cantaffordcare_mentalhealthcounseling', None) == 'MentalHealthCounseling_Yes'
                or answers.get('cantaffordcare_emergencycare', None) == 'EmergencyCare_Yes'
                or answers.get('cantaffordcare_dentalcare', None) == 'DentalCare_Yes'
                or answers.get('cantaffordcare_eyeglasses', None) == 'Eyeglasses_Yes'
                or answers.get('cantaffordcare_healthcareprovider', None) == 'HealthcareProvider_Yes'
                or answers.get('cantaffordcare_specialist', None) == 'Specialist_Yes'
                or answers.get('cantaffordcare_followupcare', None) == 'FollowupCare_Yes'):
            return UBRValueEnum.UBR

        if (answers.get('cantaffordcare_skippedmedtosavemoney', None) == 'SkippedMedToSaveMoney_Yes'
                or answers.get('cantaffordcare_tooklessmedtosavemoney', None) == 'TookLessMedToSaveMoney_Yes'
                or answers.get('cantaffordcare_delayedfillingrxtosavemoney',
                               None) == 'DelayedFillingRxToSaveMoney_Yes'):
            return UBRValueEnum.UBR

        # Remaining questions that, in combination of 2 or more qualifying answers, calculate to UBR
        answer_count = 0
        for k in ['delayedmedicalcare_transportation', 'delayedmedicalcare_timeoffwork', 'delayedmedicalcare_childcare',
                  'delayedmedicalcare_elderlycare', 'healthproviderracereligion_delayedornocare']:

            if k == 'healthproviderracereligion_delayedornocare' and answers.get(k, None) in \
                    ('DelayedOrNoCare_Always', 'DelayedOrNoCare_MostOfTheTime', 'DelayedOrNoCare_SomeOfTheTime'):
                answer_count += 1

            if answers.get(k, None) in ('Transportation_Yes', 'TimeOffWork_Yes', 'ChildCare_Yes', 'ElderlyCare_Yes'):
                answer_count += 1

        if answer_count >= 2:
            return UBRValueEnum.UBR

        # TODO:   Confirm PMI_DontKnow answers should resolve to RBR
        # There is a possibility that someone who has skipped a lot of questions needs to be re calculated
        null_skip = True
        for k in ['insurance_healthinsurance', 'healthadvice_placeforhealthadvice']:
            if answers.get(k, None) not in [None, 'PMI_Skip', 'PMI_PreferNotToAnswer']:
                null_skip = False
                break
        if null_skip is True:
            return UBRValueEnum.NotAnswer_Skip
        else:
            return UBRValueEnum.RBR


class PedParticipantUBRCalculator(ParticipantUBRCalculator):
    """
    A UBR calculator specific to peds participants, that will override the UBR calculations which differ from
    the adult UBR calculations.
    """

    @staticmethod
    def ubr_disability(answers):
        """
        Diversity Category #9 - Disability
        Calculate the disability UBR value.
        :param answers: Dict with keys and answer codes for ped_basics disability questions
        :return: UBRValueEnum
        """
        # nsch_2 series of questions on whether child uses more medical care mental health/educational services
        # pedbasics_1 = "Yes" response;  all three must be yes
        if (answers.get('nsch_2', None) == 'pedbasics_1'
                and answers.get('nsch_2_condition', None) == 'pedbasics_1'
                and answers.get('nsch_2_condition_12months', None) == 'pedbasics_1'):
            return UBRValueEnum.UBR

        # nsch_3 series of questions on whether child is limited/prevented from doing things other children can
        # All must be "Yes"/pedbasics_1
        if (answers.get('nsch_3', None) == 'pedbasics_1'
                and answers.get('nsch_3_condition', None) == 'pedbasics_1'
                and answers.get('nsch_3_condition_12months', None) == 'pedbasics_1'):
            return UBRValueEnum.UBR

        # nsch_4 series of questions on whether child needs/gets specialized therapy
        # All must be "Yes"/pedbasics_1
        if (answers.get('nsch_4', None) == 'pedbasics_1'
                and answers.get('nsch_4_condition', None) == 'pedbasics_1'
                and answers.get('nsch_4_condition_12months', None) == 'pedbasics_1'):
            return UBRValueEnum.UBR

        # nsch_5 questions on whether child has developmental/behavioral problems needing treatment
        # Both must be "Yes"/pedbasics_1
        if (answers.get('nsch_5', None) == 'pedbasics_1'
                and answers.get('nsch_5_condition_12months', None) == 'pedbasics_1'):
            return UBRValueEnum.UBR

        # aou_1 questions on hearing/vision/speech/cognitive/physical impairments
        # Both must be "Yes"/pedbasics_1
        if answers.get('aou_1', None) == 'pedbasics_1' and answers.get('aou_1_12months', None) == 'pedbasics_1':
            return UBRValueEnum.UBR

        # Check and see if all question answers are either Null, 'Prefer Not To Answer' or PMI_Skip.
        # TODO:  What does PMI_DontKnow answer map to for aou_1 answer(s)?
        null_skip = True
        for k in ['nsch_1', 'nsch_2', 'nsch_3', 'nsch_4', 'nsch_5', 'aou_1']:
            if answers.get(k, None) not in [None, 'PMI_Skip']:
                null_skip = False
                break
        if null_skip is True:
            return UBRValueEnum.NotAnswer_Skip
        else:
            return UBRValueEnum.RBR

    @staticmethod
    def ubr_access_to_care(answers):
        """
        Diversity Category - Access to Care
        Calculate the Access to Care UBR value for pediactric participants
        :param answers: Dict with keys and answer codes for ped_basics access to care questions
        :return: UBRValueEnum
        """
        # if answers or True:
        #     return None

        # Questions on access to care.
        if answers.get('insurance_healthinsurance_ped', None) == 'HealthInsurance_No':
            return UBRValueEnum.UBR
        if answers.get('healthadvice_placeforhealthadvice_ped', None) == 'PlaceforHealthAdvice_No':
            return UBRValueEnum.UBR

        if answers.get('healthadvice_whatkindofplace_ped', None) in ('WhatKindOfPlace_UrgentCare',
                                                                     'WhatKindOfPlace_EmergencyRoom'):
            return UBRValueEnum.UBR

        if (answers.get('delaydmedicalcare_ped', None) == 'DelayedCare_Yes'
                and (answers.get('cantaffordcare_skippedmedtosavemoney_ped', None) == 'SkippedMedToSaveMoney_Yes'
                     or answers.get('cantaffordcare_tooklessmedtosavemoney_ped', None) == 'TookLessMedToSaveMoney_Yes'
                     or answers.get('cantaffordcare_delayedfillingfxtosavemoney_ped',
                                    None) == 'DelayedFillingRxToSaveMoney_Yes'
                     or answers.get('cantaffordcare_pharmacy_ped', None) == 'Pharmacy_Yes'
                     or answers.get('CantAffordCare_LowerCostRxToSaveMoney', None) == 'LowerCostRxToSaveMoney_Yes'
                     or answers.get('CantAffordCare_BoughtRxFromOtherCountry', None) == 'BoughtRxFromOtherCountry_Yes'
                     or answers.get('CantAffordCare_AlternativeTherapies', None) == 'AlternativeTherapies_Yes')
        ):
            return UBRValueEnum.UBR

        # TODO:   Confirm PMI_DontKnow answers should resolve to RBR
        null_skip = True
        for k in ['insurance_healthinsurance_ped', 'healthadvice_placeforhealthadvice_ped', 'delayedmedicalcare_ped']:
            if answers.get(k, None) not in [None, 'PMI_Skip']:
                null_skip = False
                break
        if null_skip is True:
            return UBRValueEnum.NotAnswer_Skip
        else:
            return UBRValueEnum.RBR
