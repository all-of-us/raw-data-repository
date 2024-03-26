#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Note: This UBR code is no longer used in the RDR.  This code is for research purposes only.
#
# from datetime import datetime
# from dateutil.relativedelta import relativedelta
#
# from rdr_service.resource.calculators import ParticipantUBRCalculator, UBRValueEnum
# from rdr_service.resource.helpers import RURAL_2020_CUTOFF
# from tests.helpers.unittest_base import BaseTestCase
#
#
# class UBRCalculatorTest(BaseTestCase):
#
#     def setUp(self, with_data=False, with_consent_codes=False) -> None:
#         super().setUp(with_data, with_consent_codes)
#         self.ubr = ParticipantUBRCalculator()
#
#         self.disability_answers = {
#             'Disability_Blind': None,
#             'Disability_WalkingClimbing': None,
#             'Disability_DressingBathing': None,
#             'Disability_ErrandsAlone': None,
#             'Disability_Deaf': None,
#             'Disability_DifficultyConcentrating': None
#         }
#
#     def test_ubr_sex(self):
#         """
#         UBR Calculator Test - Sex
#         Note: Single Value UBR Calculation
#         """
#         # Test with Null and PMI_Skip values
#         self.assertEqual(self.ubr.ubr_sex(None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_sex('PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR value
#         self.assertEqual(self.ubr.ubr_sex('SexAtBirth_SexAtBirthNoneOfThese'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_sex('SexAtBirth_Intersex'), UBRValueEnum.UBR)
#
#         # Test RBR value
#         self.assertEqual(self.ubr.ubr_sex('SexAtBirth_Male'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_sex('SexAtBirth_Female'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_sex('PMI_PreferNotToAnswer'), UBRValueEnum.NotAnswer_Skip)
#
#     def test_ubr_sexual_orientation(self):
#         """
#         UBR Calculator Test - Sexual Orientation
#         Note: Multiple Value UBR Calculation
#         """
#         # Test with Null and PMI_Skip values
#         self.assertEqual(self.ubr.ubr_sexual_orientation(None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_sexual_orientation('PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR value
#         self.assertEqual(self.ubr.ubr_sexual_orientation('SexualOrientation_Bisexual'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_sexual_orientation('SexualOrientation_Lesbian'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_sexual_orientation('SexualOrientation_Gay'), UBRValueEnum.UBR)
#         self.assertEqual(
#             self.ubr.ubr_sexual_orientation('SexualOrientation_Lesbian,SexualOrientation_Straight'),UBRValueEnum.UBR)
#         self.assertEqual(
#             self.ubr.ubr_sexual_orientation('SexualOrientation_Straight,SexualOrientation_Lesbian'),UBRValueEnum.UBR)
#
#         # Test RBR value
#         self.assertEqual(self.ubr.ubr_sexual_orientation('SexualOrientation_Straight'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_sexual_orientation('PMI_PreferNotToAnswer'), UBRValueEnum.NotAnswer_Skip)
#
#         # Bad or unknown value will default to UBR.
#         self.assertEqual(self.ubr.ubr_sexual_orientation('BadValueTest'), UBRValueEnum.UBR)
#
#     def test_ubr_gender_identity(self):
#         """
#         UBR Calculator Test - Gender Identity
#         Note: Multiple Value UBR Calculation
#         """
#         # Test with Null and PMI_Skip values
#         self.assertEqual(self.ubr.ubr_gender_identity('SexAtBirth_Female', None, None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity('SexAtBirth_Male', None, None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity('SexAtBirth_Intersex', None, None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity('SexAtBirth_Female', 'PMI_Skip', 'PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity('SexAtBirth_Male', None, 'PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity('SexAtBirth_Intersex', 'PMI_Skip', None), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR values
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Female', 'GenderIdentity_NonBinary', 'PMI_Skip'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Female', 'GenderIdentity_Man', None), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Intersex', 'GenderIdentity_Man', None), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_SexAtBirthNoneOfThese', 'GenderIdentity_Woman', 'PMI_Skip'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Male', 'PMI_Skip', 'SexualityCloserDescription_MostlyStraight'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Male', 'GenderIdentity_AdditionalOptions', 'SexualityCloserDescription_Asexual'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'PMI_Skip', 'PMI_Skip', 'SexualityCloserDescription_NotFiguredOut'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 None, None, 'SexualityCloserDescription_PolyOmniSapioPansexual'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Male', 'GenderIdentity_Man', 'SexualityCloserDescription_TwoSpirit'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Female', 'GenderIdentity_Woman', 'SexualityCloserDescription_SomethingElse'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Male', 'GenderIdentity_Man,GenderIdentity_Woman', None), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Female', None,
#                 'SexualityCloserDescription_PolyOmniSapioPansexual,SexualityCloserDescription_Queer'), UBRValueEnum.UBR)
#
#         # Test RBR values
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Intersex', 'PMI_PreferNotToAnswer', None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#             'SexAtBirth_Male', 'GenderIdentity_PreferNotToAnswer', None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Female', 'GenderIdentity_Woman', 'PMI_Skip'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'SexAtBirth_Male', 'GenderIdentity_Man', 'PMI_Skip'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 None, 'GenderIdentity_Man', None), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#                 'PMI_Skip', 'GenderIdentity_Woman', None), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_gender_identity(
#             'PMI_Skip', 'GenderIdentity_Woman', 'PMI_Skip'), UBRValueEnum.RBR)
#
#     def test_ubr_sexual_gender_minority(self):
#         """
#         UBR Calculator Test - Sexual Gender Minority
#         The value for UBR sexual gender minority is calculated using the output of
#         self.calc.ubr_sexual_orientation() and self.calc.ubr_gender_identity().
#         Note: NullSkip values are converted to RBR in the SGM function if only one
#               of the two function argument values equal NullSKip.
#         """
#         # Test with RBR values. int(RBR) == 0.
#         self.assertEqual(UBRValueEnum.RBR,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.RBR, UBRValueEnum.RBR))
#         self.assertEqual(UBRValueEnum.NotAnswer_Skip,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.RBR, UBRValueEnum.NotAnswer_Skip))
#         self.assertEqual(0,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.RBR, UBRValueEnum.RBR))
#
#         # Test with UBR Values. int(UBR) == 1.
#         self.assertEqual(UBRValueEnum.UBR,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.UBR, UBRValueEnum.RBR))
#         self.assertEqual(UBRValueEnum.UBR,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.RBR, UBRValueEnum.UBR))
#         self.assertEqual(UBRValueEnum.UBR,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.NotAnswer_Skip, UBRValueEnum.UBR))
#         self.assertEqual(1,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.RBR, UBRValueEnum.UBR))
#
#         # Test with Null/Skip Values. int(NullSkip) == 2.
#         self.assertEqual(UBRValueEnum.NotAnswer_Skip,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.NotAnswer_Skip, UBRValueEnum.NotAnswer_Skip))
#         self.assertEqual(2,
#                          self.ubr.ubr_sexual_gender_minority(UBRValueEnum.NotAnswer_Skip, UBRValueEnum.NotAnswer_Skip))
#
#     def test_ubr_ethnicity(self):
#         """
#         UBR Calculator Test - Ethnicity
#         Note: Multiple value UBR calculation.
#         """
#         # Test with Null and PMI_Skip values
#         # Note: Multiple choice question, pass comma-delimited string.
#         self.assertEqual(self.ubr.ubr_ethnicity(None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_ethnicity('PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR value
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_AIAN'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_Black'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_MENA'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_Hispanic'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_NHPI'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_RaceEthnicityNoneOfThese'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_White,WhatRaceEthnicity_NHPI'), UBRValueEnum.UBR)
#
#         # Test RBR value
#         self.assertEqual(self.ubr.ubr_ethnicity('WhatRaceEthnicity_White'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_ethnicity('PMI_PreferNotToAnswer'), UBRValueEnum.NotAnswer_Skip)
#
#         # Bad or unknown value will default to UBR
#         self.assertEqual(self.ubr.ubr_ethnicity('ABC-123'), UBRValueEnum.UBR)
#
#
#     def test_ubr_geography(self):
#         """
#         UBR Calculator Test - Geography
#         """
#         consent_2014 = datetime(2019, 7, 1).date()  # Hit 2014 version of rural zipcodes.
#         consent_2020 = RURAL_2020_CUTOFF  # Hit 2020 version of rural zipcodes.
#
#         zip_2014 = '35052'  # Only in 2014 file.
#         zip_2020 = '35063'  # Only in 2020 file.
#         zip_both = '72529'  # In both the 2014 file and 2020 file.
#         zip_none = '58493'  # Zip code not in either file.
#
#         # Test with Null values, there are no PMI_Skip values currently for a zip code value.
#         self.assertEqual(self.ubr.ubr_geography(consent_2014, None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, None), UBRValueEnum.NotAnswer_Skip)
#
#         # Test zip codes in specific files for UBR
#         self.assertEqual(self.ubr.ubr_geography(consent_2014, zip_2014), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, zip_2020), UBRValueEnum.UBR)
#
#         # Test zip code in both files for UBR
#         self.assertEqual(self.ubr.ubr_geography(consent_2014, zip_both), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, zip_both), UBRValueEnum.UBR)
#         # Test zip+4 values for UBR.
#         self.assertEqual(self.ubr.ubr_geography(consent_2014, zip_2014 + '-1234'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, zip_2020 + '-1234'), UBRValueEnum.UBR)
#
#         # Test for RBR value
#         self.assertEqual(self.ubr.ubr_geography(consent_2014, zip_none), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, zip_none), UBRValueEnum.RBR)
#         # Test zip+4 values for RBR.
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, zip_2014 + '-1234'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_geography(consent_2014, zip_2020 + '-1234'), UBRValueEnum.RBR)
#
#         # Bad or unknown value will default to RBR.
#         self.assertEqual(self.ubr.ubr_geography(consent_2020, '123-abc'), UBRValueEnum.RBR)
#
#
#     def test_ubr_education(self):
#         """
#         UBR Calculator Test - Education
#         """
#         # Test with Null and PMI_Skip values
#         self.assertEqual(self.ubr.ubr_education(None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_education('PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR value
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_NeverAttended'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_OneThroughFour'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_FiveThroughEight'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_NineThroughEleven'), UBRValueEnum.UBR)
#
#         # Test RBR value
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_TwelveOrGED'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_CollegeOnetoThree'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_CollegeGraduate'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_education('HighestGrade_AdvancedDegree'), UBRValueEnum.RBR)
#
#         # Bad or unknown value will default to RBR
#         self.assertEqual(self.ubr.ubr_education('BadValueTest'), UBRValueEnum.RBR)
#
#     def test_ubr_income(self):
#         """
#         UBR Calculator Test - Income
#         """
#         # Test with Null and PMI_Skip values
#         self.assertEqual(self.ubr.ubr_income(None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_income('PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR value
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_less10k'), UBRValueEnum.UBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_10k25k'), UBRValueEnum.UBR)
#
#         # Test RBR value
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_25k35k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_35k50k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_50k75k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_75k100k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_100k150k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_150k200k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('AnnualIncome_more200k'), UBRValueEnum.RBR)
#         self.assertEqual(self.ubr.ubr_income('PMI_PreferNotToAnswer'), UBRValueEnum.NotAnswer_Skip)
#
#         # Bad or unknown value will default to RBR
#         self.assertEqual(self.ubr.ubr_income('BadValueTest'), UBRValueEnum.RBR)
#
#     def test_ubr_disability(self):
#         """
#         UBR Calculator Test - Disability
#         """
#         # Test with Null and PMI_Skip values
#         values = self.disability_answers
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.NotAnswer_Skip)
#         values['Disability_Deaf'] = 'PMI_Skip'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.NotAnswer_Skip)
#         for k in self.disability_answers.keys():
#             values[k] = 'PMI_Skip'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.NotAnswer_Skip)
#
#         # Test with "Prefer Not To Answer" values.
#         values = self.disability_answers
#         values['Disability_Deaf'] = 'Deaf_PreferNotToAnswer'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.NotAnswer_Skip)
#         values['Disability_ErrandsAlone'] = 'ErrandsAlone_Yes'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.UBR)
#
#         # Test UBR value
#         values = self.disability_answers
#         values['Disability_ErrandsAlone'] = 'ErrandsAlone_Yes'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.UBR)
#
#         # Test RBR value
#         values = self.disability_answers
#         for k in self.disability_answers.keys():
#             values[k] = 'Other_Answer'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.RBR)
#
#         # Bad or unknown value will default to NullSkip
#         self.assertEqual(self.ubr.ubr_disability([{'ABC': 123}]), UBRValueEnum.NotAnswer_Skip)
#
#         # PDR-658:  Test that EmploymentStatus_UnableToWork no longer results in a UBR result
#         values = self.disability_answers
#         values['Employment_EmploymentStatus'] = 'EmploymentStatus_UnableToWork'
#         self.assertEqual(self.ubr.ubr_disability([values]), UBRValueEnum.RBR)
#
#     def test_ubr_disability_basics_and_lfs(self):
#         """
#         UBR Calculator - Disability based on both TheBasics and lfs simulated survey responses
#         """
#         # Simulate an early version TheBasics with Nulls for disability questions, followed by lfs answers
#         basics_values = self.disability_answers
#         self.assertEqual(self.ubr.ubr_disability([basics_values]), UBRValueEnum.NotAnswer_Skip)
#
#         # All "no" answers for follow-on lfs survey = RBR
#         lfs_values = {
#             'Disability_WalkingClimbing': 'WalkingClimbing_No',
#             'Disability_DressingBathing': 'DressingBathing_No',
#             'Disability_ErrandsAlone': 'ErrandsAlone_No',
#             'Disability_Deaf': 'Deaf_No',
#             'Disability_DifficultyConcentrating': 'Blind_No'
#         }
#         self.assertEqual(self.ubr.ubr_disability([basics_values, lfs_values]), UBRValueEnum.RBR)
#
#         # Setting an lfs answer to "yes" =  UBR
#         lfs_values['Disability_WalkingClimbing'] = 'WalkingClimbing_Yes'
#         self.assertEqual(self.ubr.ubr_disability([basics_values, lfs_values]), UBRValueEnum.UBR)
#
#     def test_ubr_age_at_consent(self):
#         """
#         UBR Calculator Test - Age at Consent
#         """
#         consent_ts = datetime(2020, 7, 1)
#         consent = consent_ts.date()
#
#         # Test with Null and PMI_Skip values
#         self.assertEqual(self.ubr.ubr_age_at_consent(consent, None), UBRValueEnum.NotAnswer_Skip)
#         self.assertEqual(self.ubr.ubr_age_at_consent(consent, 'PMI_Skip'), UBRValueEnum.NotAnswer_Skip)
#
#         # Test UBR value
#         dob = (consent_ts - relativedelta(years=17)).date()
#         self.assertEqual(self.ubr.ubr_age_at_consent(consent, dob), UBRValueEnum.RBR)
#         dob = (consent_ts - relativedelta(years=65)).date()
#         self.assertEqual(self.ubr.ubr_age_at_consent(consent, dob), UBRValueEnum.UBR)
#
#         # Test RBR value
#         dob = (consent_ts - relativedelta(years=18)).date()
#         self.assertEqual(self.ubr.ubr_age_at_consent(consent, dob), UBRValueEnum.RBR)
#         dob = (consent_ts - relativedelta(years=64)).date()
#         self.assertEqual(self.ubr.ubr_age_at_consent(consent, dob), UBRValueEnum.RBR)
#
#         dob = (consent_ts - relativedelta(years=55)).date()
#         self.assertEqual(self.ubr.ubr_age_at_consent(None, dob), UBRValueEnum.RBR)
#         dob = (consent_ts - relativedelta(years=70)).date()
#         self.assertEqual(self.ubr.ubr_age_at_consent(None, dob), UBRValueEnum.RBR)
#
#     def test_ubr_overall(self):
#         """
#         UBR Calculator Test - UBR Overall
#         """
#         # Test RBR with Null value.
#         data = {'ubr_age_at_consent': None}
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.RBR)
#
#         # Test RBR with RBR value
#         data = {'ubr_age_at_consent': UBRValueEnum.RBR}
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.RBR)
#
#         # Test RBR with PMI_Skip value
#         data = {'ubr_age_at_consent': UBRValueEnum.NotAnswer_Skip}
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.RBR)
#
#         # Test RBR with mixed values, but no UBR value.
#         data = {
#             'ubr_age_at_consent': None,
#             'ubr_disability': UBRValueEnum.RBR,
#             'ubr_education': UBRValueEnum.NotAnswer_Skip
#         }
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.RBR)
#
#         # Test UBR with mixed values, with UBR value.
#         data = {
#             'ubr_age_at_consent': None,
#             'ubr_disability': UBRValueEnum.RBR,
#             'ubr_education': UBRValueEnum.NotAnswer_Skip,
#             'ubr_ethnicity': UBRValueEnum.UBR
#         }
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.UBR)
#
#         # Test UBR with all UBR values.
#         data = {
#             'ubr_age_at_consent': UBRValueEnum.UBR,
#             'ubr_disability': UBRValueEnum.UBR,
#             'ubr_education': UBRValueEnum.UBR,
#             'ubr_ethnicity': UBRValueEnum.UBR
#         }
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.UBR)
#
#         # Test RBR with all NotAnswered/Skip values.
#         data = {
#             'ubr_age_at_consent': UBRValueEnum.NotAnswer_Skip,
#             'ubr_disability': UBRValueEnum.NotAnswer_Skip,
#             'ubr_education': UBRValueEnum.NotAnswer_Skip,
#             'ubr_ethnicity': UBRValueEnum.NotAnswer_Skip
#         }
#         overall = self.ubr.ubr_overall(data)
#         self.assertEqual(overall, UBRValueEnum.RBR)
