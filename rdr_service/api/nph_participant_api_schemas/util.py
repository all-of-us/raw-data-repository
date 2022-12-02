from types import MappingProxyType

expected_fields = MappingProxyType(
    {
        "event_collection": ["InformedConsentModule1", "InformedConsentModule2", "InformedConsentModule3",
                             "RetrospectiveVisualAnalogueScales", "EatingInventory", "FoodCravingInventory",
                             "MultifactorialAssessmentOfEatingDisordersSymptoms", "IntuitiveEatingScale2",
                             "RepetitiveEatingQuestionnaire", "BarrattImpulsivityScale11",
                             "DietSatisfactionQuestionnaire",
                             "WeightHistoryQuestionnaire", "NhanesSleepDisorderQuestionnaire",
                             "MunichChronotypeQuestionnaire", "MacArthurScaleOfSubjectiveSocialStatus",
                             "AouQualityOfLife",
                             "PatientHealthQuestionnaire", "AouMentalHealthAndWellBeing", "GeneralizedAnxietyDisorder",
                             "TenItemPersonalityInventory", "ChildhoodAndAdultAdversityTrauma",
                             "ChildhoodAdverseEventsQuestionnaire", "FiveTrialAdjustingDelayDiscountingTask",
                             "BirthAndBreastfeedingHistory", "AouSocialDeterminantsOfHealth", "FoodInsecurity",
                             "InformationAboutTheHousehold", "SocialNetworksAndSocialCircles", "Geolocation",
                             "EatingAttitudesAndBehaviors", "ModifiedFoodCravingInventory", "DietAcceptabilityScale",
                             "PatientHealthQuestionnaire", "PerceivedStressScale", "EligibilityEvaluation", "Height",
                             "Weight", "Circumference", "VitalSigns", "CgmApplicationRemoval",
                             "AccelerometerHrApplicationRemoval", "Bia", "DietHistoryQuestionnaire", "Randomization",
                             "DietaryAssessmentOrientation", "DietaryAssessment", "AdverseEventEvaluation",
                             "LiquidMmtt",
                             "OnDietMealTest", "VisualAnalogueScale", "OnDietMealsProvided", "GripStrength", "Domicile",
                             "LongCorridorWalkPushTest", "GutTransitTime", "RestingMetabolicRate", "DoublyLabeledWater",
                             "Dxa", "PregnancyTest", "Awardee", "Organization", "Site", "EnrollmentStatus"],
        "event": ["AouBasicsQuestionnaire", "AouOverallHealthQuestionnaire", "AouLifestyleQuestionnaire"],
        "string": ["EnrollmentSite", "FirstName", "MiddleName", "LastName", "State", "City", "StreetAddress",
                   "StreetAddress2", "PhoneNumber", "LoginPhoneNumber", "Email", "PrimaryLanguage", "RecontactMethod",
                   "AgeRange", "GenderIdentity", "Race", "Sex", "SexualOrientation", "OnsiteIdVerificationSite",
                   "OnsiteIdVerificationUser", "EnrollmentStatus", "EhrConsentExpireStatus", "WithdrawlStatus",
                   "WithdrawlReason", "WithdrawlReasonJustification", "ParticipantOrigin", "BiospcimenSourceSite",
                   "BiospecimenCollectedSite", "BiospecimenAliquotSite", "BiospecimenFinalizedSite", "NphModule"],
        "int": ["ParticipantNphId", "BiobankId", "VisitNumber"],
        "date": ["LastModified", "DateOfBirth", "OnsiteIdVerificationTime", "WithdrawlTime", "WithdrawlAuthored",
                 "TimeInterval", "VisitStarted", "VisitCompleted", "ModuleStarted", "ModuleCompleted"],
        "sample_collection": ["Sample8.5mLSSTS1", "Sample4mLSSTS1", "Sample8mLLHPSTP1", "Sample4.5mLLHPSTP1",
                              "Sample2mLP800P1", "Sample10mLEDTAP1", "Sample6mLEDTAP1", "Sample4mLEDTAP1", "SampleRU1",
                              "SampleRU2", "SampleRU3", "SampleTU1", "SampleSA1", "SampleSA2", "SampleHA1", "SampleNA1",
                              "SampleNA2", "SampleST1", "SampleST2", "SampleST3", "SampleST4"]
    }
)

camel_case_fields = MappingProxyType(
    {
        "InformedConsentModule1": "informed_consent_module_1",
        "InformedConsentModule2": "informed_consent_module_2",
        "InformedConsentModule3": "informed_consent_module_3",
        "AouBasicsQuestionnaire": "aou_basics_questionnaire",
        "AouOverallHealthQuestionnaire": "aou_overall_health_questionnaire",
        "AouLifestyleQuestionnaire": "aou_lifestyle_questionnaire",
        "RetrospectiveVisualAnalogueScales": "retrospective_visual_analogue_scales",
        "EatingInventory": "eating_inventory",
        "FoodCravingInventory": "food_craving_inventory",
        "MultifactorialAssessmentOfEatingDisordersSymptoms": "multifactorial_assessment_of_eating_disorders_symptoms",
        "IntuitiveEatingScale2": "intuitive_eating_scale_2",
        "RepetitiveEatingQuestionnaire": "repetitive_eating_questionnaire",
        "BarrattImpulsivityScale11": "barratt_impulsivity_scale_11",
        "DietSatisfactionQuestionnaire": "diet_satisfaction_questionnaire",
        "WeightHistoryQuestionnaire": "weight_history_questionnaire",
        "NhanesSleepDisorderQuestionnaire": "nhanes_sleep_disorder_questionnaire",
        "MunichChronotypeQuestionnaire": "munich_chronotype_questionnaire",
        "MacArthurScaleOfSubjectiveSocialStatus": "mac_arthur_scale_of_subjective_social_status",
        "AouQualityOfLife": "aou_quality_of_life",
        "AouMentalHealthAndWellBeing": "aou_mental_health_and_well_being",
        "GeneralizedAnxietyDisorder": "generalized_anxiety_disorder",
        "TenItemPersonalityInventory": "ten_item_personality_inventory",
        "ChildhoodAndAdultAdversityTrauma": "childhood_and_adult_adversity_trauma",
        "ChildhoodAdverseEventsQuestionnaire": "childhood_adverse_events_questionnaire",
        "FiveTrialAdjustingDelayDiscountingTask": "five_trial_adjusting_delay_discounting_task",
        "BirthAndBreastfeedingHistory": "birth_and_breastfeeding_history",
        "AouSocialDeterminantsOfHealth": "aou_social_determinants_of_health",
        "FoodInsecurity": "food_insecurity",
        "InformationAboutTheHousehold": "information_about_the_household",
        "SocialNetworksAndSocialCircles": "social_networks_and_social_circles",
        "Geolocation": "geolocation",
        "EatingAttitudesAndBehaviors": "eating_attitudes_and_behaviors",
        "ModifiedFoodCravingInventory": "modified_food_craving_inventory",
        "DietAcceptabilityScale": "diet_acceptability_scale",
        "PatientHealthQuestionnaire": "patient_health_questionnaire",
        "PerceivedStressScale": "perceived_stress_scale",
        "EligibilityEvaluation": "eligibility_evaluation",
        "Height": "height",
        "Weight": "weight",
        "Circumference": "circumference",
        "VitalSigns": "vital_signs",
        "CgmApplicationRemoval": "cgm_application_removal",
        "AccelerometerHrApplicationRemoval": "accelerometer_hr_application_removal",
        "Bia": "bia",
        "DietHistoryQuestionnaire": "diet_history_questionnaire",
        "Randomization": "randomization",
        "DietaryAssessmentOrientation": "dietary_assessment_orientation",
        "DietaryAssessment": "dietary_assessment",
        "AdverseEventEvaluation": "adverse_event_evaluation",
        "LiquidMmtt": "liquid_mmtt",
        "OnDietMealTest": "on_diet_meal_test",
        "VisualAnalogueScale": "visual_analogue_scale",
        "OnDietMealsProvided": "on_diet_meals_provided",
        "GripStrength": "grip_strength",
        "Domicile": "domicile",
        "LongCorridorWalkPushTest": "long_corridor_walk_push_test",
        "GutTransitTime": "gut_transit_time",
        "RestingMetabolicRate": "resting_metabolic_rate",
        "DoublyLabeledWater": "doubly_labeled_water",
        "Dxa": "dxa",
        "PregnancyTest": "pregnancy_test",
        "Awardee": "awardee",
        "Organization": "organization",
        "Site": "site",
        "EnrollmentStatus": "enrollment_status",
        "EnrollmentSite": "enrollment_site",
        "ParticipantNphId": "participant_nph_id",
        "BiobankId": "biobank_id",
        "FirstName": "first_name",
        "MiddleName": "middle_name",
        "LastName": "last_name",
        "State": "state",
        "City": "city",
        "StreetAddress": "street_address",
        "StreetAddress2": "street_address_2",
        "PhoneNumber": "phone_number",
        "LoginPhoneNumber": "login_phone_number",
        "Email": "email",
        "PrimaryLanguage": "primary_language",
        "RecontactMethod": "recontact_method",
        "DateOfBirth": "date_of_birth",
        "AgeRange": "age_range",
        "GenderIdentity": "gender_identity",
        "Race": "race",
        "Sex": "sex",
        "SexOrientation": "sex_orientation",
        "OnsiteIdVerificationSite": "onsite_id_verification_site",
        "OnsiteIdVerificationUser": "onsite_id_verification_user",
        "OnsiteIdVerificationTime": "onsite_id_verification_time",
        "LastModified": "last_modified",
        "EhrConsentExpireStatus": "ehr_consent_expire_status",
        "WithdrawalStatus": "withdrawal_status",
        "WithdrawalReason": "withdrawal_reason",
        "WithdrawalTime": "withdrawal_time",
        "WithdrawalAuthored": "withdrawal_authored",
        "WithdrawalReasonJustification": "withdrawal_reason_justification",
        "ParticipantOrigin": "participant_origin",
        "BiospecimenSourceSite": "biospecimen_source_site",
        "BiospecimenCollectedSite": "biospecimen_collected_site",
        "BiospecimenAliquotSite": "biospecimen_aliquot_site",
        "BiospecimenFinalizedSite": "biospecimen_finalized_site",
        "NphModule": "nph_module",
        "VisitNumber": "visit_number",
        "TimeInterval": "time_interval",
        "VisitStarted": "visit_started",
        "VisitCompleted": "visit_completed",
        "ModuleStarted": "module_started",
        "ModuleCompleted": "module_completed",
        "Sample8.5mLSSTS1": "sample_8_5ml_ssts_1",
        "Sample4mLSSTS1": "sample_4ml_ssts_1",
        "Sample8mLLHPSTP1": "sample_8_ml_lhpstp_1",
        "Sample4.5mLLHPSTP1": "sample_4_5ml_lhpstp_1",
        "Sample2mLP800P1": "sample_2ml_p800p_1",
        "Sample10mLEDTAP1": "sample_10ml_edtap_1",
        "Sample6mLEDTAP1": "sample_6ml_edtap_1",
        "Sample4mLEDTAP1": "sample_4ml_edtap_1",
        "SampleRU1": "sample_ru_1",
        "SampleRU2": "sample_ru_2",
        "SampleRU3": "sample_ru_3",
        "SampleTU1": "sample_tu_1",
        "SampleSA1": "sample_sa_1",
        "SampleSA2": "sample_sa_2",
        "SampleHA1": "sample_ha_1",
        "SampleNA1": "sample_na_1",
        "SampleNA2": "sample_na_1",
        "SampleST1": "sample_st_1",
        "SampleST2": "sample_st_2",
        "SampleST3": "sample_st_3",
        "SampleST4": "sample_st_1"
    }
)


def validation_error_message(errors):

    return {"error": [error.formatted for error in errors]}


def error_message(message):

    return {"errors": message}
