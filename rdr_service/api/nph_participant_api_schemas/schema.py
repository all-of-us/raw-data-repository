from graphene import (
    ObjectType,
    String,
    Int,
    Date,
    DateTime,
    NonNull,
    Field,
    List,
    Interface
)
from graphene import relay

from rdr_service.api.nph_participant_api_schemas import db


class Event(ObjectType):
    """ NPH Participant Event Status """

    value = NonNull(String)
    time = NonNull(DateTime)


class EventInterface(Interface):
    """ NPH Participant Event Status """

    value = NonNull(String)
    time = NonNull(DateTime)


class EventObjectType(ObjectType):
    current = Field(Event)
    # TODO: historical field need to sort by newest to oldest for a given aspect of a participant’s data
    historical = List(Event, default_value=[])


class EventCollection(Interface):
    """ List of a NPH Participant Event including historical events sort by newest to oldest """

    current = Field(Event)
    # TODO: historical field need to sort by newest to oldest for a given aspect of a participant’s data
    historical = List(Event, default_value=[])


class Sample(ObjectType):
    parent = List(EventObjectType)
    child = List(EventObjectType)


class SampleCollection(Interface):
    ordered = List(Sample)
    stored = List(Sample)


#  BASED ON SAMPLE-COLLECTION interfaces
class Sample8_5mLSSTS1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample4mLSSTS1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample8mLLHPSTP1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample4_5mLLHPSTP1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample2mLP800P1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample10mLEDTAP1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample6mLEDTAP1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class Sample4mLEDTAP1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleRU1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleRU2(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleRU3(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleTU1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleSA1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleSA2(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleHA1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleNA1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleNA2(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleST1(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleST2(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleST3(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class SampleST4(ObjectType):
    class Meta:
        interfaces = (SampleCollection, )


class AouBasicsQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventInterface, )


class AouOverallHealthQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventInterface, )


class AouLifestyleQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventInterface, )


#  BASED ON EVENT-COLLECTION interfaces
class InformedConsentModule1(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class InformedConsentModule2(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class InformedConsentModule3(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class RetrospectiveVisualAnalogueScales(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class EatingInventory(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class FoodCravingInventory(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class MultifactorialAssessmentOfEatingDisordersSymptoms(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class IntuitiveEatingScale2(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class RepetitiveEatingQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class BarrattImpulsivityScale11(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class DietSatisfactionQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class WeightHistoryQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class NhanesSleepDisorderQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class MunichChronotypeQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class MacArthurScaleOfSubjectiveSocialStatus(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class AouQualityOfLife(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class PatientHealthQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class AouMentalHealthAndWellBeing(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class GeneralizedAnxietyDisorder(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class TenItemPersonalityInventory(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class ChildhoodAndAdultAdversityTrauma(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class ChildhoodAdverseEventsQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class FiveTrialAdjustingDelayDiscountingTask(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class BirthAndBreastfeedingHistory(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class AouSocialDeterminantsOfHealth(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class FoodInsecurity(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class InformationAboutTheHousehold(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class SocialNetworksAndSocialCircles(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Geolocation(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class EatingAttitudesAndBehaviors(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class ModifiedFoodCravingInventory(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class DietAcceptabilityScale(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class PerceivedStressScale(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class EligibilityEvaluation(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Height(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Weight(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Circumference(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class VitalSigns(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class CgmApplicationRemoval(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class AccelerometerHrApplicationRemoval(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Bia(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class DietHistoryQuestionnaire(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Randomization(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class DietaryAssessmentOrientation(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class DietaryAssessment(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class AdverseEventEvaluation(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class LiquidMmtt(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class OnDietMealTest(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class VisualAnalogueScale(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class OnDietMealsProvided(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class GripStrength(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Domicile(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class LongCorridorWalkPushTest(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class GutTransitTime(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class RestingMetabolicRate(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class DoublyLabeledWater(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Dxa(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class PregnancyTest(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Awardee(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Organization(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class Site(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class EnrollmentStatus(ObjectType):
    class Meta:
        interfaces = (EventCollection, )


class ParticipantSchema(ObjectType):

    accelerometer_hr_application_removal = Field(AccelerometerHrApplicationRemoval)
    adverse_event_evaluation = Field(AdverseEventEvaluation)
    aou_basics_questionnaire = Field(AouBasicsQuestionnaire)
    aou_overall_health_questionnaire = Field(AouOverallHealthQuestionnaire)
    aou_lifestyle_questionnaire = Field(AouLifestyleQuestionnaire)
    informed_consent_module_1 = Field(InformedConsentModule1)
    informed_consent_module_2 = Field(InformedConsentModule2)
    informed_consent_module_3 = Field(InformedConsentModule3)
    retrospective_visual_analogue_scales = Field(RetrospectiveVisualAnalogueScales)
    eating_inventory = Field(EatingInventory)
    food_craving_inventory = Field(FoodCravingInventory)
    multifactorial_assessment_of_eating_disorders_symptoms = \
        Field(MultifactorialAssessmentOfEatingDisordersSymptoms)
    intuitive_eating_scale_2 = Field(IntuitiveEatingScale2)
    repetitive_eating_questionnaire = Field(RepetitiveEatingQuestionnaire)
    barratt_impulsivity_scale_11 = Field(BarrattImpulsivityScale11)
    diet_satisfaction_questionnaire = Field(DietSatisfactionQuestionnaire)
    weight_history_questionnaire = Field(WeightHistoryQuestionnaire)
    nhanes_sleep_disorder_questionnaire = Field(NhanesSleepDisorderQuestionnaire)
    munich_chronotype_questionnaire = Field(MunichChronotypeQuestionnaire)
    mac_arthur_scale_of_subjective_social_status = Field(MacArthurScaleOfSubjectiveSocialStatus)
    aou_quality_of_life = Field(AouQualityOfLife)
    patient_health_questionnaire = Field(PatientHealthQuestionnaire)
    aou_mental_health_and_well_being = Field(AouMentalHealthAndWellBeing)
    generalized_anxiety_disorder = Field(GeneralizedAnxietyDisorder)
    ten_item_personality_inventory = Field(TenItemPersonalityInventory)
    childhood_and_adult_adversity_trauma = Field(ChildhoodAndAdultAdversityTrauma)
    childhood_adverse_events_questionnaire = Field(ChildhoodAdverseEventsQuestionnaire)
    five_trial_adjusting_delay_discounting_task = Field(FiveTrialAdjustingDelayDiscountingTask)
    birth_and_breastfeeding_history = Field(BirthAndBreastfeedingHistory)
    aou_social_determinants_of_health = Field(AouSocialDeterminantsOfHealth)
    food_insecurity = Field(FoodInsecurity)
    information_about_the_household = Field(InformationAboutTheHousehold)
    social_networks_and_social_circles = Field(SocialNetworksAndSocialCircles)
    geolocation = Field(Geolocation)
    eating_attitudes_and_behaviors = Field(EatingAttitudesAndBehaviors)
    modified_food_craving_inventory = Field(ModifiedFoodCravingInventory)
    diet_acceptability_scale = Field(DietAcceptabilityScale)
    perceived_stress_scale = Field(PerceivedStressScale)
    eligibility_evaluation = Field(EligibilityEvaluation)
    height = Field(Height)
    weight = Field(Weight)
    circumference = Field(Circumference)
    vital_signs = Field(VitalSigns)
    cgm_application_removal = Field(CgmApplicationRemoval)
    bia = Field(Bia)
    diet_history_questionnaire = Field(DietHistoryQuestionnaire)
    randomization = Field(Randomization)
    dietary_assessment_orientation = Field(DietaryAssessmentOrientation)
    dietary_assessment = Field(DietaryAssessment)
    liquid_mmtt = Field(LiquidMmtt)
    on_diet_meal_test = Field(OnDietMealTest)
    visual_analogue_scale = Field(VisualAnalogueScale)
    on_diet_meals_provided = Field(OnDietMealsProvided)
    grip_strength = Field(GripStrength)
    domicile = Field(Domicile)
    long_corridor_walk_push_test = Field(LongCorridorWalkPushTest)
    gut_transit_time = Field(GutTransitTime)
    resting_metabolic_rate = Field(RestingMetabolicRate)
    doubly_labeled_water = Field(DoublyLabeledWater)
    dxa = Field(Dxa)
    pregnancy_test = Field(PregnancyTest)
    Awardee = Field(Awardee)
    organization = Field(Organization)
    site = Field(Site)
    enrollment_status = Field(EnrollmentStatus)
    sample_8_5ml_ssts_1 = Field(Sample8_5mLSSTS1)
    sample_4ml_ssts_1 = Field(Sample4mLSSTS1)
    sample_8_ml_lhpstp_1 = Field(Sample8mLLHPSTP1)
    sample_4_5ml_lhpstp_1 = Field(Sample4_5mLLHPSTP1)
    sample_2ml_p800p_1 = Field(Sample2mLP800P1)
    sample_10ml_edtap_1 = Field(Sample10mLEDTAP1)
    sample_6ml_edtap_1 = Field(Sample6mLEDTAP1)
    sample_4ml_edtap_1 = Field(Sample4mLEDTAP1)
    sample_ru_1 = Field(SampleRU1)
    sample_ru_2 = Field(SampleRU2)
    sample_ru_3 = Field(SampleRU3)
    sample_tu_1 = Field(SampleTU1)
    sample_sa_1 = Field(SampleSA1)
    sample_sa_2 = Field(SampleSA2)
    sample_ha_1 = Field(SampleHA1)
    sample_na_1 = Field(SampleNA1)
    sample_na_2 = Field(SampleNA2)
    sample_st_1 = Field(SampleST1)
    sample_st_2 = Field(SampleST2)
    sample_st_3 = Field(SampleST3)
    sample_st_4 = Field(SampleST4)
    first_name = Field(String)
    last_name = Field(String)
    onsite_id_verification_site = Field(String)
    onsite_id_verification_user = Field(String)
    onsite_id_verification_time = Field(Date)
    state = Field(String)
    city = Field(String)
    street_address = Field(String)
    enrollment_site = Field(String, description='This field will not be null')
    participant_nph_id = Field(Int, description='NPH Participant unique identifier')
    bio_bank_id = Field(Int, description='Participant\'s BioBank ID')
    middle_name = String()
    street_address2 = String()
    phone_number = String()
    login_phone_number = Field(String, description='Participant login phone number')
    email = Field(String, description='Participant email address')
    primary_language = Field(String, description='Participant primary language')
    recontact_method = Field(String, description='Participant preferred recontact method')
    date_of_birth = Field(Date, description='Paricipant date of birth')
    age_range = Field(String, description='Participant\'s age range')
    gender_identity = Field(String, description='Participant gender')
    race = Field(String, description='Participant race')
    sex = Field(String, description='Participant')
    sexual_orientation = Field(String, description='Participant sexual orientation')
    last_modified = Field(Date)
    # enrollment_status = Field(String)  TODO: Need to check with Kenny to confirm which one to use (vs Ln 85)
    ehr_consent_expire_status = Field(String)
    withdrawal_status = Field(String)
    withdrawal_reason = Field(String)
    withdrawal_time = Field(Date)
    withdrawal_authored = Field(Date)
    withdrawal_reason_justification = Field(String)
    participant_origin = Field(String)
    biospecimen_source_site = Field(String)
    biospecimen_collected_site = Field(String)
    biospecimen_aliquot_site = Field(String)
    biospecimen_finalized_site = Field(String)
    nph_module = Field(String)
    visit_number = Field(Int)
    time_interval = Field(Int)
    visit_started = Field(DateTime)
    visit_completed = Field(DateTime)
    module_started = Field(DateTime)
    module_completed = Field(DateTime)


class AllParticipantConnection(relay.Connection):
    class Meta:
        node = ParticipantSchema

    total_count = Int()
    result_count = Int()

    def resolve_total_count(root, info, **kwargs):
        print(info, kwargs)
        return len(db.datas)

    def resolve_result_count(root, info , **kwargs):
        print(info, kwargs)
        return len(root.edges)

