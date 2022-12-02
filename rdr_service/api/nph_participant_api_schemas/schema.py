from graphene import (
    ObjectType,
    String,
    Int,
    Date,
    DateTime,
    NonNull,
    Field,
    List,
    Schema
)
from graphene import relay
from rdr_service.api.nph_participant_api_schemas import db


class Event(ObjectType):
    """ NPH Participant Event Status """

    value = NonNull(String)
    time = NonNull(DateTime)


class EventCollection(ObjectType):
    current = Field(Event)
    # TODO: historical field need to sort by newest to oldest for a given aspect of a participant’s data
    historical = List(Event)


class Sample(ObjectType):
    parent = List(EventCollection)
    child = List(EventCollection)


class SampleCollection(ObjectType):
    ordered = List(Sample)
    stored = List(Sample)


class Participant(ObjectType):

    accelerometer_hr_application_removal = Field(EventCollection)
    adverse_event_evaluation = Field(EventCollection)
    aou_basics_questionnaire = Field(Event)
    aou_overall_health_questionnaire = Field(Event)
    aou_lifestyle_questionnaire = Field(Event)
    informed_consent_module_1 = Field(EventCollection)
    informed_consent_module_2 = Field(EventCollection)
    informed_consent_module_3 = Field(EventCollection)
    retrospective_visual_analogue_scales = Field(EventCollection)
    eating_inventory = Field(EventCollection)
    food_craving_inventory = Field(EventCollection)
    multifactorial_assessment_of_eating_disorders_symptoms = \
        Field(EventCollection)
    intuitive_eating_scale_2 = Field(EventCollection)
    repetitive_eating_questionnaire = Field(EventCollection)
    barratt_impulsivity_scale_11 = Field(EventCollection)
    diet_satisfaction_questionnaire = Field(EventCollection)
    weight_history_questionnaire = Field(EventCollection)
    nhanes_sleep_disorder_questionnaire = Field(EventCollection)
    munich_chronotype_questionnaire = Field(EventCollection)
    mac_arthur_scale_of_subjective_social_status = Field(EventCollection)
    aou_quality_of_life = Field(EventCollection)
    patient_health_questionnaire = Field(EventCollection)
    aou_mental_health_and_well_being = Field(EventCollection)
    generalized_anxiety_disorder = Field(EventCollection)
    ten_item_personality_inventory = Field(EventCollection)
    childhood_and_adult_adversity_trauma = Field(EventCollection)
    childhood_adverse_events_questionnaire = Field(EventCollection)
    five_trial_adjusting_delay_discounting_task = Field(EventCollection)
    birth_and_breastfeeding_history = Field(EventCollection)
    aou_social_determinants_of_health = Field(EventCollection)
    food_insecurity = Field(EventCollection)
    information_about_the_household = Field(EventCollection)
    social_networks_and_social_circles = Field(EventCollection)
    geolocation = Field(EventCollection)
    eating_attitudes_and_behaviors = Field(EventCollection)
    modified_food_craving_inventory = Field(EventCollection)
    diet_acceptability_scale = Field(EventCollection)
    perceived_stress_scale = Field(EventCollection)
    eligibility_evaluation = Field(EventCollection)
    height = Field(EventCollection)
    weight = Field(EventCollection)
    circumference = Field(EventCollection)
    vital_signs = Field(EventCollection)
    cgm_application_removal = Field(EventCollection)
    bia = Field(EventCollection)
    diet_history_questionnaire = Field(EventCollection)
    randomization = Field(EventCollection)
    dietary_assessment_orientation = Field(EventCollection)
    dietary_assessment = Field(EventCollection)
    liquid_mmtt = Field(EventCollection)
    on_diet_meal_test = Field(EventCollection)
    visual_analogue_scale = Field(EventCollection)
    on_diet_meals_provided = Field(EventCollection)
    grip_strength = Field(EventCollection)
    domicile = Field(EventCollection)
    long_corridor_walk_push_test = Field(EventCollection)
    gut_transit_time = Field(EventCollection)
    resting_metabolic_rate = Field(EventCollection)
    doubly_labeled_water = Field(EventCollection)
    dxa = Field(EventCollection)
    pregnancy_test = Field(EventCollection)
    Awardee = Field(EventCollection)
    organization = Field(EventCollection)
    site = Field(EventCollection)
    enrollment_status = Field(EventCollection)
    sample_8_5ml_ssts_1 = Field(SampleCollection)
    sample_4ml_ssts_1 = Field(SampleCollection)
    sample_8_ml_lhpstp_1 = Field(SampleCollection)
    sample_4_5ml_lhpstp_1 = Field(SampleCollection)
    sample_2ml_p800p_1 = Field(SampleCollection)
    sample_10ml_edtap_1 = Field(SampleCollection)
    sample_6ml_edtap_1 = Field(SampleCollection)
    sample_4ml_edtap_1 = Field(SampleCollection)
    sample_ru_1 = Field(SampleCollection)
    sample_ru_2 = Field(SampleCollection)
    sample_ru_3 = Field(SampleCollection)
    sample_tu_1 = Field(SampleCollection)
    sample_sa_1 = Field(SampleCollection)
    sample_sa_2 = Field(SampleCollection)
    sample_ha_1 = Field(SampleCollection)
    sample_na_1 = Field(SampleCollection)
    sample_na_2 = Field(SampleCollection)
    sample_st_1 = Field(SampleCollection)
    sample_st_2 = Field(SampleCollection)
    sample_st_3 = Field(SampleCollection)
    sample_st_4 = Field(SampleCollection)
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


class ParticipantConnection(relay.Connection):
    class Meta:
        node = Participant

    total_count = Int() #1000
    result_count = Int() #25

    def resolve_total_count(root, info, **kwargs):
        print(info, kwargs)
        return len(db.datas)

    def resolve_result_count(root, info, **kwargs):
        print(info, kwargs)
        return len(root.edges)


class ParticipantQuery(ObjectType):
    class Meta:
        interfaces = (relay.Node,)
        connection_class = ParticipantConnection

    participant = relay.ConnectionField(ParticipantConnection, nph_id=Int(required=False))


    def resolve_participant(root, info, nph_id=None, **kwargs):
        print(info, kwargs)
        try:
            if nph_id:
                return [x for x in db.datas if nph_id == x.get("participant_nph_id")]
            else:
                return db.datas
        except Exception as ex:
            raise ex


NPHParticipantSchema = Schema(query=ParticipantQuery)

