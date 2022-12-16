import logging
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
from sqlalchemy.orm import Query

from rdr_service.api.nph_participant_api_schemas import db
from rdr_service.model.participant import Participant as DbParticipant
from rdr_service.model.nph_sample import NphSample
from rdr_service.dao import database_factory
from rdr_service.api.nph_participant_api_schemas.util import SortContext


class SortableField(Field):
    def __init__(self, *args, sort_modifier=None, **kwargs):
        super(SortableField, self).__init__(*args, **kwargs)
        self.sort_modifier = sort_modifier

    @staticmethod
    def sort(current_class, field_name, context):
        return current_class.sort(context, field_name)


class Event(ObjectType):
    """ NPH Participant Event Status """

    value = SortableField(
        NonNull(String), sort_modifier=lambda context: context.set_order_expression(context.sort_table.status)
    )
    time = SortableField(
        NonNull(DateTime),
        sort_modifier=lambda context: context.set_order_expression(context.sort_table.time)  # Order by time
    )

    @staticmethod
    def sort(context, value):
        if value.upper() == "TIME":
            return context.set_order_expression(context.sort_table.time)
        return context.set_order_expression(context.sort_table.status)


class EventCollection(ObjectType):
    current = SortableField(Event)
    # TODO: historical field need to sort by newest to oldest for a given aspect of a participant’s data
    historical = List(Event)

    @staticmethod
    def sort(context, value):
        pass


class Sample(ObjectType):
    parent = SortableField(EventCollection)
    child = SortableField(EventCollection)  # todo: join 'child' on parent ('sample') and sort_table = 'child'

    @staticmethod
    def sort(context, value):
        if value.upper() == 'PARENT':
            return context.set_sort_table('sample')
        else:
            return context.add_ref(
                NphSample, 'child'
            ).add_join(
                context.references['child'],
                context.references['child'].parent_id == context.references['sample'].id
            ).set_sort_table('child')


class SampleCollection(ObjectType):
    ordered = List(Sample)
    stored = SortableField(Sample)

    @staticmethod
    def sort(context, _):
        return context.add_ref(
            NphSample, 'sample'
        ).add_join(
            context.references['sample'], context.references['sample'].participant_id == DbParticipant.participantId
        ).add_filter(context.references['sample'].test == context.table)


class Participant(ObjectType):

    accelerometer_hr_application_removal = Field(EventCollection, description='Accelerometer HR application removal')
    adverse_event_evaluation = Field(EventCollection, description='Adverse event evaluation')
    aou_basics_questionnaire = Field(Event, description='AOU basics questionnaire')
    aou_overall_health_questionnaire = Field(Event, description='AOU overall health questionnaire')
    aou_lifestyle_questionnaire = Field(Event, description='AOU lifestyle questionnaire')
    informed_consent_module_1 = Field(EventCollection, description='Informed consent module 1')
    informed_consent_module_2 = Field(EventCollection, description='Informed consent module 2')
    informed_consent_module_3 = Field(EventCollection, description='Informed consent module 3')
    retrospective_visual_analogue_scales = Field(EventCollection, description='Retrospective visual analogue scales')
    eating_inventory = Field(EventCollection, description='Eating Inventory')
    food_craving_inventory = Field(EventCollection, description='Food craving inventory')
    multifactorial_assessment_of_eating_disorders_symptoms = Field(EventCollection,
                                                                   description='Multifactorial assessment of eating '
                                                                               'disorders symptoms')
    intuitive_eating_scale_2 = Field(EventCollection, description='Intuitive eating scale 2')
    repetitive_eating_questionnaire = Field(EventCollection, description='Repetitive eating questionnaire')
    barratt_impulsivity_scale_11 = Field(EventCollection, description='Barratt implusivity scale 11')
    diet_satisfaction_questionnaire = Field(EventCollection, description='Diet satisfaction questionnaire')
    weight_history_questionnaire = Field(EventCollection, description='Weight history questionnaire')
    nhanes_sleep_disorder_questionnaire = Field(EventCollection, description='Nhanes sleep disorder questionnaire')
    munich_chronotype_questionnaire = Field(EventCollection, description='Munich chronotype questionnaire')
    mac_arthur_scale_of_subjective_social_status = Field(EventCollection, description='MacArthur scale of subjective '
                                                                                      'social status')
    aou_quality_of_life = Field(EventCollection, description='AOU Quality of life')
    patient_health_questionnaire = Field(EventCollection, description='Patient health questionnaire')
    aou_mental_health_and_well_being = Field(EventCollection, description='AOU mental health and well being')
    generalized_anxiety_disorder = Field(EventCollection, description='Generalized anxiety disorder')
    ten_item_personality_inventory = Field(EventCollection, description='Ten item personality inventory')
    childhood_and_adult_adversity_trauma = Field(EventCollection, description='Childhood and adult adversity trauma')
    childhood_adverse_events_questionnaire = Field(EventCollection, description='Childhood adverse event questionnaire')
    five_trial_adjusting_delay_discounting_task = Field(EventCollection, description='Five trial adjusting delay '
                                                                                     'discounting task')
    birth_and_breastfeeding_history = Field(EventCollection, description='Birth and breastfeeding history')
    aou_social_determinants_of_health = Field(EventCollection, description='AOU soical determinants of healths')
    food_insecurity = Field(EventCollection, description='Food insecurity')
    information_about_the_household = Field(EventCollection, description='Information about the household')
    social_networks_and_social_circles = Field(EventCollection, description='Social network and social circles')
    geolocation = Field(EventCollection, description='Geolocation')
    eating_attitudes_and_behaviors = Field(EventCollection, description='Eating attitudes and behaviors')
    modified_food_craving_inventory = Field(EventCollection, description='Modified food craving inventory')
    diet_acceptability_scale = Field(EventCollection, description='diet acceptability scale')
    perceived_stress_scale = Field(EventCollection, description='perceived stress scale')
    eligibility_evaluation = Field(EventCollection, description='Eligibility evaluation')
    height = Field(EventCollection, description='Height')
    weight = Field(EventCollection, description='Weight')
    circumference = Field(EventCollection, description='Circumference')
    vital_signs = Field(EventCollection, description='Vital signs')
    cgm_application_removal = Field(EventCollection, description='CGM application removal')
    bia = Field(EventCollection, description='Bia')
    diet_history_questionnaire = Field(EventCollection, description='Diet history questionnaire')
    randomization = Field(EventCollection, description='Randomization')
    dietary_assessment_orientation = Field(EventCollection, description='Dietary assessment orientation')
    dietary_assessment = Field(EventCollection, description='Dietary assessment')
    liquid_mmtt = Field(EventCollection, description='Liquid MMTT')
    on_diet_meal_test = Field(EventCollection, description='On diet meal test')
    visual_analogue_scale = Field(EventCollection, description='Visual analogue scale')
    on_diet_meals_provided = Field(EventCollection, description='On diet meals provided')
    grip_strength = Field(EventCollection, description='Grip strength')
    domicile = Field(EventCollection, description='Domicile')
    long_corridor_walk_push_test = Field(EventCollection, description='Long corridor walk push test')
    gut_transit_time = Field(EventCollection, description='Gut transit time')
    resting_metabolic_rate = Field(EventCollection, description='Resting metabolic rate')
    doubly_labeled_water = Field(EventCollection, description='Doubly labeled water')
    dxa = Field(EventCollection, description='DXA')
    pregnancy_test = Field(EventCollection, description='Pregnancy Test')
    Awardee = Field(EventCollection, description='Awardee')
    organization = Field(EventCollection, description='Organization')
    site = Field(EventCollection, description='Site')
    enrollment_status = Field(EventCollection, description='Enrollment Status')
    sample_8_5ml_ssts_1 = Field(SampleCollection, description='Sample 8.5ml SSTS1')
    sample_4ml_ssts_1 = Field(SampleCollection, description='Sample 4ml SSTS1')
    sample_8_ml_lhpstp_1 = Field(SampleCollection, description='Sample 8ml LHPSTP1')
    sample_4_5ml_lhpstp_1 = Field(SampleCollection, description='Sample 4.5ml LHPSTP1')
    sample_2ml_p800p_1 = Field(SampleCollection, description='Sample 2ml P800P1')
    sample_10ml_edtap_1 = Field(SampleCollection, description='Sample 10ml EDTAP1')
    sample_6ml_edtap_1 = Field(SampleCollection, description='Sample 6ml EDTAP1')
    sample_4ml_edtap_1 = Field(SampleCollection, description='Sample 4ml EDTAP1')
    sample_ru_1 = Field(SampleCollection, description='Sample RU1')
    sample_ru_2 = Field(SampleCollection, description='Sample RU2')
    sampleRU3 = SortableField(SampleCollection, description='Sample RU3')
    sample_tu_1 = Field(SampleCollection, description='Sample TU1')
    sample_sa_1 = Field(SampleCollection, description='Sample SA1')
    sampleSA2 = SortableField(SampleCollection, description='Sample SA2')
    sample_ha_1 = Field(SampleCollection, description='Sample HA1')
    sample_na_1 = Field(SampleCollection, description='Sample NA1')
    sample_na_2 = Field(SampleCollection, description='Sample NA2')
    sample_st_1 = Field(SampleCollection, description='Sample ST1')
    sample_st_2 = Field(SampleCollection, description='Sample ST2')
    sample_st_3 = Field(SampleCollection, description='Sample ST3')
    sample_st_4 = Field(SampleCollection, description='Sample ST4')
    first_name = Field(String, description='Participant first name')
    last_name = Field(String, description='Participant last name')
    onsite_id_verification_site = Field(String, description='Onsite ID verification site')
    onsite_id_verification_user = Field(String, description='Onsite ID verification user')
    onsite_id_verification_time = Field(Date, description='Onsite ID verification time')
    state = Field(String, description='Participant state address')
    city = Field(String, description='Participant city address')
    street_address = Field(String, description='Participant street address')
    enrollment_site = Field(String, description='Participant enrollment site')
    participantNphId = SortableField(
        Int, description='NPH Participant unique identifier',
        sort_modifier=lambda context: context.set_order_expression(DbParticipant.participantId)
    )
    biobankId = SortableField(
        Int, description='Participant\'s BioBank ID',
        sort_modifier=lambda context: context.set_order_expression(DbParticipant.biobankId)
    )
    middle_name = Field(String, description='Participant middle name')
    street_address2 = Field(String, description='Participant street address2')
    phone_number = Field(String, description='Participant phone number')
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
    lastModified = SortableField(
        Date,
        sort_modifier=lambda context: context.set_order_expression(DbParticipant.lastModified)
    )
    ehr_consent_expire_status = Field(String, description='Participant EHR consent expire status')
    withdrawal_status = Field(String, description='Participant withdrawal status')
    withdrawal_reason = Field(String, description='Participant withdrawal reason')
    withdrawal_time = Field(Date, description='Participant time of withdrawal')
    withdrawal_authored = Field(Date, description='Withdrawal authored')
    withdrawal_reason_justification = Field(String, description='Participant withdrawal reason justification')
    participant_origin = Field(String, description='Participant\'s origin')
    biospecimen_source_site = Field(String, description='Biospecimen source site')
    biospecimen_collected_site = Field(String, description='Biospecimen collected site')
    biospecimen_aliquot_site = Field(String, description='Biospecimen aliquot site')
    biospecimen_finalized_site = Field(String, description='Biospecimen finalize site')
    nph_module = Field(String, description='Participant NPH module')
    visit_number = Field(Int, description='Participant\'s visit number')
    time_interval = Field(Int, description='Time interval')
    visit_started = Field(DateTime, description='Participant visit start time')
    visit_completed = Field(DateTime, description='Participant vist completed time')
    module_started = Field(DateTime, description='Participant module start time')
    module_completed = Field(DateTime, description='Participant module completed time')

    @staticmethod
    def sort(context, value):
        if value.upper() == "SAMPLESA2":
            context.set_table("SA2")
        elif value.upper() == "SAMPLERU3":
            context.set_table("RU3")


class ParticipantConnection(relay.Connection):
    class Meta:
        node = Participant

    total_count = Int()
    result_count = Int()

    def resolve_total_count(root, _):
        with database_factory.get_database().session() as sessions:
            query = Query(DbParticipant)
            query.session = sessions
            return query.count()

    def resolve_result_count(root, _):
        return len(root.edges)


class ParticipantQuery(ObjectType):
    class Meta:
        interfaces = (relay.Node,)
        connection_class = ParticipantConnection

    participant = relay.ConnectionField(
        ParticipantConnection, nph_id=Int(required=False), sort_by=String(required=False), limit=Int(required=False),
        off_set=Int(required=False))

    def resolve_participant(root, info, nph_id=None, sort_by=None, limit=None, off_set=None, **kwargs):
        with database_factory.get_database().session() as sessions:
            logging.info(f'info: {info}, kwargs: {kwargs}')
            query = Query(DbParticipant)
            query.session = sessions
            current_class = Participant
            sort_context = SortContext(query)

            # sampleSA2:ordered:child:current:time
            if sort_by:
                sort_parts = sort_by.split(':')
                logging.info(f'sort by: {sort_parts}')
                if len(sort_parts) == 1:
                    sort_field: SortableField = getattr(current_class, sort_parts[0])
                    sort_field.sort_modifier(sort_context)
                else:
                    for sort_field_name in sort_parts:
                        sort_field: SortableField = getattr(current_class, sort_field_name)
                        sort_field.sort(current_class, sort_field_name, sort_context)
                        current_class = sort_field.type

            try:
                if nph_id:
                    logging.info(f'Fetch NPH ID: {nph_id}')
                    query = query.filter(DbParticipant.participantId == nph_id)
                    logging.info(query)
                    return db.loadParticipantData(query)
                query = sort_context.get_resulting_query()
                if limit:
                    query = query.limit(limit)
                if off_set:
                    query = query.offset(off_set)
                logging.info(query)
                return db.loadParticipantData(query)
            except Exception as ex:
                logging.error(ex)
                raise ex


NPHParticipantSchema = Schema(query=ParticipantQuery)
