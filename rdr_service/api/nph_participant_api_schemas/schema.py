import logging
from graphene import ObjectType, String, Int, DateTime, NonNull, Field, List, Schema
from graphene import relay
from sqlalchemy.orm import Query

from rdr_service.model.participant import Participant as DbParticipant
from rdr_service.model.nph_sample import NphSample
from rdr_service.dao import database_factory
from rdr_service.api.nph_participant_api_schemas.util import SortContext, load_participant_data


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
        if value.upper() == 'VALUE':
            return context.set_order_expression(context.sort_table.status)
        raise ValueError(f"{value} : Invalid Key -- Event Object Type")


class EventCollection(ObjectType):
    current = SortableField(Event)
    # TODO: historical field need to sort by newest to oldest for a given aspect of a participant’s data
    historical = List(Event)

    @staticmethod
    def sort(_, value):
        if value.upper() == "HISTORICAL":
            raise ValueError("Sorting Historical is not available.")


class Sample(ObjectType):
    parent = SortableField(EventCollection)
    child = SortableField(EventCollection)

    @staticmethod
    def sort(context, value):
        if value.upper() == 'PARENT':
            return context.set_sort_table('sample')
        if value.upper() == 'CHILD':
            return context.add_ref(
                NphSample, 'child'
            ).add_join(
                context.references['child'],
                context.references['child'].parent_id == context.references['sample'].id
            ).set_sort_table('child')
        raise ValueError(f"{value} : Invalid Key -- Sample Object Type")


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

    participantNphId = SortableField(
        Int, description='NPH participant id for the participant, sourced from NPH participant data table',
        name='ParticipantNphId', sort_modifier=lambda context: context.set_order_expression(DbParticipant.participantId)
    )
    biobankId = SortableField(
        Int, description='NPH Biobank id value for the participant, sourced from NPH participant data table',
        name='BiobankId', sort_modifier=lambda context: context.set_order_expression(DbParticipant.biobankId)
    )
    first_name = Field(String, name='FirstName', description='Participant’s first name, sourced from AoU participant_'
                                                             'summary table')
    middle_name = Field(String, name='MiddleName', description='Participant’s middle name, sourced from AoU '
                                                               'participant_summary table')
    last_name = Field(String, name='LastName', description='Participant’s last name, sourced from AoU '
                                                           'participant_summary table')
    zip_code = Field(String, name='ZipCode', description='Participant’s zip code, sourced from AoU '
                                                         'participant_summary table')
    phone_number = Field(String, name='PhoneNumber', description='Participant’s phone number, sourced from AoU '
                                                                 'participant_summary table. Use login_phone_number if '
                                                                 'available, phone_number column otherwise.')
    email = Field(String, name='Email', description='Participant’s email address, sourced from AoU '
                                                    'participant_summary table')
    aou_aian_status = Field(Event, name='AouAianStatus', description='Provides whether a participant self identifies as'
                                                                     ' AIAN. Value should bea “Y” if the participant '
                                                                     'identifies as AIAN, “N” otherwise. This can be '
                                                                     'determined from the AoU participant_'
                                                                     'summary table’s aian column. The time value '
                                                                     'should be set to the same time that the '
                                                                     'participant completed TheBasics module '
                                                                     '(details below) since the AIAN question is '
                                                                     'contained there.')
    aou_basics_questionnaire = Field(Event, name='AouBasicsQuestionnaire',
                                     description='Provides submission status and authored time for the '
                                     'participant’s completion of TheBasics module. Value should be '
                                     'UNSET or SUBMITTED and time should be the authored time. '
                                     'Both should be sourced from the AoU participant_summary '
                                     'table.')
    aou_deceased_status = Field(Event, name='AouDeceasedStatus',
                                description='Provides deceased information about the participant. Value should '
                                'be UNSET, PENDING, or APPROVED, and time should be the authored '
                                'time. Both should be sourced from the AoU participant_summary '
                                'table.')
    aou_withdrawal_status = Field(Event, name='AouWithdrawalStatus',
                                  description='Provides withdrawal information about the participant. Value '
                                  'should be UNSET, NO_USE, or EARLY_OUT, and time should be the '
                                  'authored time. Both should be sourced from the AoU '
                                  'participant_summary table.')
    aou_deactivation_status = Field(Event, name='AouDeactivationStatus',
                                    description='Provides deactivation (aka suspension) information about the '
                                    'participant. Value should be NOT_SUSPENDED or NO_CONTACT, and '
                                    'time should be the corresponding time. Both should be sourced '
                                    'from the AoU participant_summary table’s suspension columns')
    aou_site = Field(String, name='AouSite',
                     description='Google-group name of the site that the participant is paired to. Sourced from'
                     ' the AoU site table using the site_id column of the participant_summary '
                     'table.')
    aou_enrollment_status = Field(Event, name='AouEnrollmentStatus',
                                  description='Value should provide a string giving the participant’s enrollment'
                                  ' status (MEMBER, FULL_PARTICIPANT, CORE, …). Time should be the '
                                  'latest non-empty timestamp from the set of legacy enrollment '
                                  'fields. Both should be sourced from the AoU participant_summary '
                                  'table.')
    nph_paired_site = Field(String, name='NphPairedSite', description='Sourced from NPH Schema.')
    nph_enrollment_site = Field(String, name='NphEnrollmentStatus', description='Sourced from NPH Schema.')
    nph_withdrawal_site = Field(Event, name='NphWithdrawalStatus', description='Sourced from NPH Schema.')
    nph_deactivation_status = Field(Event, name='NphDeactivationStatus', description='Sourced from NPH Schema.')
    aou_overall_health_questionnaire = Field(Event, name='AouOverallHealthQuestionnaire',
                                             description='Provides submission status and authored time for the '
                                             'participant’s completion of the OverallHealth module. '
                                             'Value should be UNSET or SUBMITTED and time should be '
                                             'the authored time. Both should be sourced from the '
                                             'AoU participant_summary table.')
    aou_lifestyle_questionnaire = Field(Event, name='AouLifestyleQuestionnaire',
                                        description='Provides submission status and authored time for the '
                                        'participant’s completion of the Lifestyle module. Value '
                                        'should be UNSET or SUBMITTED and time should be the '
                                        'authored time. Both should be sourced from the AoU '
                                        'participant_summary table.')
    aou_sdoh_questionnaire = Field(Event, name='AouSdohQuestionnaire',
                                   description='Provides submission status and authored time for the '
                                   'participant’s completion of the SDOH module. Value should be '
                                   'UNSET or SUBMITTED and time should be the authored time. Both '
                                   'should be sourced from the AoU participant_summary table.')
    sample_8_5ml_ssts_1 = Field(SampleCollection, name='Sample8_5mLSSTS1', description='Sample 8.5ml SSTS1')
    sample_4ml_ssts_1 = Field(SampleCollection, name='Sample4mLSSTS1', description='Sample 4ml SSTS1')
    sample_8ml_lhpstp_1 = Field(SampleCollection, name='Sample8mLLHPSTP1', description='Sample 8ml LHPSTP1')
    sample_4_5ml_lhpstp_1 = Field(SampleCollection, name='Sample4_5mLLHPSTP1', description='Sample 4.5ml LHPSTP1')
    sample_2ml_p800p_1 = Field(SampleCollection, name='Sample2mLP800P1', description='Sample 2ml P800P1')
    sample_10ml_edtap_1 = Field(SampleCollection, name='Sample10mLEDTAP1', description='Sample 10ml EDTAP1')
    sample_6ml_edtap_1 = Field(SampleCollection, name='Sample6mLEDTAP1', description='Sample 6ml EDTAP1')
    sample_4ml_edtap_1 = Field(SampleCollection, name='Sample4mLEDTAP1', description='Sample 4ml EDTAP1')
    sample_ru_1 = Field(SampleCollection, name='SampleRU1', description='Sample RU1')
    sample_ru_2 = Field(SampleCollection, name='SampleRU2', description='Sample RU2')
    sample_ru_3 = SortableField(SampleCollection, name='SampleRU3', description='Sample RU3')
    sample_tu_1 = Field(SampleCollection, name='SampleTU1', description='Sample TU1')
    sample_sa_1 = Field(SampleCollection, name='SampleSA1', description='Sample SA1')
    sample_sa_2 = SortableField(SampleCollection, name='SampleSA2', description='Sample SA2')
    sample_ha_1 = Field(SampleCollection, name='SampleHA1', description='Sample HA1')
    sample_na_1 = Field(SampleCollection, name='SampleNA1', description='Sample NA1')
    sample_na_2 = Field(SampleCollection, name='SampleNA2', description='Sample NA2')
    sample_st_1 = Field(SampleCollection, name='SampleST1', description='Sample ST1')
    sample_st_2 = Field(SampleCollection, name='SampleST2', description='Sample ST2')
    sample_st_3 = Field(SampleCollection, name='SampleST3', description='Sample ST3')
    sample_st_4 = Field(SampleCollection, name='SampleST4', description='Sample ST4')

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

    @staticmethod
    def resolve_total_count(root, _):
        with database_factory.get_database().session() as sessions:
            logging.debug(root)
            query = Query(DbParticipant)
            query.session = sessions
            return query.count()

    @staticmethod
    def resolve_result_count(root, _):
        return len(root.edges)


class ParticipantQuery(ObjectType):
    class Meta:
        interfaces = (relay.Node,)
        connection_class = ParticipantConnection

    participant = relay.ConnectionField(
        ParticipantConnection, nph_id=Int(required=False), sort_by=String(required=False), limit=Int(required=False),
        off_set=Int(required=False))

    @staticmethod
    def resolve_participant(root, info, nph_id=None, sort_by=None, limit=None, off_set=None, **kwargs):
        with database_factory.get_database().session() as sessions:
            logging.info('root: %s, info: %s, kwargs: %s', root, info, kwargs)
            query = Query(DbParticipant)
            query.session = sessions
            current_class = Participant
            sort_context = SortContext(query)

            # sampleSA2:ordered:child:current:time
            try:
                if sort_by:
                    sort_parts = sort_by.split(':')
                    logging.info('sort by: %s', sort_parts)
                    if len(sort_parts) == 1:
                        sort_field: SortableField = getattr(current_class, sort_parts[0])
                        sort_field.sort_modifier(sort_context)
                    else:
                        for sort_field_name in sort_parts:
                            sort_field: SortableField = getattr(current_class, sort_field_name)
                            sort_field.sort(current_class, sort_field_name, sort_context)
                            current_class = sort_field.type

                if nph_id:
                    logging.info('Fetch NPH ID: %d', nph_id)
                    query = query.filter(DbParticipant.participantId == nph_id)
                    logging.info(query)
                    return load_participant_data(query)
                query = sort_context.get_resulting_query()
                if limit:
                    query = query.limit(limit)
                if off_set:
                    query = query.offset(off_set)
                logging.info(query)
                return load_participant_data(query)
            except Exception as ex:
                logging.error(ex)
                raise ex


NPHParticipantSchema = Schema(query=ParticipantQuery)
print(NPHParticipantSchema)
