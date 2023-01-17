import logging
from graphene import ObjectType, String, Int, DateTime, NonNull, Field, List, Date, Schema
from graphene import relay
from sqlalchemy.orm import Query

from rdr_service.model.study_nph import Participant as DbParticipant
from rdr_service.model.site import Site
from rdr_service.model.rex import ParticipantMapping
from rdr_service.model.participant_summary import ParticipantSummary as ParticipantSummaryModel
from rdr_service.dao import database_factory
from rdr_service.api.nph_participant_api_schemas.util import SortContext, load_participant_summary_data, \
    schema_field_lookup


class SortableField(Field):
    def __init__(self, *args, sort_modifier=None, **kwargs):
        super(SortableField, self).__init__(*args, **kwargs)
        self.sort_modifier = sort_modifier

    @staticmethod
    def sort(current_class, sort_info, field_name, context):
        return current_class.sort(context, sort_info, field_name)


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
    def sort(context, sort_info, value):
        if value.upper() == "TIME":
            return context.set_order_expression(sort_info.get('time'))
        if value.upper() == 'VALUE':
            return context.set_order_expression(sort_info.get('value'))
        raise ValueError(f"{value} : Invalid Key -- Event Object Type")


class EventCollection(ObjectType):
    current = SortableField(Event)
    # TODO: historical field need to sort by newest to oldest for a given aspect of a participant’s data
    historical = List(Event)

    @staticmethod
    def sort(_, sort_info, value):
        logging.info(sort_info)
        if value.upper() == "HISTORICAL":
            raise ValueError("Sorting Historical is not available.")


class Sample(ObjectType):
    parent = SortableField(EventCollection)
    child = SortableField(EventCollection)

    @staticmethod
    def sort(context, value):
        raise NotImplementedError
        # if value.upper() == 'PARENT':
        #     return context.set_sort_table('sample')
        # if value.upper() == 'CHILD':
        #     return context.add_ref(
        #         NphSample, 'child'
        #     ).add_join(
        #         context.references['child'],
        #         context.references['child'].parent_id == context.references['sample'].id
        #     ).set_sort_table('child')
        # raise NotImplementedError


class SampleCollection(ObjectType):
    ordered = List(Sample)
    stored = SortableField(Sample)

    @staticmethod
    def sort(context, sort_info, _):
        raise NotImplementedError
        # return context.add_ref(
        #     NphSample, 'sample'
        # ).add_join(
        #     context.references['sample'], context.references['sample'].participant_id == DbParticipant.participantId
        # ).add_filter(context.references['sample'].test == context.table)


class Participant(ObjectType):

    participantNphId = SortableField(
        Int, description='NPH participant id for the participant, sourced from NPH participant data table',
        sort_modifier=lambda context: context.set_order_expression(DbParticipant.id)
    )
    biobankId = SortableField(
        Int, description='NPH Biobank id value for the participant, sourced from NPH participant data table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.biobankId)
    )
    firstName = SortableField(String, name="firstName",
                              description='Participant’s first name, sourced from AoU participant_summary table',
                              sort_modifier=lambda context: context.set_order_expression
                              (ParticipantSummaryModel.firstName))
    middleName = SortableField(String, description='Participant’s middle name, sourced from AoU '
                                                   'participant_summary table',
                               sort_modifier=lambda context: context.set_order_expression
                               (ParticipantSummaryModel.middleName))
    lastName = SortableField(String, description='Participant’s last name, sourced from AoU '
                                                 'participant_summary table',
                             sort_modifier=lambda context: context.set_order_expression
                             (ParticipantSummaryModel.lastName))
    dateOfBirth = SortableField(Date, name='DOB', description="Participant's date of birth, sourced from Aou "
                                                              "participant_summary_table",
                                sort_modifier=lambda context: context.set_order_expression
                                (ParticipantSummaryModel.dateOfBirth))
    zipCode = SortableField(String, description='Participant’s zip code, sourced from AoU '
                                                'participant_summary table',
                            sort_modifier=lambda context: context.set_order_expression
                            (ParticipantSummaryModel.zipCode))
    phoneNumber = SortableField(String, description='Participant’s phone number, sourced from AoU '
                                                    'participant_summary table. Use login_phone_number if '
                                                    'available, phone_number column otherwise.',
                                sort_modifier=lambda context: context.set_order_expression
                                (ParticipantSummaryModel.phoneNumber))
    email = SortableField(String, description='Participant’s email address, sourced from AoU '
                                              'participant_summary table',
                          sort_modifier=lambda context: context.set_order_expression
                          (ParticipantSummaryModel.email))
    aianStatus = SortableField(String, name="aouAianStatus",
                               description='Provides whether a participant self identifies as'
                                           ' AIAN. Value should bea “Y” if the participant '
                                           'identifies as AIAN, “N” otherwise. This can be '
                                           'determined from the AoU participant_summary table’s aian column. '
                                           'The time value should be set to the same time that the '
                                           'participant completed TheBasics module '
                                           '(details below) since the AIAN question is '
                                           'contained there.',
                               sort_modifier=lambda context: context.set_order_expression
                               (ParticipantSummaryModel.aian))
    siteId = SortableField(String,
                           description='Google-group name of the site that the participant is paired to. Sourced from'
                                       ' the AoU site table using the site_id column of the participant_summary '
                                       'table.',
                           sort_modifier=lambda context: context.set_order_expression(Site.siteName))
    questionnaireOnTheBasics = SortableField(
        Event, name="aouBasicStatus", description='Provides submission status and authored time for the '
                                                  'participant’s completion of TheBasics module. Value should be '
                                                  'UNSET or SUBMITTED and time should be the authored time. '
                                                  'Both should be sourced from the AoU participant_summary '
                                                  'table.')
    deceasedStatus = SortableField(Event, name="aouDeceasedStatus",
                                   description='Provides deceased information about the participant. Value should '
                                   'be UNSET, PENDING, or APPROVED, and time should be the authored '
                                   'time. Both should be sourced from the AoU participant_summary '
                                   'table.')

    withdrawalStatus = SortableField(Event, name="aouWithdrawalStatus",
                                     description='Provides withdrawal information about the participant. Value '
                                     'should be UNSET, NO_USE, or EARLY_OUT, and time should be the '
                                     'authored time. Both should be sourced from the AoU '
                                     'participant_summary table.')
    suspensionStatus = SortableField(Event, name="aouDeactivationStatus",
                                     description='Provides deactivation (aka suspension) information about the '
                                     'participant. Value should be NOT_SUSPENDED or NO_CONTACT, and '
                                     'time should be the corresponding time. Both should be sourced '
                                     'from the AoU participant_summary table’s suspension columns')
    enrollmentStatus = SortableField(Event, name="aouEnrollmentStatus",
                                     description='Value should provide a string giving the participant’s enrollment'
                                     ' status (MEMBER, FULL_PARTICIPANT, CORE, …). Time should be the '
                                     'latest non-empty timestamp from the set of legacy enrollment '
                                     'fields. Both should be sourced from the AoU participant_summary '
                                     'table.')

    questionnaireOnHealthcareAccess = SortableField(Event, name="aouOverallHealthStatus",
                                                    description='Provides submission status and authored time for the '
                                                    'participant’s completion of the OverallHealth module. '
                                                    'Value should be UNSET or SUBMITTED and time should be '
                                                    'the authored time. Both should be sourced from the '
                                                    'AoU participant_summary table.')
    questionnaireOnLifestyle = SortableField(Event, name="aouLifestyleStatus",
                                             description='Provides submission status and authored time for the '
                                             'participant’s completion of the Lifestyle module. Value '
                                             'should be UNSET or SUBMITTED and time should be the '
                                             'authored time. Both should be sourced from the AoU '
                                             'participant_summary table.')
    questionnaireOnSocialDeterminantsOfHealth = SortableField(
        Event, name="aouSDOHStatus", description='Provides submission status and authored time for the participant’s '
                                                 'completion of the SDOH module. Value should be UNSET or SUBMITTED '
                                                 'and time should be the authored time. Both should be sourced from '
                                                 'the AoU participant_summary table.')
    sample_8_5ml_ssts_1 = Field(SampleCollection, description='Sample 8.5ml SSTS1')
    sample_4ml_ssts_1 = Field(SampleCollection, description='Sample 4ml SSTS1')
    sample_8ml_lhpstp_1 = Field(SampleCollection, description='Sample 8ml LHPSTP1')
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
    nph_paired_site = Field(String, description='Sourced from NPH Schema.')
    nph_enrollment_status = Field(Event, description='Sourced from NPH Schema.')
    nph_withdrawal_status = Field(Event, description='Sourced from NPH Schema.')
    nph_deactivation_status = Field(Event, description='Sourced from NPH Schema.')

    @staticmethod
    def sort(context, sort_info, _):
        context.set_table(sort_info.get("table"))


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
            query = sessions.query(ParticipantSummaryModel, Site, ParticipantMapping) \
                .join(Site, ParticipantSummaryModel.siteId == Site.siteId) \
                .join(ParticipantMapping,
                      ParticipantSummaryModel.participantId == ParticipantMapping.primary_participant_id)
            current_class = Participant
            sort_context = SortContext(query)
            # sampleSA2:ordered:child:current:time
            try:
                if sort_by:
                    sort_parts = sort_by.split(':')
                    sort_info = schema_field_lookup(sort_parts[0])
                    logging.info('sort by: %s', sort_parts)
                    if len(sort_parts) == 1:
                        sort_field: SortableField = getattr(current_class, sort_info.get("field"))
                        sort_field.sort_modifier(sort_context)
                    else:
                        sort_parts[0] = sort_info.get("field")
                        for sort_field_name in sort_parts:
                            sort_field: SortableField = getattr(current_class, sort_field_name)
                            sort_field.sort(current_class, sort_info, sort_field_name, sort_context)
                            current_class = sort_field.type

                if nph_id:
                    logging.info('Fetch NPH ID: %d', nph_id)
                    query = query.filter(ParticipantMapping.ancillary_participant_id == nph_id)
                    logging.info(query)
                    return load_participant_summary_data(query)
                query = sort_context.get_resulting_query()
                if limit:
                    query = query.limit(limit)
                if off_set:
                    query = query.offset(off_set)
                logging.info(query)
                return load_participant_summary_data(query)
            except Exception as ex:
                logging.error(ex)
                raise ex


NPHParticipantSchema = Schema(query=ParticipantQuery)

