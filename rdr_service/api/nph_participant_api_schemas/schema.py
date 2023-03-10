import logging
from graphene import ObjectType, String, Int, DateTime, Field, List, Schema, NonNull
from graphene import relay
from sqlalchemy.orm import Query, aliased
from sqlalchemy import and_, func
from sqlalchemy.dialects.mysql import JSON

from rdr_service.config import NPH_PROD_BIOBANK_PREFIX, NPH_TEST_BIOBANK_PREFIX, NPH_STUDY_ID
from rdr_service.model.study_nph import Participant as DbParticipant, Site as nphSite, PairingEvent, DeactivatedEvent, \
    WithdrawalEvent, EnrollmentEvent, EnrollmentEventType, ParticipantOpsDataElement
from rdr_service.model.site import Site
from rdr_service.model.rex import ParticipantMapping, Study
from rdr_service.model.participant_summary import ParticipantSummary as ParticipantSummaryModel
from rdr_service.dao import database_factory
from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.api.nph_participant_api_schemas.util import QueryBuilder, load_participant_summary_data, \
    schema_field_lookup
from rdr_service import config

NPH_BIOBANK_PREFIX = NPH_PROD_BIOBANK_PREFIX if config.GAE_PROJECT == "all-of-us-rdr-prod" else NPH_TEST_BIOBANK_PREFIX


class SortableField(Field):
    def __init__(self, *args, sort_modifier=None, filter_modifier=None, **kwargs):
        super(SortableField, self).__init__(*args, **kwargs)
        self.sort_modifier = sort_modifier
        self.filter_modifier = filter_modifier

    @staticmethod
    def sort(current_class, sort_info, field_name, context):
        return current_class.sort(context, sort_info, field_name)


class Event(ObjectType):
    """ NPH Participant Event Status """

    value = SortableField(
        NonNull(String), sort_modifier=lambda context: context.set_order_expression(context.sort_table.status)
    )
    time = SortableField(
        DateTime,
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


class SampleCollection(ObjectType):
    ordered = List(Sample)
    stored = SortableField(Sample)

    @staticmethod
    def sort(context, sort_info, _):
        raise NotImplementedError


def _build_filter_parameters(cls):
    result = {}
    for name, field_def in cls.__dict__.items():
        if isinstance(field_def, SortableField) and getattr(field_def, 'filter_modifier', None):
            result[name] = field_def.type(required=False)
    return result


class Participant(ObjectType):
    # AOU
    participantNphId = SortableField(
        String,
        description='NPH participant id for the participant, sourced from NPH participant data table',
        sort_modifier=lambda context: context.set_order_expression(DbParticipant.id),
        filter_modifier=lambda context, value: context.add_filter(DbParticipant.id == value)
    )
    biobankId = SortableField(
        String,
        description='NPH Biobank id value for the participant, sourced from NPH participant data table',
        sort_modifier=lambda context: context.set_order_expression(DbParticipant.biobank_id),
        filter_modifier=lambda context, value: context.add_filter(DbParticipant.biobank_id == value)
    )
    firstName = SortableField(
        String,
        name="firstName",
        description='Participant’s first name, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.firstName),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.firstName == value)
    )
    middleName = SortableField(
        String,
        description='Participant’s middle name, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.middleName),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.middleName == value)
    )
    lastName = SortableField(
        String,
        description='Participant’s last name, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.lastName),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.lastName == value)
    )
    dateOfBirth = SortableField(
        String,
        name='DOB',
        description="Participant's date of birth, sourced from Aou participant_summary_table",
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.dateOfBirth),
        filter_modifier=lambda context, value: context.add_filter(
            ParticipantSummaryModel.dateOfBirth == value
        )
    )
    zipCode = SortableField(
        String,
        description='Participant’s zip code, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.zipCode),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.zipCode == value)
    )
    phoneNumber = SortableField(
        String,
        description='''
            Participant’s phone number, sourced from AoU participant_summary table.
            Use login_phone_number if available, phone_number column otherwise.
        ''',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.phoneNumber),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.phoneNumber == value)
    )
    email = SortableField(
        String,
        description='Participant’s email address, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.email),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.email == value)
    )
    aianStatus = SortableField(
        String,
        name="aouAianStatus",
        description='''
            Provides whether a participant self identifies as AIAN. Value should be a “Y” if the participant
            identifies as AIAN, “N” otherwise. This can be determined from the AoU participant_summary table’s
            aian column.
        ''',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummaryModel.aian),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummaryModel.aian == value)
    )
    siteId = SortableField(
        String,
        description='''
            Google-group name of the site that the participant is paired to. Sourced from the AoU site table using
            the site_id column of the participant_summary table.
        ''',
        sort_modifier=lambda context: context.set_order_expression(Site.siteName)
    )
    questionnaireOnTheBasics = SortableField(
        Event,
        name="aouBasicsStatus",
        description='''
            Provides submission status and authored time for the participant’s completion of TheBasics module.
            Value should be UNSET or SUBMITTED and time should be the authored time. Both should be sourced from
            the AoU participant_summary table.
        '''
    )
    deceasedStatus = SortableField(
        Event,
        name="aouDeceasedStatus",
        description='''
            Provides deceased information about the participant. Value should be UNSET, PENDING, or APPROVED,
            and time should be the authored time. Both should be sourced from the AoU participant_summary table.
        '''
    )
    withdrawalStatus = SortableField(
        Event,
        name="aouWithdrawalStatus",
        description='''
            Provides withdrawal information about the participant. Value should be UNSET, NO_USE, or EARLY_OUT, and
            time should be the authored time. Both should be sourced from the AoU participant_summary table.
        '''
    )
    suspensionStatus = SortableField(
        Event,
        name="aouDeactivationStatus",
        description='''
            Provides deactivation (aka suspension) information about the participant. Value should be
            NOT_SUSPENDED or NO_CONTACT, and time should be the corresponding time. Both should be sourced
            from the AoU participant_summary table’s suspension columns
        '''
    )
    aouEnrollmentStatus = SortableField(
        Event,
        name="aouEnrollmentStatus",
        description='''
            Value should provide a string giving the participant’s enrollment status (MEMBER,
            FULL_PARTICIPANT, CORE, …). Time should be the latest non-empty timestamp from the set of legacy enrollment
            fields. Both should be sourced from the AoU participant_summary table.
        '''
    )
    questionnaireOnHealthcareAccess = SortableField(
        Event,
        name="aouOverallHealthStatus",
        description='''
            Provides submission status and authored time for the participant’s completion of the OverallHealth module.
            Value should be UNSET or SUBMITTED and time should be the authored time. Both should be sourced from the
            AoU participant_summary table.
        '''
    )
    questionnaireOnLifestyle = SortableField(
        Event,
        name="aouLifestyleStatus",
        description='''
            Provides submission status and authored time for the participant’s completion of the Lifestyle module.
            Value should be UNSET or SUBMITTED and time should be the authored time. Both should be sourced from the
            AoU participant_summary table.
        '''
    )
    questionnaireOnSocialDeterminantsOfHealth = SortableField(
        Event,
        name="aouSDOHStatus",
        description='''
            Provides submission status and authored time for the participant’s completion of the SDOH module.
            Value should be UNSET or SUBMITTED and time should be the authored time. Both should be sourced from
            the AoU participant_summary table.
        '''
    )
    # NPH
    external_id = SortableField(String, name="nphPairedSite", description='Sourced from NPH Schema.',
                                sort_modifier=lambda context: context.set_order_expression(nphSite.external_id))
    organization_external_id = SortableField(String, name="nphPairedOrg", description='Sourced from NPH Schema.',
                                             sort_modifier=lambda context: context.set_order_expression(
                                                 nphSite.organization_external_id))
    awardee_external_id = SortableField(String, name="nphPairedAwardee", description='Sourced from NPH Schema.',
                                        sort_modifier=lambda context: context.set_order_expression(
                                            nphSite.awardee_external_id))
    nph_enrollment_status = List(Event, name="nphEnrollmentStatus", description='Sourced from NPH Schema.')
    nph_withdrawal_status = SortableField(Event, name="nphWithdrawalStatus", description='Sourced from NPH Schema.')
    nph_deactivation_status = SortableField(Event, name="nphDeactivationStatus", description='Sourced from NPH Schema.')
    # Bio-specimen
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
        ParticipantConnection, nph_id=String(required=False), sort_by=String(required=False), limit=Int(required=False),
        off_set=Int(required=False),
        **_build_filter_parameters(Participant)
    )

    @staticmethod
    def resolve_participant(root, info, nph_id=None, sort_by=None, limit=None, off_set=None, **filter_kwargs):
        with database_factory.get_database().session() as sessions:
            logging.info('root: %s, info: %s, kwargs: %s', root, info, filter_kwargs)
            pm2 = aliased(PairingEvent)
            enrollment_subquery = sessions.query(
                DbParticipant.id.label('enrollment_pid'),
                func.json_object(
                    'enrollment_json',
                    func.json_arrayagg(
                            func.json_object(
                                'time', EnrollmentEvent.event_authored_time,
                                'value', EnrollmentEventType.source_name)
                    ), type_=JSON
                ).label('enrollment_status'),
            ).join(
                EnrollmentEvent,
                EnrollmentEvent.participant_id == DbParticipant.id
            ).join(
                 EnrollmentEventType,
                 EnrollmentEventType.id == EnrollmentEvent.event_type_id,
            ).group_by(DbParticipant.id).subquery()

            query = sessions.query(
                ParticipantSummaryModel,
                Site,
                nphSite,
                ParticipantMapping,
                DbParticipant,
                enrollment_subquery.c.enrollment_status,
                DeactivatedEvent,
                WithdrawalEvent,
                ParticipantOpsDataElement
            ).join(
                Site,
                ParticipantSummaryModel.siteId == Site.siteId
            ).join(
                ParticipantMapping,
                ParticipantSummaryModel.participantId == ParticipantMapping.primary_participant_id
            ).join(
                DbParticipant,
                DbParticipant.id == ParticipantMapping.ancillary_participant_id
            ).join(
                enrollment_subquery,
                enrollment_subquery.c.enrollment_pid == DbParticipant.id
            ).join(
                PairingEvent,
                PairingEvent.participant_id == ParticipantMapping.ancillary_participant_id
            ).outerjoin(
                pm2,
                and_(
                    PairingEvent.participant_id == pm2.participant_id,
                    PairingEvent.event_type_id == pm2.event_type_id,
                    PairingEvent.event_authored_time < pm2.event_authored_time
                )
            ).join(
                nphSite,
                nphSite.id == PairingEvent.site_id
            ).outerjoin(
                 DeactivatedEvent,
                 ParticipantMapping.ancillary_participant_id == DeactivatedEvent.participant_id
            ).outerjoin(
                WithdrawalEvent,
                ParticipantMapping.ancillary_participant_id == WithdrawalEvent.participant_id
            ).outerjoin(
                ParticipantOpsDataElement,
                ParticipantMapping.ancillary_participant_id == ParticipantOpsDataElement.participant_id
            ).filter(
                pm2.id.is_(None),
                ParticipantMapping.ancillary_study_id == NPH_STUDY_ID,
            )
            study_query = sessions.query(Study).filter(Study.schema_name == "nph")
            study = study_query.first()
            current_class = Participant
            query_builder = QueryBuilder(query)

            try:
                if sort_by:
                    sort_parts = sort_by.split(':')
                    sort_info = schema_field_lookup(sort_parts[0])
                    logging.info('sort by: %s', sort_parts)
                    if len(sort_parts) == 1:
                        sort_field: SortableField = getattr(current_class, sort_info.get("field"))
                        sort_field.sort_modifier(query_builder)
                    else:
                        sort_parts[0] = sort_info.get("field")
                        for sort_field_name in sort_parts:
                            sort_field: SortableField = getattr(current_class, sort_field_name)
                            sort_field.sort(current_class, sort_info, sort_field_name, query_builder)
                            current_class = sort_field.type

                for field_name, value in filter_kwargs.items():
                    field_def = getattr(Participant, field_name, None)
                    if not field_def:
                        raise NotImplementedError(f'Unable to filter by {field_name}.')
                    if not field_def.filter_modifier:
                        raise NotImplementedError(f'Filtering by {field_name} is not yet implemented.')
                    field_def.filter_modifier(query_builder, value)

                if nph_id:
                    logging.info('Fetch NPH ID: %d', nph_id)
                    nph_participant_dao = NphParticipantDao()
                    nph_participant_id = nph_participant_dao.convert_id(nph_id)
                    query = query.filter(ParticipantMapping.ancillary_participant_id == int(nph_participant_id))
                    logging.info(query)
                    return load_participant_summary_data(query, study.prefix, NPH_BIOBANK_PREFIX)

                query = query_builder.get_resulting_query()
                if limit:
                    query = query.limit(limit)
                if off_set:
                    query = query.offset(off_set)
                logging.info(query)
                return load_participant_summary_data(query, study.prefix, NPH_BIOBANK_PREFIX)
            except Exception as ex:
                logging.error(ex)
                raise ex


NPHParticipantSchema = Schema(query=ParticipantQuery)
