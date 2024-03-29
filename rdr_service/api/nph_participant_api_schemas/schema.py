import logging
from graphene import ObjectType, String, Int, DateTime, Field, List, Schema, NonNull, relay, Boolean
from sqlalchemy.orm import aliased
from sqlalchemy import and_

from rdr_service.ancillary_study_resources.nph.enums import ParticipantOpsElementTypes
from rdr_service.config import NPH_PROD_BIOBANK_PREFIX, NPH_TEST_BIOBANK_PREFIX, NPH_STUDY_ID
from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.model.study_nph import (
    Participant,
    Site as nphSite,
    PairingEvent,
    ParticipantOpsDataElement,
)
from rdr_service.model.site import Site
from rdr_service.model.rex import ParticipantMapping
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.api.nph_participant_api_schemas.util import QueryBuilder, NphParticipantData
from rdr_service import config

NPH_BIOBANK_PREFIX = NPH_PROD_BIOBANK_PREFIX if config.GAE_PROJECT == "all-of-us-rdr-prod" else NPH_TEST_BIOBANK_PREFIX

DEFAULT_LIMIT = 100
MIN_LIMIT = 1
MAX_LIMIT = 1000

DEFAULT_OFFSET = 0
MIN_OFFSET = 0


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


class GraphQLConsentEvent(Event):
    """ NPH ConsentEvent """
    opt_in = Field(NonNull(String))


class GraphQLActiveEvent(ObjectType):

    value = Field(NonNull(String))
    time = Field(String)
    module = Field(NonNull(String))


class GraphQLDietStatus(ObjectType):

    time = Field(String)
    status = Field(String)
    current = Field(Boolean)


class GraphQLDietEvent(ObjectType):

    dietName = Field(String)
    dietStatus = Field(List(GraphQLDietStatus))


class GraphQLNphBiobankStatus(ObjectType):
    """
    ObjectType to serialize biobankStatus field as a list of dictionaries.
    """
    limsID = Field(String)
    biobankModified = Field(String)
    status = Field(String)


class EventCollection(ObjectType):

    current = SortableField(Event)
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


def _strip_prefix(value: str, pos: int = 0) -> str:
    return value[1:] if not value[pos].isnumeric() else value


class ParticipantField(ObjectType):
    # AOU
    participantNphId = SortableField(
        String,
        description='NPH participant id for the participant, sourced from NPH participant data table',
        sort_modifier=lambda context: context.set_order_expression(Participant.id),
        filter_modifier=lambda context, value: context.add_filter(Participant.id == value)
    )
    biobankId = SortableField(
        String,
        description='NPH Biobank id value for the participant, sourced from NPH participant data table',
        sort_modifier=lambda context: context.set_order_expression(Participant.biobank_id),
        filter_modifier=lambda context, value: context.add_filter(
            Participant.biobank_id == _strip_prefix(value))
    )
    firstName = SortableField(
        String,
        name="firstName",
        description='Participant’s first name, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.firstName),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.firstName == value)
    )
    middleName = SortableField(
        String,
        description='Participant’s middle name, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.middleName),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.middleName == value)
    )
    lastName = SortableField(
        String,
        description='Participant’s last name, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.lastName),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.lastName == value)
    )
    nphDateOfBirth = SortableField(
        String,
        name='nphDateOfBirth',
        description="Participant's date of birth, sourced from Aou participant_summary_table",
        filter_modifier=lambda context, value: context.add_filter(
            and_(ParticipantOpsDataElement.source_data_element == ParticipantOpsElementTypes.lookup_by_name(
                'BIRTHDATE'),
                 ParticipantOpsDataElement.source_value == value)
        )
    )
    zipCode = SortableField(
        String,
        description='Participant’s zip code, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.zipCode),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.zipCode == value)
    )
    phoneNumber = SortableField(
        String,
        description='''
            Participant’s phone number, sourced from AoU participant_summary table.
            Use login_phone_number if available, phone_number column otherwise.
        ''',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.phoneNumber),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.phoneNumber == value)
    )
    email = SortableField(
        String,
        description='Participant’s email address, sourced from AoU participant_summary table',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.email),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.email == value)
    )
    aianStatus = SortableField(
        String,
        name="aouAianStatus",
        description='''
            Provides whether a participant self identifies as AIAN. Value should be a “Y” if the participant
            identifies as AIAN, “N” otherwise. This can be determined from the AoU participant_summary table’s
            aian column.
        ''',
        sort_modifier=lambda context: context.set_order_expression(ParticipantSummary.aian),
        filter_modifier=lambda context, value: context.add_filter(ParticipantSummary.aian == value)
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
    nphPairedSite = SortableField(
        String,
        description='Sourced from NPH Schema.',
        sort_modifier=lambda context: context.set_order_expression(nphSite.external_id),
        filter_modifier=lambda context, value: context.add_filter(nphSite.external_id == value)
    )
    nphPairedOrg = SortableField(
        String,
        description='Sourced from NPH Schema.',
        sort_modifier=lambda context: context.set_order_expression(nphSite.organization_external_id),
        filter_modifier=lambda context, value: context.add_filter(nphSite.organization_external_id == value)
    )
    nphPairedAwardee = SortableField(
        String,
        description='Sourced from NPH Schema.',
        sort_modifier=lambda context: context.set_order_expression(
                                          nphSite.awardee_external_id),
        filter_modifier=lambda context, value: context.add_filter(nphSite.awardee_external_id == value)
    )
    nphEnrollmentStatus = List(Event, name="nphEnrollmentStatus", description='Sourced from NPH Schema.')
    nphModule1ConsentStatus = List(
        GraphQLConsentEvent, name="nphModule1ConsentStatus", description="Sourced from NPH Schema"
    )
    nphModule2ConsentStatus = List(
        GraphQLConsentEvent, name="nphModule2ConsentStatus", description="Sourced from NPH Schema"
    )
    nphModule3ConsentStatus = List(
        GraphQLConsentEvent, name="nphModule3ConsentStatus", description="Sourced from NPH Schema"
    )
    nphModule2DietStatus = List(
        GraphQLDietEvent, name="nphModule2DietStatus", description="Sourced from NPH Schema"
    )
    nphModule3DietStatus = List(
        GraphQLDietEvent, name="nphModule3DietStatus", description="Sourced from NPH Schema"
    )
    nphWithdrawalStatus = List(GraphQLActiveEvent, name="nphWithdrawalStatus", description='Sourced from NPH Schema.')
    nphDeactivationStatus = List(GraphQLActiveEvent, name="nphDeactivationStatus",
                                 description='Sourced from NPH Schema.')
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
        node = ParticipantField

    total_count = Int()
    result_count = Int()

    @staticmethod
    def resolve_total_count(_, info):
        with NphParticipantDao().session() as session:
            participant_query = info.context.get('participant_query')
            if participant_query is None:
                logging.error('graphql context is missing participant_query for resolving total count')
                return 0

            full_query = participant_query.offset(0).limit(None)
            full_query.session = session
            return full_query.count()

    @staticmethod
    def resolve_result_count(root, _):
        return len(root.edges)


class ParticipantQuery(ObjectType):
    class Meta:
        interfaces = (relay.Node,)
        connection_class = ParticipantConnection

    participant = relay.ConnectionField(
        ParticipantConnection,
        nph_id=String(required=False),
        sort_by=String(required=False),
        limit=Int(required=False, default_value=DEFAULT_LIMIT),
        off_set=Int(required=False, default_value=DEFAULT_OFFSET),
        **_build_filter_parameters(ParticipantField)
    )

    @staticmethod
    def resolve_participant(
        root,
        info,
        nph_id=None,
        sort_by=None,
        limit=None,
        off_set=None,
        **filter_kwargs
    ):
        pm2 = aliased(PairingEvent)
        participant_dob = aliased(ParticipantOpsDataElement)

        nph_participant_dao = NphParticipantDao()
        consent_subquery = nph_participant_dao.get_consents_subquery()
        enrollment_subquery = nph_participant_dao.get_enrollment_subquery()
        diet_status_subquery = nph_participant_dao.get_diet_status_subquery()
        deactivated_subquery = nph_participant_dao.get_deactivated_subquery()
        withdrawal_subquery = nph_participant_dao.get_withdrawal_subquery()

        limit = min(max(limit, MIN_LIMIT), MAX_LIMIT)
        off_set = max(off_set, MIN_OFFSET)

        with nph_participant_dao.session() as session:
            logging.info('root: %s, info: %s, kwargs: %s', root, info, filter_kwargs)
            query = session.query(
                ParticipantSummary,
                Site,
                nphSite,
                ParticipantMapping,
                Participant,
                enrollment_subquery.c.enrollment_status,
                consent_subquery.c.consent_status,
                diet_status_subquery.c.diet_status,
                deactivated_subquery.c.deactivation_status,
                withdrawal_subquery.c.withdrawal_status,
                ParticipantOpsDataElement
            ).outerjoin(
                Site,
                ParticipantSummary.siteId == Site.siteId
            ).join(
                ParticipantMapping,
                ParticipantSummary.participantId == ParticipantMapping.primary_participant_id
            ).join(
                Participant,
                Participant.id == ParticipantMapping.ancillary_participant_id
            ).join(
                consent_subquery,
                consent_subquery.c.consent_pid == Participant.id
            ).outerjoin(
                enrollment_subquery,
                enrollment_subquery.c.enrollment_pid == Participant.id
            ).outerjoin(
                diet_status_subquery,
                diet_status_subquery.c.diet_pid == Participant.id
            ).outerjoin(
                deactivated_subquery,
                deactivated_subquery.c.deactivation_pid == Participant.id
            ).outerjoin(
                withdrawal_subquery,
                withdrawal_subquery.c.withdrawal_pid == Participant.id
            ).outerjoin(
                PairingEvent,
                PairingEvent.participant_id == ParticipantMapping.ancillary_participant_id
            ).outerjoin(
                pm2,
                and_(
                    PairingEvent.participant_id == pm2.participant_id,
                    PairingEvent.event_type_id == pm2.event_type_id,
                    PairingEvent.id < pm2.id
                )
            ).outerjoin(
                nphSite,
                nphSite.id == PairingEvent.site_id
            ).outerjoin(
                ParticipantOpsDataElement,
                and_(
                    ParticipantMapping.ancillary_participant_id == ParticipantOpsDataElement.participant_id,
                    ParticipantOpsDataElement.source_data_element == ParticipantOpsElementTypes.BIRTHDATE,
                    ParticipantOpsDataElement.source_value.isnot(None),
                    ParticipantOpsDataElement.ignore_flag != 1
                )
            ).outerjoin(
                participant_dob,
                and_(
                    ParticipantOpsDataElement.participant_id == participant_dob.participant_id,
                    ParticipantOpsDataElement.source_data_element == ParticipantOpsElementTypes.BIRTHDATE,
                    ParticipantOpsDataElement.source_value.isnot(None),
                    ParticipantOpsDataElement.id < participant_dob.id,
                )
            ).filter(
                pm2.id.is_(None),
                participant_dob.id.is_(None),
                ParticipantMapping.ancillary_study_id == NPH_STUDY_ID,
            ).distinct()

            current_field_class = ParticipantField
            query_builder = QueryBuilder(query)
            try:
                if sort_by:
                    sort_parts = sort_by.split(':')
                    sort_info = NphParticipantData.schema_field_lookup(sort_parts[0])
                    logging.info('sort by: %s', sort_parts)

                    if len(sort_parts) == 1:
                        sort_field: SortableField = getattr(current_field_class, sort_info.get("field"))
                        sort_field.sort_modifier(query_builder)
                    else:
                        sort_parts[0] = sort_info.get("field")
                        for sort_field_name in sort_parts:
                            sort_field: SortableField = getattr(current_field_class, sort_field_name)
                            sort_field.sort(current_field_class, sort_info, sort_field_name, query_builder)
                            current_field_class = sort_field.type

                for field_name, value in filter_kwargs.items():
                    field_def = getattr(ParticipantField, field_name, None)
                    if not field_def:
                        raise NotImplementedError(f'Unable to filter by {field_name}.')
                    if not field_def.filter_modifier:
                        raise NotImplementedError(f'Filtering by {field_name} is not yet implemented.')
                    field_def.filter_modifier(query_builder, value)

                if not nph_id:
                    query = query_builder.get_resulting_query()
                    query = query.limit(limit).offset(off_set)
                    info.context['participant_query'] = query

                    logging.info(query)
                    return NphParticipantData.load_nph_participant_data(query, NPH_BIOBANK_PREFIX)

                logging.info('Fetch NPH ID: %d', nph_id)
                query = query.filter(ParticipantMapping.ancillary_participant_id == int(nph_id))
                info.context['participant_query'] = query
                logging.info(query)
                return NphParticipantData.load_nph_participant_data(query, NPH_BIOBANK_PREFIX)

            except Exception as ex:
                logging.error(ex)
                raise ex


NPHParticipantSchema = Schema(query=ParticipantQuery)
