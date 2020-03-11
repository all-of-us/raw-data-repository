import logging
import json
from rdr_service.lib_fhir.fhirclient_1_0_6.models import fhirdate
from rdr_service.lib_fhir.fhirclient_1_0_6.models.backboneelement import BackboneElement
from rdr_service.lib_fhir.fhirclient_1_0_6.models.domainresource import DomainResource
from rdr_service.lib_fhir.fhirclient_1_0_6.models.fhirdate import FHIRDate
from rdr_service.lib_fhir.fhirclient_1_0_6.models.identifier import Identifier
from sqlalchemy import or_
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest, Conflict, PreconditionFailed, ServiceUnavailable
from rdr_service.api.mayolink_api import MayoLinkApi
from rdr_service import clock
from rdr_service.api_util import get_site_id_by_site_value as get_site, format_json_code
from rdr_service.app_util import get_account_origin_id
from rdr_service.code_constants import BIOBANK_TESTS_SET, HEALTHPRO_USERNAME_SYSTEM, SITE_ID_SYSTEM, \
    QUEST_SITE_ID_SYSTEM, QUEST_BIOBANK_ORDER_ORIGIN, KIT_ID_SYSTEM, QUEST_USERNAME_SYSTEM
from rdr_service.dao.base_dao import FhirMixin, FhirProperty, UpdatableDao
from rdr_service.dao.participant_dao import ParticipantDao, raise_if_withdrawn
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.model.biobank_order import (
    BiobankOrder,
    BiobankOrderHistory,
    BiobankOrderIdentifier,
    BiobankOrderIdentifierHistory,
    BiobankOrderedSample,
    BiobankOrderedSampleHistory,
    MayolinkCreateOrderHistory
)
from rdr_service.model.log_position import LogPosition
from rdr_service.model.participant import Participant
from rdr_service.model.utils import to_client_participant_id
from rdr_service.participant_enums import BiobankOrderStatus, OrderStatus
from rdr_service.model.config_utils import to_client_biobank_id


def _ToFhirDate(dt):
    if not dt:
        return None
    return FHIRDate.with_json(dt.isoformat())


class _FhirBiobankOrderNotes(FhirMixin, BackboneElement):
    """Notes sub-element."""

    resource_name = "BiobankOrderNotes"
    _PROPERTIES = [FhirProperty("collected", str), FhirProperty("processed", str), FhirProperty("finalized", str)]


class _FhirBiobankOrderedSample(FhirMixin, BackboneElement):
    """Sample sub-element."""

    resource_name = "BiobankOrderedSample"
    _PROPERTIES = [
        FhirProperty("test", str, required=True),
        FhirProperty("description", str, required=True),
        FhirProperty("processing_required", bool, required=True),
        FhirProperty("collected", fhirdate.FHIRDate),
        FhirProperty("processed", fhirdate.FHIRDate),
        FhirProperty("finalized", fhirdate.FHIRDate),
    ]


class _FhirBiobankOrderHandlingInfo(FhirMixin, BackboneElement):
    """Information about what user and site handled an order."""

    resource_name = "BiobankOrderHandlingInfo"
    _PROPERTIES = [FhirProperty("author", Identifier), FhirProperty("site", Identifier)]


class _FhirBiobankOrder(FhirMixin, DomainResource):
    """FHIR client definition of the expected JSON structure for a BiobankOrder resource."""

    resource_name = "BiobankOrder"
    _PROPERTIES = [
        FhirProperty("subject", str, required=True),
        FhirProperty("identifier", Identifier, is_list=True, required=True),
        FhirProperty("created", fhirdate.FHIRDate, required=True),
        FhirProperty("samples", _FhirBiobankOrderedSample, is_list=True, required=True),
        FhirProperty("notes", _FhirBiobankOrderNotes),
        FhirProperty("created_info", _FhirBiobankOrderHandlingInfo),
        FhirProperty("collected_info", _FhirBiobankOrderHandlingInfo),
        FhirProperty("processed_info", _FhirBiobankOrderHandlingInfo),
        FhirProperty("finalized_info", _FhirBiobankOrderHandlingInfo),
        FhirProperty("cancelledInfo", _FhirBiobankOrderHandlingInfo),
        FhirProperty("restoredInfo", _FhirBiobankOrderHandlingInfo),
        FhirProperty("restoredSiteId", int, required=False),
        FhirProperty("restoredUsername", str, required=False),
        FhirProperty("amendedInfo", _FhirBiobankOrderHandlingInfo),
        FhirProperty("version", int, required=False),
        FhirProperty("status", str, required=False),
        FhirProperty("amendedReason", str, required=False),
        FhirProperty("origin", str, required=False)
    ]


class BiobankOrderDao(UpdatableDao):
    def __init__(self):
        super(BiobankOrderDao, self).__init__(BiobankOrder)

    def get_id(self, obj):
        return obj.biobankOrderId

    def _order_as_dict(self, order):
        result = order.asdict(follow={"identifiers": {}, "samples": {}})
        result["version"] = int(result["version"])
        if result["orderStatus"] is None:
            result["orderStatus"] = BiobankOrderStatus.UNSET
        del result["created"]
        del result["logPositionId"]
        for identifier in result.get("identifiers", []):
            del identifier["biobankOrderId"]
        samples = result.get("samples")
        if samples:
            for sample in samples:
                del sample["biobankOrderId"]
        return result

    def insert_with_session(self, session, obj):
        obj.version = 1
        if obj.logPosition is not None:
            raise BadRequest(f"{self.model_type.__name__}.logPosition must be auto-generated.")
        obj.logPosition = LogPosition()
        if obj.biobankOrderId is None:
            raise BadRequest("Client must supply biobankOrderId.")
        existing_order = self.get_with_children_in_session(session, obj.biobankOrderId)
        if existing_order:
            existing_order_dict = self._order_as_dict(existing_order)
            new_dict = self._order_as_dict(obj)
            if existing_order_dict == new_dict:
                # If an existing matching order exists, just return it without trying to create it again.
                return existing_order
            else:
                raise Conflict(f"Order with ID {obj.biobankOrderId} already exists")
        self._update_participant_summary(session, obj)
        inserted_obj = super(BiobankOrderDao, self).insert_with_session(session, obj)
        if inserted_obj.collectedSiteId is not None:
            ParticipantDao().add_missing_hpo_from_site(
                session, inserted_obj.participantId, inserted_obj.collectedSiteId
            )
        self._update_history(session, obj)
        return inserted_obj

    def _validate_model(self, session, obj):
        if obj.participantId is None:
            raise BadRequest("participantId is required")
        participant_summary = ParticipantSummaryDao().get_with_session(session, obj.participantId)
        if not participant_summary:
            raise BadRequest(f"Can't submit order for participant {obj.participantId} without consent")
        raise_if_withdrawn(participant_summary)
        for sample in obj.samples:
            self._validate_order_sample(sample)
        # TODO(mwf) FHIR validation for identifiers?
        # Verify that no identifier is in use by another order.
        for identifier in obj.identifiers:
            for existing in (
                session.query(BiobankOrderIdentifier)
                .filter_by(system=identifier.system)
                .filter_by(value=identifier.value)
                .filter(BiobankOrderIdentifier.biobankOrderId != obj.biobankOrderId)
            ):
                raise BadRequest(f"Identifier {identifier} is already in use by order {existing.biobankOrderId}")

    def _validate_order_sample(self, sample):
        # TODO(mwf) Make use of FHIR validation?
        if sample.test not in BIOBANK_TESTS_SET:
            raise BadRequest(f"Invalid test value {sample.test} not in {BIOBANK_TESTS_SET}.")

    def get_with_session(self, session, obj_id, **kwargs):
        result = super(BiobankOrderDao, self).get_with_session(session, obj_id, **kwargs)
        if result:
            ParticipantDao().validate_participant_reference(session, result)
        return result

    def get_with_children_in_session(self, session, obj_id, for_update=False):
        query = session.query(BiobankOrder).options(
            subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples)
        )

        if for_update:
            query = query.with_for_update()

        existing_obj = query.get(obj_id)
        return existing_obj

    def get_with_children(self, obj_id):
        with self.session() as session:
            return self.get_with_children_in_session(session, obj_id)

    def get_biobank_orders_for_participant(self, pid):
        """Retrieves all ordered samples for a participant."""
        with self.session() as session:
            return session.query(BiobankOrder).filter(BiobankOrder.participantId == pid).all()

    def get_biobank_orders_with_children_for_participant(self, pid):
        """Retrieves all ordered with children for a participant."""
        if pid is None:
            raise BadRequest("invalid participant id")
        with self.session() as session:
            return session.query(BiobankOrder).\
                options(subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples)).\
                filter(BiobankOrder.participantId == pid).all()

    def get_biobank_order_by_kit_id(self, kit_id):
        if kit_id is None:
            raise BadRequest("invalid kit id")
        with self.session() as session:
            return (session.query(BiobankOrder).
                    join(BiobankOrderIdentifier).
                    options(subqueryload(BiobankOrder.identifiers), subqueryload(BiobankOrder.samples)).
                    filter(BiobankOrder.biobankOrderId == BiobankOrderIdentifier.biobankOrderId,
                           BiobankOrderIdentifier.system == KIT_ID_SYSTEM,
                           BiobankOrderIdentifier.value == kit_id)
                    .all()
                    )
    def get_ordered_samples_for_participant(self, participant_id):
        """Retrieves all ordered samples for a participant."""
        with self.session() as session:
            return (
                session.query(BiobankOrderedSample)
                .join(BiobankOrder)
                .filter(BiobankOrder.participantId == participant_id)
                .all()
            )

    def get_ordered_samples_sample(self, session, percentage, batch_size):
        """
        Retrieves the biobank ID, collected time, and test for a percentage of ordered samples.
        Used in fake data generation.
        """
        return (
            session.query(Participant.biobankId, BiobankOrderedSample.collected, BiobankOrderedSample.test)
            .join(BiobankOrder, Participant.participantId == BiobankOrder.participantId)
            .join(BiobankOrderedSample, BiobankOrder.biobankOrderId == BiobankOrderedSample.biobankOrderId)
            .filter(Participant.biobankId % 100 < percentage * 100)
            .yield_per(batch_size)
        )

    def insert_mayolink_create_order_history(self, mayolink_create_order_history):
        with self.session() as session:
            self.insert_mayolink_create_order_history_with_session(session, mayolink_create_order_history)

    def insert_mayolink_create_order_history_with_session(self, session, mayolink_create_order_history):
        session.add(mayolink_create_order_history)

    def _get_order_status_and_time(self, sample, order):
        if sample.finalized:
            return (OrderStatus.FINALIZED, sample.finalized)
        if sample.processed:
            return (OrderStatus.PROCESSED, sample.processed)
        if sample.collected:
            return (OrderStatus.COLLECTED, sample.collected)
        return (OrderStatus.CREATED, order.created)

    def _update_participant_summary(self, session, obj):
        """ called on insert"""
        participant_summary_dao = ParticipantSummaryDao()
        participant_summary = participant_summary_dao.get_for_update(session, obj.participantId)
        if not participant_summary:
            raise BadRequest(f"Can't submit biospecimens for participant {obj.participantId} without consent")
        raise_if_withdrawn(participant_summary)
        self._set_participant_summary_fields(obj, participant_summary)
        participant_summary_dao.update_enrollment_status(participant_summary)

        finalized_time = self.get_random_sample_finalized_time(obj)
        is_distinct_visit = ParticipantSummaryDao().calculate_distinct_visits(
            participant_summary.participantId, finalized_time, obj.biobankOrderId
        )

        if is_distinct_visit:
            participant_summary.numberDistinctVisits += 1

    def get_random_sample_finalized_time(self, obj):
        """all samples are set to same finalized time in an order, we only need one."""
        for sample in obj.samples:
            if sample.finalized is not None:
                return sample.finalized

    def _set_participant_summary_fields(self, obj, participant_summary):
        participant_summary.biospecimenStatus = OrderStatus.FINALIZED
        participant_summary.biospecimenOrderTime = obj.created
        if not hasattr(obj, "barcode"):  # barcode means a DV order, they have no siteId's
            participant_summary.biospecimenSourceSiteId = obj.sourceSiteId
            participant_summary.biospecimenCollectedSiteId = obj.collectedSiteId
            participant_summary.biospecimenProcessedSiteId = obj.processedSiteId
            participant_summary.biospecimenFinalizedSiteId = obj.finalizedSiteId

        participant_summary.lastModified = clock.CLOCK.now()

        for sample in obj.samples:
            status_field = "sampleOrderStatus" + sample.test
            status, time = self._get_order_status_and_time(sample, obj)
            setattr(participant_summary, status_field, status)
            setattr(participant_summary, status_field + "Time", time)

    def _get_non_cancelled_biobank_orders(self, session, participantId):
        # look up latest order without cancelled status
        return (
            session.query(BiobankOrder)
            .filter(BiobankOrder.participantId == participantId)
            .filter(or_(BiobankOrder.orderStatus != BiobankOrderStatus.CANCELLED, BiobankOrder.orderStatus == None))
            .order_by(BiobankOrder.created)
            .all()
        )

    def _refresh_participant_summary(self, session, obj):
        # called when cancelled/restored/amended
        participant_summary_dao = ParticipantSummaryDao()
        participant_summary = participant_summary_dao.get_for_update(session, obj.participantId)
        non_cancelled_orders = self._get_non_cancelled_biobank_orders(session, obj.participantId)
        participant_summary.biospecimenStatus = OrderStatus.UNSET
        participant_summary.biospecimenOrderTime = None
        participant_summary.biospecimenSourceSiteId = None
        participant_summary.biospecimenCollectedSiteId = None
        participant_summary.biospecimenProcessedSiteId = None
        participant_summary.biospecimenFinalizedSiteId = None

        amendment = False
        if obj.orderStatus == BiobankOrderStatus.AMENDED:
            amendment = True
        finalized_time = self.get_random_sample_finalized_time(obj)
        is_distinct_visit = ParticipantSummaryDao().calculate_distinct_visits(
            participant_summary.participantId, finalized_time, obj.biobankOrderId, amendment
        )

        if is_distinct_visit and obj.orderStatus != BiobankOrderStatus.CANCELLED:
            participant_summary.numberDistinctVisits += 1

        if (
            obj.orderStatus == BiobankOrderStatus.CANCELLED
            and participant_summary.numberDistinctVisits > 0
            and is_distinct_visit
        ):
            participant_summary.numberDistinctVisits -= 1

        participant_summary.lastModified = clock.CLOCK.now()
        for sample in obj.samples:
            status_field = "sampleOrderStatus" + sample.test
            setattr(participant_summary, status_field, OrderStatus.UNSET)
            setattr(participant_summary, status_field + "Time", None)

        if len(non_cancelled_orders) > 0:
            for order in non_cancelled_orders:
                self._set_participant_summary_fields(order, participant_summary)
        participant_summary_dao.update_enrollment_status(participant_summary)

    def _parse_handling_info(self, handling_info):
        site_id = None
        username = None
        if handling_info.site:
            if handling_info.site.system == QUEST_SITE_ID_SYSTEM:
                site_id = None
                # TODO - check with CE for what site value will they use in the payload
            elif handling_info.site.system == SITE_ID_SYSTEM:
                site = SiteDao().get_by_google_group(handling_info.site.value)
                if not site:
                    raise BadRequest(f"Unrecognized site: {handling_info.site.value}")
                site_id = site.siteId
            else:
                raise BadRequest(f"Invalid site system: {handling_info.site.system}")

        if handling_info.author:
            if handling_info.author.system in [QUEST_USERNAME_SYSTEM, HEALTHPRO_USERNAME_SYSTEM]:
                username = handling_info.author.value
            else:
                raise BadRequest(f"Invalid author system: {handling_info.author.system}")
        return username, site_id

    def _to_handling_info(self, username, site_id):
        if not username and not site_id:
            return None
        info = _FhirBiobankOrderHandlingInfo()
        if site_id:
            site = SiteDao().get(site_id)
            info.site = Identifier()
            info.site.system = SITE_ID_SYSTEM
            info.site.value = site.googleGroup
        if username:
            info.author = Identifier()
            info.author.system = HEALTHPRO_USERNAME_SYSTEM
            info.author.value = username
        return info

    # pylint: disable=unused-argument
    def from_client_json(self, resource_json, id_=None, expected_version=None, participant_id=None, client_id=None):
        resource = _FhirBiobankOrder(resource_json)
        if not resource.created.date:  # FHIR warns but does not error on bad date values.
            raise BadRequest(f"Invalid created date {resource.created.origval}.")

        order = BiobankOrder(participantId=participant_id, created=resource.created.date.replace(tzinfo=None))
        order.orderOrigin = get_account_origin_id()

        if not resource.created_info:
            raise BadRequest("Created Info is required, but was missing in request.")
        order.sourceUsername, order.sourceSiteId = self._parse_handling_info(resource.created_info)
        order.collectedUsername, order.collectedSiteId = self._parse_handling_info(resource.collected_info)
        if order.collectedSiteId is None and order.orderOrigin != QUEST_BIOBANK_ORDER_ORIGIN:
            raise BadRequest("Collected site is required in request.")
        order.processedUsername, order.processedSiteId = self._parse_handling_info(resource.processed_info)
        order.finalizedUsername, order.finalizedSiteId = self._parse_handling_info(resource.finalized_info)

        if resource.notes:
            order.collectedNote = resource.notes.collected
            order.processedNote = resource.notes.processed
            order.finalizedNote = resource.notes.finalized
        if resource.subject != self._participant_id_to_subject(participant_id):
            raise BadRequest(
                f"Participant ID {participant_id} from path and {resource.subject} \
                in request do not match, should be {self._participant_id_to_subject(participant_id)}."
            )

        biobank_order_id = None
        if order.orderOrigin == QUEST_BIOBANK_ORDER_ORIGIN:
            biobank_order_id = self._make_mayolink_order(participant_id, resource)

        self._add_identifiers_and_main_id(order, resource, biobank_order_id)
        self._add_samples(order, resource)

        # order.finalizedTime uses the time from biobank_ordered_sample.finalized
        try:
            order.finalizedTime = self.get_random_sample_finalized_time(resource).date.replace(tzinfo=None)
        except AttributeError:
            order.finalizedTime = None

        if resource.amendedReason:
            order.amendedReason = resource.amendedReason
        if resource.amendedInfo:
            order.amendedUsername, order.amendedSiteId = self._parse_handling_info(resource.amendedInfo)
        order.version = expected_version
        return order

    def _make_mayolink_order(self, participant_id, resource):
        mayo = MayoLinkApi()
        summary = ParticipantSummaryDao().get(participant_id)
        if not summary:
            raise BadRequest("No summary for participant id: {}".format(participant_id))
        code_dict = summary.asdict()
        code_dao = CodeDao()
        format_json_code(code_dict, code_dao, "genderIdentityId")
        format_json_code(code_dict, code_dao, "stateId")
        if "genderIdentity" in code_dict and code_dict["genderIdentity"]:
            if code_dict["genderIdentity"] == "GenderIdentity_Woman":
                gender_val = "F"
            elif code_dict["genderIdentity"] == "GenderIdentity_Man":
                gender_val = "M"
            else:
                gender_val = "U"
        else:
            gender_val = "U"
        if not resource.samples:
            raise BadRequest("No sample found in the payload")
        collected_time = resource.samples[0].collected.date.replace(tzinfo=None)

        kit_id = None
        for item in resource.identifier:
            if item.system == KIT_ID_SYSTEM:
                kit_id = item.value

        order = {
            "order": {
                "collected": str(collected_time),
                "account": "",
                "number": kit_id,
                "patient": {
                    "medical_record_number": str(to_client_biobank_id(summary.biobankId)),
                    "first_name": "*",
                    "last_name": str(to_client_biobank_id(summary.biobankId)),
                    "middle_name": "",
                    "birth_date": "3/3/1933",
                    "gender": gender_val,
                    "address1": summary.streetAddress,
                    "address2": summary.streetAddress2,
                    "city": summary.city,
                    "state": code_dict["state"],
                    "postal_code": str(summary.zipCode),
                    "phone": str(summary.phoneNumber),
                    "account_number": None,
                    "race": str(summary.race),
                    "ethnic_group": None,
                },
                "physician": {"name": "None", "phone": None, "npi": None},
                "report_notes": "",
                "tests": [],
                "comments": "",
            }
        }
        test_codes = []
        for sample in resource.samples:
            sample_dict = {"test": {"code": sample.test, "name": sample.description, "comments": None}}
            order['order']['tests'].append(sample_dict)
            test_codes.append(sample.test)
        response = mayo.post(order)
        try:
            biobank_order_id = response["orders"]["order"]["number"]
            mayo_order_status = response["orders"]["order"]["status"]
        except KeyError:
            raise ServiceUnavailable("Failed to get biobank order id from MayoLink API")

        mayolink_create_order_history = MayolinkCreateOrderHistory()
        mayolink_create_order_history.requestParticipantId = participant_id
        mayolink_create_order_history.requestTestCode = ','.join(test_codes)
        mayolink_create_order_history.requestOrderId = biobank_order_id
        mayolink_create_order_history.requestOrderStatus = mayo_order_status
        try:
            mayolink_create_order_history.requestPayload = json.dumps(order)
            mayolink_create_order_history.responsePayload = json.dumps(response)
        except TypeError:
            logging.info(f"TypeError when create mayolink_create_order_history")
        self.insert_mayolink_create_order_history(mayolink_create_order_history)
        return biobank_order_id

    @classmethod
    def _add_identifiers_and_main_id(cls, order, resource, biobank_order_id):
        found_main_id = False
        for i in resource.identifier:
            order.identifiers.append(BiobankOrderIdentifier(system=i.system, value=i.value))
            if i.system == BiobankOrder._MAIN_ID_SYSTEM:
                order.biobankOrderId = i.value
                found_main_id = True
        if not found_main_id and biobank_order_id:
            order.biobankOrderId = biobank_order_id
        elif not found_main_id and biobank_order_id is None:
            raise BadRequest(f"No identifier for system {BiobankOrder._MAIN_ID_SYSTEM}, required for primary key.")

    @classmethod
    def _add_samples(cls, order, resource):
        all_tests = sorted([s.test for s in resource.samples])
        if len(set(all_tests)) != len(all_tests):
            raise BadRequest(f"Duplicate test in sample list for order: {all_tests}.")
        for s in resource.samples:
            order.samples.append(
                BiobankOrderedSample(
                    biobankOrderId=order.biobankOrderId,
                    test=s.test,
                    description=s.description,
                    processingRequired=s.processing_required,
                    collected=s.collected and s.collected.date.replace(tzinfo=None),
                    processed=s.processed and s.processed.date.replace(tzinfo=None),
                    finalized=s.finalized and s.finalized.date.replace(tzinfo=None),
                )
            )

    @classmethod
    def _participant_id_to_subject(cls, participant_id):
        return "Patient/%s" % to_client_participant_id(participant_id)

    @classmethod
    def _add_samples_to_resource(cls, resource, model):
        resource.samples = []
        for sample in model.samples:
            client_sample = _FhirBiobankOrderedSample()
            client_sample.test = sample.test
            client_sample.description = sample.description
            client_sample.processing_required = sample.processingRequired
            client_sample.collected = _ToFhirDate(sample.collected)
            client_sample.processed = _ToFhirDate(sample.processed)
            client_sample.finalized = _ToFhirDate(sample.finalized)
            resource.samples.append(client_sample)

    @classmethod
    def _add_identifiers_to_resource(cls, resource, model):
        resource.identifier = []
        for identifier in model.identifiers:
            fhir_id = Identifier()
            fhir_id.system = identifier.system
            fhir_id.value = identifier.value
            resource.identifier.append(fhir_id)

    def to_client_json(self, model):
        resource = _FhirBiobankOrder()
        resource.subject = self._participant_id_to_subject(model.participantId)
        resource.created = _ToFhirDate(model.created)
        resource.notes = _FhirBiobankOrderNotes()
        resource.notes.collected = model.collectedNote
        resource.notes.processed = model.processedNote
        resource.notes.finalized = model.finalizedNote
        resource.source_site = Identifier()
        resource.created_info = self._to_handling_info(model.sourceUsername, model.sourceSiteId)
        resource.collected_info = self._to_handling_info(model.collectedUsername, model.collectedSiteId)
        resource.processed_info = self._to_handling_info(model.processedUsername, model.processedSiteId)
        resource.finalized_info = self._to_handling_info(model.finalizedUsername, model.finalizedSiteId)
        resource.amendedReason = model.amendedReason
        resource.origin = model.orderOrigin

        restored = getattr(model, "restoredSiteId")
        if model.orderStatus == BiobankOrderStatus.CANCELLED:
            resource.status = str(BiobankOrderStatus.CANCELLED)
            resource.cancelledInfo = self._to_handling_info(model.cancelledUsername, model.cancelledSiteId)

        elif restored:
            resource.status = str(BiobankOrderStatus.UNSET)
            resource.restoredInfo = self._to_handling_info(model.restoredUsername, model.restoredSiteId)

        elif model.orderStatus == BiobankOrderStatus.AMENDED:
            resource.status = str(BiobankOrderStatus.AMENDED)
            resource.amendedInfo = self._to_handling_info(model.amendedUsername, model.amendedSiteId)

        self._add_identifiers_to_resource(resource, model)
        self._add_samples_to_resource(resource, model)
        client_json = resource.as_json()  # also validates required fields
        client_json["id"] = model.biobankOrderId
        del client_json["resourceType"]
        return client_json

    def _do_update(self, session, order, existing_obj):
        order.lastModified = clock.CLOCK.now()
        order.biobankOrderId = existing_obj.biobankOrderId
        order.orderStatus = BiobankOrderStatus.AMENDED
        if hasattr(existing_obj, "amendedInfo") and existing_obj.amendedInfo.get("author") is not None:
            order.amendedUsername = existing_obj.amendedInfo.get("author").get("value")
        if hasattr(existing_obj, "amendedInfo"):
            order.amendedSiteId = get_site(existing_obj.amendedInfo)
        order.amendedTime = clock.CLOCK.now()
        order.logPosition = LogPosition()
        order.version += 1
        # Ensure that if an order was previously cancelled/restored those columns are removed.
        self._clear_cancelled_and_restored_fields(order)

        super(BiobankOrderDao, self)._do_update(session, order, existing_obj)
        session.add(order.logPosition)

        self._refresh_participant_summary(session, order)
        self._update_history(session, order)

    def update_with_patch(self, id_, resource, expected_version):
        """creates an atomic patch request on an object. It will fail if the object
    doesn't exist already, or if obj.version does not match the version of the existing object.
    May modify the passed in object."""
        with self.session() as session:
            obj = self.get_with_children_in_session(session, id_, for_update=True)
            return self._do_update_with_patch(session, obj, resource, expected_version)

    def _do_update_with_patch(self, session, order, resource, expected_version):
        self._validate_patch_update(order, resource, expected_version)
        order.lastModified = clock.CLOCK.now()
        order.logPosition = LogPosition()
        order.version += 1
        if resource["status"].lower() == "cancelled":
            order.amendedReason = resource["amendedReason"]
            order.cancelledUsername = resource["cancelledInfo"]["author"]["value"]
            order.cancelledSiteId = get_site(resource["cancelledInfo"])
            order.cancelledTime = clock.CLOCK.now()
            order.orderStatus = BiobankOrderStatus.CANCELLED
        elif resource["status"].lower() == "restored":
            order.amendedReason = resource["amendedReason"]
            order.restoredUsername = resource["restoredInfo"]["author"]["value"]
            order.restoredSiteId = get_site(resource["restoredInfo"])
            order.restoredTime = clock.CLOCK.now()
            order.orderStatus = BiobankOrderStatus.UNSET
        else:
            raise BadRequest("status must be restored or cancelled for patch request.")

        super(BiobankOrderDao, self)._do_update(session, order, resource)
        self._update_history(session, order)
        self._refresh_participant_summary(session, order)
        return order

    def _validate_patch_update(self, model, resource, expected_version):
        if expected_version != model.version:
            raise PreconditionFailed(
                f"Expected version was {expected_version}; stored version was {model.version}"
            )
        required_cancelled_fields = ["amendedReason", "cancelledInfo", "status"]
        required_restored_fields = ["amendedReason", "restoredInfo", "status"]
        if "status" not in resource:
            raise BadRequest("status of cancelled/restored is required")

        if resource["status"] == "cancelled":
            if model.orderStatus == BiobankOrderStatus.CANCELLED:
                raise BadRequest("Can not cancel an order that is already cancelled.")
            for field in required_cancelled_fields:
                if field not in resource:
                    raise BadRequest(f"{field} is required for a cancelled biobank order")
            if "site" not in resource["cancelledInfo"] or "author" not in resource["cancelledInfo"]:
                raise BadRequest("author and site are required for cancelledInfo")

        elif resource["status"] == "restored":
            if model.orderStatus != BiobankOrderStatus.CANCELLED:
                raise BadRequest("Can not restore an order that is not cancelled.")
            for field in required_restored_fields:
                if field not in resource:
                    raise BadRequest(f"{field} is required for a restored biobank order")
            if "site" not in resource["restoredInfo"] or "author" not in resource["restoredInfo"]:
                raise BadRequest("author and site are required for restoredInfo")

    def _update_history(self, session, order):
        # Increment the version and add a new history entry.
        session.flush()
        history = BiobankOrderHistory()
        history.fromdict(order.asdict(follow=["logPosition"]), allow_pk=True)
        history.logPositionId = order.logPosition.logPositionId
        session.add(history)
        self._update_identifier_history(session, order)
        self._update_sample_history(session, order)

    @staticmethod
    def _update_identifier_history(session, order):
        session.flush()
        for identifier in order.identifiers:
            history = BiobankOrderIdentifierHistory()
            history.fromdict(identifier.asdict(), allow_pk=True)
            history.version = order.version
            history.biobankOrderId = order.biobankOrderId
            session.add(history)

    @staticmethod
    def _update_sample_history(session, order):
        session.flush()
        for sample in order.samples:
            history = BiobankOrderedSampleHistory()
            history.fromdict(sample.asdict(), allow_pk=True)
            history.version = order.version
            history.biobankOrderId = order.biobankOrderId
            session.add(history)

    @staticmethod
    def _clear_cancelled_and_restored_fields(order):
        # pylint: disable=unused-argument
        """ Just in case these fields have values, we don't want them in the most recent record for an
    amendment, they will exist in history tables."""
        order.restoredUsername = None
        order.restoredTime = None
        order.cancelledUsername = None
        order.cancelledTime = None
        order.restoredSiteId = None
        order.cancelledSiteId = None
        order.status = BiobankOrderStatus.UNSET
