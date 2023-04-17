import logging
from datetime import datetime
from typing import Tuple, Dict, List, Any, Optional, Iterator, Iterable
import json
from types import SimpleNamespace as Namespace
from typing import Optional
from protorpc import messages
from werkzeug.exceptions import BadRequest, NotFound

from sqlalchemy.orm import Query, aliased
from sqlalchemy import exc

from rdr_service.model.study_nph import (
    StudyCategory, Participant, Site, Order, OrderedSample,
    Activity, ParticipantEventActivity, EnrollmentEventType,
    PairingEventType, PairingEvent, ConsentEventType,
    SampleUpdate, BiobankFileExport, SampleExport,
    StoredSample, EnrollmentEvent, Incident
)
from rdr_service.dao.base_dao import BaseDao, UpdatableDao
from rdr_service.config import NPH_MIN_BIOBANK_ID, NPH_MAX_BIOBANK_ID


_logger = logging.getLogger("rdr_logger")


def _format_timestamp(timestamp: datetime) -> Optional[str]:
    return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ') if timestamp else None


class OrderStatus(messages.Enum):
    """A status reflecting the NPH order of the participant"""

    RESTORED = 1
    CANCELED = 2


class NphParticipantDao(BaseDao):
    def __init__(self):
        super(NphParticipantDao, self).__init__(Participant)

    @staticmethod
    def fetch_participant_id(obj) -> int:
        return obj.id

    def get_id(self, session, nph_participant_id: str) -> int:
        nph_participant_id = self.convert_id(nph_participant_id)
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == int(nph_participant_id)).first()
        if result:
            return result.id
        else:
            raise NotFound(f"Participant ID not found : {nph_participant_id}")

    def get_participant(self, nph_participant_id: str, session) -> Participant:
        nph_participant_id = self.convert_id(nph_participant_id)
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == int(nph_participant_id)).first()
        if result:
            return result
        else:
            raise NotFound(f"Participant not found : {nph_participant_id}")

    @classmethod
    def check_participant_exist(cls, nph_participant_id: str, session=None) -> bool:
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == int(nph_participant_id)).first()
        if result:
            return True
        else:
            return False

    @staticmethod
    def convert_id(nph_participant_id: str) -> str:
        return nph_participant_id[4:]

    def insert_participant_with_random_biobank_id(self, obj):
        return self._insert_with_random_id(
            obj,
            ['biobank_id'],
            min_id=NPH_MIN_BIOBANK_ID,
            max_id=NPH_MAX_BIOBANK_ID
        )

    def from_client_json(self):
        pass


class NphStudyCategoryDao(UpdatableDao):
    def __init__(self):
        super(NphStudyCategoryDao, self).__init__(StudyCategory)

    def from_client_json(self):
        pass

    def get_id(self, session, order: Namespace) -> Tuple[bool, str]:
        # Compare the module, vistType and time point using self join
        # return False and empty string if module not exist
        # otherwise, return True and time point id
        module = aliased(StudyCategory)
        visit_type = aliased(StudyCategory)
        time_point = aliased(StudyCategory)
        query = Query(time_point)
        query.session = session
        result = query.filter(module.id == visit_type.parent_id, visit_type.id == time_point.parent_id,
                              module.name == order.module, visit_type.name == order.visitType,
                              time_point.name == order.timepoint).first()
        if not result:
            return False, ""
        else:
            return True, result.id

    @staticmethod
    def get_study_category_sample(category_id, session) -> Tuple[StudyCategory, StudyCategory, StudyCategory]:
        # Fetching study category values from the db table
        time_point_record, visit_type_record, module_record = None, None, None
        query = Query(StudyCategory)
        query.session = session
        time_point_record = query.filter(StudyCategory.id == category_id).first()
        if time_point_record:
            visit_type_record = query.filter(StudyCategory.id == time_point_record.parent_id,
                                             StudyCategory.type_label == "visitType").first()
            if visit_type_record:
                module_record = query.filter(StudyCategory.id == visit_type_record.parent_id,
                                             StudyCategory.type_label == "module").first()
        return time_point_record, visit_type_record, module_record

    @staticmethod
    def validate_model(obj):
        if obj.__dict__.get("module") is None:
            raise BadRequest("Module is missing")
        if obj.__dict__.get("visitType") is None:
            raise BadRequest("Visit Type is missing")
        if obj.__dict__.get("timepoint") is None:
            raise BadRequest("Time Point ID is missing")

    @staticmethod
    def module_exist(order: Namespace, session) -> Tuple[bool, Optional[StudyCategory]]:

        query = Query(StudyCategory)
        query.session = session
        result = query.filter(StudyCategory.type_label == "module", StudyCategory.name == order.module).first()
        if result:
            return True, result
        else:
            return False, None

    @staticmethod
    def visit_type_exist(order: Namespace, module: StudyCategory, session) -> Tuple[bool, Optional[StudyCategory]]:

        query = Query(StudyCategory)
        query.session = session
        if module:
            result = query.filter(StudyCategory.type_label == "visitType", StudyCategory.name == order.visitType,
                                  StudyCategory.parent_id == module.id).first()
            if result:
                return True, result

        return False, None

    @staticmethod
    def timepoint_exist(order: Namespace, visit_type: StudyCategory, session) -> Tuple[bool, Optional[StudyCategory]]:
        query = Query(StudyCategory)
        query.session = session
        if visit_type:
            result = query.filter(
                StudyCategory.type_label == "timepoint",
                StudyCategory.name == order.timepoint,
                StudyCategory.parent_id == visit_type.id
            ).first()
            if result:
                return True, result
        return False, None

    def get_study_category(self, study_category_id: int) -> StudyCategory:
        with self.session() as session:
            return session.query(StudyCategory).get(study_category_id)

    def get_parent_study_category(self, study_category_id: int) -> StudyCategory:
        with self.session() as session:
            study_category: StudyCategory = session.query(StudyCategory).get(study_category_id)
            parent_study_category = study_category.parent
            return parent_study_category


class NphSiteDao(BaseDao):
    def __init__(self):
        super(NphSiteDao, self).__init__(Site)

    def get_site_id_from_external(self, external_id):
        with self.session() as session:
            return session.query(
                Site
            ).filter(
                Site.external_id == external_id
            ).first()

    @staticmethod
    def _fetch_site_id(session, external_id) -> int:
        query = Query(Site)
        query.session = session
        result = query.filter(Site.external_id == external_id).first()
        if result is None:
            raise NotFound(f"Site is not found -- {external_id}")
        return result.id

    def get_id(self, session, site_name: str) -> int:
        try:
            return self._fetch_site_id(session, site_name)
        except NotFound:
            raise

    @staticmethod
    def site_exist(session, external_id: str) -> bool:
        query = Query(Site)
        query.session = session
        result = query.filter(Site.external_id == external_id).first()
        if result is None:
            return False
        return True

    def get_site_from_external_id(self, external_id):
        with self.session() as session:
            return session.query(Site).filter(Site.external_id == external_id).one_or_none()

    def from_client_json(self):
        pass

    def get_site_using_params(
        self,
        external_id: str,
        awardee_external_id: str,
        organization_external_id: str,
        name: str
    ) -> Optional[Site]:
        with self.session() as session:
            return session.query(Site).filter(
                Site.name == name,
                Site.external_id == external_id,
                Site.awardee_external_id == awardee_external_id,
                Site.organization_external_id == organization_external_id,
            ).one_or_none()


class NphOrderDao(UpdatableDao):
    def __init__(self):
        super(NphOrderDao, self).__init__(Order)
        self.study_category_dao = NphStudyCategoryDao()
        self.order_sample_dao = NphOrderedSampleDao()
        self.site_dao = NphSiteDao()
        self.participant_dao = NphParticipantDao()
        self.order_cls = None

    def get_id(self, obj: Order):
        return obj.id

    def validate(self, order_id: int, nph_participant_id: str, session):
        participant_exist = self.participant_dao.check_participant_exist(nph_participant_id, session)
        order_exist, order = self.check_order_exist(order_id, session)
        create_site_exist = self.site_dao.site_exist(session, self.order_cls.createdInfo.site.value)
        collected_site_exist = self.site_dao.site_exist(session, self.order_cls.collectedInfo.site.value)
        finalized_site_exist = self.site_dao.site_exist(session, self.order_cls.finalizedInfo.site.value)
        if participant_exist is not True:
            raise BadRequest(f"Participant ID does not exist: {nph_participant_id}")
        if order_exist is not True:
            raise BadRequest(f"Order ID does not exist: {order_id}")
        if create_site_exist is not True:
            raise BadRequest(f"Created Site does not exist: {self.order_cls.createdInfo.site.value}")
        if collected_site_exist is not True:
            raise BadRequest(f"Collected Site does not exist: {self.order_cls.collectedInfo.site.value}")
        if finalized_site_exist is not True:
            raise BadRequest(f"Finalized Site does not exist: {self.order_cls.finalizedInfo.site.value}")

        time_point_record, visit_type_record, module_record = self.study_category_dao\
            .get_study_category_sample(order.category_id, session)
        if time_point_record is None:
            raise BadRequest("TimePoint does not match the corresponding visitType")
        if visit_type_record is None:
            raise BadRequest("VisitType does not match the corresponding module")
        if module_record is None:
            raise BadRequest("Module does not exist")
        payload = self.order_cls

        if payload.module != module_record.name:
            raise BadRequest(f"Module does not exist: {payload.module}")
        if payload.visitType != visit_type_record.name:
            raise BadRequest(f"VisitType does not match the corresponding module: {payload.visitType}")
        if payload.timepoint != time_point_record.name:
            raise BadRequest(f"TimePoint does not match the corresponding visitType: {payload.timepoint}")

    def patch_update(self, order: Namespace, rdr_order_id: int, nph_participant_id: str, session) -> Order:
        if order.status.upper() == "RESTORED":
            site_name = order.restoredInfo.site.value
            amended_author = order.restoredInfo.author.value
        elif order.status.upper() == "CANCELLED":
            site_name = order.cancelledInfo.site.value
            amended_author = order.cancelledInfo.author.value
        elif order.status.upper() == "AMENDED":
            site_name = order.amendedInfo.site.value
            amended_author = order.amendedInfo.author.value
        else:
            raise BadRequest(f"Invalid status value: {order.status}")
        site_id = self.site_dao.get_id(session, site_name)
        amended_reason = getattr(order, 'amendedReason', None)
        db_order = self.get_order(rdr_order_id, session)
        p_id = self.participant_dao.get_id(session, nph_participant_id)
        if db_order.participant_id == p_id:
            db_order.amended_author = amended_author
            db_order.amended_site = site_id
            db_order.amended_reason = amended_reason
            db_order.status = order.status

            self._patch_update_order(order_json=order, db_order=db_order)

            sample_update_dao = NphSampleUpdateDao()
            for ordered_sample in db_order.samples:
                sample_update_dict = {
                    "rdr_ordered_sample_id": ordered_sample.id
                }
                sample_update_dao.insert_with_session(
                    session=session,
                    obj=SampleUpdate(**sample_update_dict)
                )

        else:
            raise BadRequest("Participant ID does not match the corresponding Order ID.")
        return db_order

    @classmethod
    def _update_if_present(cls, target_obj, target_field_name, source_obj, source_field_name=None):
        source_field_name = source_field_name or target_field_name
        if hasattr(source_obj, source_field_name):
            new_value = getattr(source_obj, source_field_name)
            setattr(target_obj, target_field_name, new_value)

    def _patch_update_order(self, order_json, db_order: Order):
        # as of writing this, all nph orders have one, and only one, parent sample
        if not db_order.samples:
            return

        parent_sample: OrderedSample = db_order.samples[0]
        if hasattr(order_json, 'sample'):
            self._update_if_present(parent_sample, 'test', order_json.sample)
            self._update_if_present(parent_sample, 'description', order_json.sample)
            self._update_if_present(parent_sample, 'collected', order_json.sample)
            self._update_if_present(parent_sample, 'finalized', order_json.sample)

        # update aliquot level data, adding aliquots as needed, and setting others as cancelled
        if not hasattr(order_json, 'aliquots'):
            return

        updated_aliquots = set()
        for aliquot_json in order_json.aliquots:
            # find the matching aliquot data we already have based on the id
            matching_db_aliquot: Optional[OrderedSample] = None
            for db_aliquot in parent_sample.children:
                if db_aliquot.aliquot_id == aliquot_json.id:
                    matching_db_aliquot = db_aliquot
                    break

            if matching_db_aliquot:
                self._update_if_present(matching_db_aliquot, 'collected', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'container', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'description', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'finalized', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'identifier', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'status', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'volume', aliquot_json)
                self._update_if_present(matching_db_aliquot, 'volumeUnits', aliquot_json, 'units')
                updated_aliquots.add(matching_db_aliquot)

        # any aliquots in the database not seen in the request should be cancelled
        for db_aliquot in parent_sample.children:
            if db_aliquot not in updated_aliquots:
                db_aliquot.status = 'cancelled'

    def update_order(self, rdr_order_id: int, nph_participant_id: str, session) -> Order:
        create_site = self.site_dao.get_id(session, self.order_cls.createdInfo.site.value)
        collected_site = self.site_dao.get_id(session, self.order_cls.collectedInfo.site.value)
        finalized_site = self.site_dao.get_id(session, self.order_cls.finalizedInfo.site.value)
        db_order = self.get_order(rdr_order_id, session)
        if db_order.participant_id == self.participant_dao.get_id(session, nph_participant_id):
            db_order.nph_order_id = fetch_identifier_value(self.order_cls, "order-id")
            db_order.created_author = self.order_cls.createdInfo.author.value
            db_order.created_site = create_site
            db_order.collected_author = self.order_cls.collectedInfo.author.value
            db_order.collected_site = collected_site
            db_order.finalized_author = self.order_cls.finalizedInfo.author.value
            db_order.finalized_site = finalized_site
            db_order.client_id = fetch_identifier_value(self.order_cls, "client-id")
            db_order.notes = self.order_cls.notes.__dict__
        else:
            raise BadRequest("Participant ID does not match the corresponding Order ID.")
        return db_order

    @staticmethod
    def get_order(order_id: int, session) -> Order:
        query = Query(Order)
        query.session = session
        result = query.filter(Order.id == order_id).first()
        if result:
            return result
        else:
            raise NotFound(f"Order Id does not exist -- {order_id}.")

    @staticmethod
    def check_order_exist(order_id: int, session) -> Tuple[bool, Any]:
        query = Query(Order)
        query.session = session
        result = query.filter(Order.id == order_id).first()
        if result:
            return True, result
        else:
            return False, None

    def get_study_category_id(self, session):
        return self.study_category_dao.get_id(session, self.order_cls)

    def set_order_cls(self, resource_data):
        self.order_cls = json.loads(resource_data, object_hook=lambda d: Namespace(**d))

    def from_client_json(self, session, nph_participant_id, category_id):
        try:
            create_site = self.site_dao.get_id(session, self.order_cls.createdInfo.site.value)
            collected_site = self.site_dao.get_id(session, self.order_cls.collectedInfo.site.value)
            finalized_site = self.site_dao.get_id(session, self.order_cls.finalizedInfo.site.value)
            participant = self.participant_dao.get_participant(nph_participant_id, session)
        except NotFound:
            raise
        if not create_site and not collected_site and not finalized_site:
            raise BadRequest("Site has not been populated in Site Table")
        if not participant:
            raise NotFound(f"Participant not Found: {nph_participant_id}")
        order = Order()
        for order_model_field, resource_value in [("nph_order_id", fetch_identifier_value(self.order_cls, "order-id")),
                                                  ("order_created", self.order_cls.created),
                                                  ("client_id", fetch_identifier_value(self.order_cls, "client-id")),
                                                  ("category_id", category_id),
                                                  ("participant_id", participant.id),
                                                  ("created_author", self.order_cls.createdInfo.author.value),
                                                  ("created_site", create_site),
                                                  ("collected_author", self.order_cls.collectedInfo.author.value),
                                                  ("collected_site", collected_site),
                                                  ("finalized_author", self.order_cls.finalizedInfo.author.value),
                                                  ("finalized_site", finalized_site),
                                                  ("notes", self.order_cls.notes.__dict__)]:

            if resource_value is not None:
                order.__setattr__(order_model_field, resource_value)

        return order

    def _validate_model(self, obj):
        if obj.category_id is None:
            raise BadRequest("Category ID is missing")
        if obj.created_site is None:
            raise BadRequest("Created Site ID is missing")
        if obj.collected_site is None:
            raise BadRequest("Collected Site ID is missing")
        if obj.finalized_site is None:
            raise BadRequest("Finalized Site ID is missing")

    def _get_or_insert_module_visit_type_and_timepoint_study_categories(self, order: Namespace, session):
        module_exist, module = self.study_category_dao.module_exist(order, session)
        visit_exist, visit = self.study_category_dao.visit_type_exist(order, module, session)
        if not module_exist:
            _module = StudyCategory(name=order.module, type_label="module")
            module: StudyCategory = self.study_category_dao.insert(_module)
        if not visit_exist:
            _visit_type: StudyCategory = StudyCategory(name=order.visitType, type_label="visitType")
            visit_type: StudyCategory = self.study_category_dao.insert(_visit_type)
            module.children.append(visit_type)

        timepoint_exist, timepoint = self.study_category_dao.timepoint_exist(order, module, session)
        if not timepoint_exist:
            timepoint_sc = StudyCategory(name=order.timepoint, type_label="timepoint")
            timepoint: StudyCategory = self.study_category_dao.insert(timepoint_sc)
        visit.children.append(timepoint)
        return module, timepoint

    def insert_study_category_with_session(self, order: Namespace, session):
        module, timepoint = self._get_or_insert_module_visit_type_and_timepoint_study_categories(order, session)
        return module, timepoint.id

    def insert_ordered_sample_dao_with_session(self, session, order: Namespace):
        return self.order_sample_dao.insert_with_session(session, order)

    def insert_with_session(self, session, order: Order) -> Order:
        # Adding record(s) to nph.order table
        session.add(order)
        session.commit()
        session.refresh(order)
        return order

    def get_nph_biospecimens_for_participant(self, nph_participant: Participant):
        with self.session() as session:
            orders_for_participant: Iterable[Order] = list(
                session.query(Order).filter(Order.participant_id == nph_participant.id).all()
            )

        biospecimens: Iterable[Dict[str, Any]] = []
        nph_ordered_sample_dao = NphOrderedSampleDao()
        for order in orders_for_participant:
            for biospecimen in nph_ordered_sample_dao.get_biospecimens_for_order(nph_participant, order):
                biospecimens.append(biospecimen)
        return biospecimens


class NphOrderedSampleDao(UpdatableDao):
    def __init__(self):
        super(NphOrderedSampleDao, self).__init__(OrderedSample)

    def get_id(self, obj: OrderedSample):
        return obj.id

    @staticmethod
    def _is_order_cancelled(order: Order) -> bool:
        return order.status == "cancelled"

    @staticmethod
    def _is_ordered_sample_cancelled(ordered_sample: OrderedSample) -> bool:
        return str(ordered_sample.status).lower() == "cancelled"

    def get_biospecimens_for_order(self, nph_participant: Participant, order: Order) -> Iterator[Dict[str, Any]]:
        nph_stored_sample_session = NphStoredSampleDao()
        nph_study_category_dao = NphStudyCategoryDao()
        with self.session() as session:
            ordered_samples_for_participant: Iterable[OrderedSample] = list(
                session.query(OrderedSample).filter(OrderedSample.order_id == order.id).all()
            )
            for ordered_sample in ordered_samples_for_participant:
                parent_study_category = nph_study_category_dao.get_parent_study_category(order.category_id)
                nph_module_id = nph_study_category_dao.get_parent_study_category(parent_study_category.id)
                sample_processing_ts = ordered_sample.collected if not ordered_sample.parent is None else None
                collection_date_utc = _format_timestamp((ordered_sample.parent or ordered_sample).collected)
                processing_date_utc = _format_timestamp(sample_processing_ts)
                finalized_date_utc = _format_timestamp(ordered_sample.finalized) if ordered_sample.finalized else None
                sample_is_cancelled = (
                    self._is_order_cancelled(order) or
                    self._is_ordered_sample_cancelled(ordered_sample)
                )
                kit_id = None
                if (ordered_sample.identifier or ordered_sample.test).startswith("ST"):
                    kit_id = order.nph_order_id

                sample_status = "Cancelled" if sample_is_cancelled else "Active"
                biospecimen_dict = {
                    "orderID": order.nph_order_id,
                    "visitID": parent_study_category.name if parent_study_category else "",
                    "studyID": f"NPH Module {nph_module_id.name}",
                    "specimenCode": (ordered_sample.identifier or ordered_sample.test),
                    "timepointID": nph_study_category_dao.get_study_category(order.category_id).name,
                    "volume": ordered_sample.volume,
                    "volumeUOM": ordered_sample.volumeUnits,
                    "orderedSampleStatus": sample_status,
                    "clientID": order.client_id,
                    "collectionDateUTC": collection_date_utc,
                    "processingDateUTC": processing_date_utc,
                    "finalizedDateUTC": finalized_date_utc,
                    "sampleID": (ordered_sample.aliquot_id or ordered_sample.nph_sample_id),
                    "kitID": kit_id,
                    "biobankStatus": None,
                }
                biobank_status_and_lims_id = (
                    nph_stored_sample_session.get_biobank_status_and_lims_id(
                        nph_participant, ordered_sample
                    )
                )
                if biobank_status_and_lims_id:
                    biospecimen_dict.update({"biobankStatus": biobank_status_and_lims_id})
                yield biospecimen_dict

    @staticmethod
    def _get_parent_order_sample(order_id, session) -> OrderedSample:
        query = Query(OrderedSample)
        query.session = session
        result = query.filter(OrderedSample.order_id == order_id, OrderedSample.parent_sample_id == None).first()
        if result:
            return result
        else:
            raise NotFound("Order sample not found")

    @staticmethod
    def _get_child_order_sample(parent_id, order_id, session) -> List[OrderedSample]:
        try:
            query = Query(OrderedSample)
            query.session = session
            result = query.filter(OrderedSample.order_id == order_id, OrderedSample.parent_sample_id == parent_id).all()
            return result
        except exc.SQLAlchemyError as sql:
            raise sql

    def from_client_json(self, obj: Namespace, order_id: int, nph_sample_id: str) -> OrderedSample:
        return OrderedSample(nph_sample_id=nph_sample_id,
                             order_id=order_id,
                             test=obj.sample.test,
                             description=obj.sample.description,
                             collected=obj.sample.collected,
                             finalized=obj.sample.finalized,
                             supplemental_fields=self._fetch_supplemental_fields(obj)
                             )

    @staticmethod
    def from_aliquot_client_json(aliquot, order_id: int, nph_sample_id: str) -> OrderedSample:
        return OrderedSample(nph_sample_id=nph_sample_id,
                             order_id=order_id,
                             aliquot_id=aliquot.id,
                             description=aliquot.description,
                             identifier=aliquot.identifier,
                             collected=aliquot.collected,
                             container=aliquot.container,
                             volume=aliquot.volume,
                             volumeUnits=aliquot.units
                             )

    @staticmethod
    def _fetch_supplemental_fields(order_cls) -> Dict:
        keys = ["test", "description", "collected", "finalized"]
        result = {k: v for k, v in order_cls.sample.__dict__.items() if k not in keys}
        return result

    def insert_with_session(self, session, order: Namespace) -> Namespace:
        return self._insert_order_sample(session, order)

    def _insert_order_sample(self, session, order: Namespace):
        # Adding record(s) to nph.order_sample table
        ordered_sample_list = []
        nph_sample_id = fetch_identifier_value(order, "sample-id")
        os = self.from_client_json(order, order.id, nph_sample_id)
        ordered_sample_list.append(os)
        if order.__dict__.get("aliquots"):
            for aliquot in order.aliquots:
                oa = self.from_aliquot_client_json(aliquot, order.id, nph_sample_id)
                os.children.append(oa)
                ordered_sample_list.append(oa)
        session.add(os)
        session.commit()
        sample_update_dao = NphSampleUpdateDao()
        for ordered_sample in ordered_sample_list:
            sample_update_dao.insert(
                SampleUpdate(
                    rdr_ordered_sample_id=ordered_sample.id,
                    ordered_sample_json=ordered_sample.asdict()
                )
            )
        return os

    def update_order_sample(self, order: Namespace, rdr_order_id: int, session):
        ordered_sample_list = []
        db_parent_order_sample = self._get_parent_order_sample(rdr_order_id, session)
        self._update_parent_order(order, db_parent_order_sample)
        ordered_sample_list.append(db_parent_order_sample)
        db_child_order_sample = self._get_child_order_sample(db_parent_order_sample.id, rdr_order_id, session)
        if len(db_child_order_sample) > 0:
            co_list = self._update_child_order(order, db_child_order_sample, db_parent_order_sample.nph_sample_id,
                                               rdr_order_id)
            for co in co_list:
                db_parent_order_sample.children.append(co)
                ordered_sample_list.append(co)
        session.add(db_parent_order_sample)
        session.commit()
        sample_update_dao = NphSampleUpdateDao()

        for ordered_sample in ordered_sample_list:
            sample_update_dict = {
                "rdr_ordered_sample_id": ordered_sample.id,
                "ordered_sample_json": ordered_sample.asdict()
            }
            sample_update_dao.insert(SampleUpdate(**sample_update_dict))

    def _update_child_order(self, payload: Namespace, order_sample: List[OrderedSample], nph_sample_id: str,
                            rdr_order_id: int) -> List[OrderedSample]:
        db_child_sample_dict = {co.aliquot_id: co for co in order_sample}
        db_child_sample_keys = [co.aliquot_id for co in order_sample]
        os_list = []
        if payload.__dict__.get("aliquots"):
            payload_sample_keys = [po.id for po in payload.aliquots]
            payload_sample_dict = {po.id: po for po in payload.aliquots}
            os_to_cancel = set(db_child_sample_keys) - set(payload_sample_keys)
            os_to_insert = set(payload_sample_keys) - set(db_child_sample_keys)
            os_to_update = set(payload_sample_keys).intersection(db_child_sample_keys)
            for os_id in os_to_cancel:
                co = self._update_canceled_child_order(db_child_sample_dict.get(os_id))
                os_list.append(co)
            for os_id in os_to_insert:
                co = self.from_aliquot_client_json(payload_sample_dict.get(os_id), rdr_order_id, nph_sample_id)
                os_list.append(co)
            for os_id in os_to_update:
                db_child_sample = db_child_sample_dict.get(os_id)
                co = self._update_restored_child_order(payload_sample_dict.get(os_id),
                                                       db_child_sample, nph_sample_id)
                os_list.append(co)
        else:
            for each in order_sample:
                self._update_canceled_child_order(each)
                os_list.append(each)
        return os_list

    def _update_parent_order(self, obj: Namespace, order_sample: OrderedSample) -> OrderedSample:
        order_sample.nph_sample_id = fetch_identifier_value(obj, "sample-id")
        order_sample.test = obj.sample.test
        order_sample.description = obj.sample.description
        order_sample.collected = obj.sample.collected
        order_sample.finalized = obj.sample.finalized
        order_sample.supplemental_fields = self._fetch_supplemental_fields(obj)
        return order_sample

    @staticmethod
    def _update_restored_child_order(obj: Namespace, order_sample: OrderedSample, nph_sample_id: str) -> OrderedSample:
        incoming_status = getattr(obj, 'status', None)
        if incoming_status is not None and 'cancel' in incoming_status:
            incoming_status = 'cancelled'

        order_sample.nph_sample_id = nph_sample_id
        order_sample.identifier = obj.identifier
        order_sample.container = obj.container
        order_sample.volume = obj.volume
        order_sample.description = obj.description
        order_sample.collected = obj.collected
        order_sample.status = incoming_status or "restored"
        return order_sample

    @staticmethod
    def _update_canceled_child_order(order_sample: OrderedSample) -> OrderedSample:
        order_sample.status = "cancelled"
        return order_sample

    def _validate_model(self, obj):
        if obj.order_id is None:
            raise BadRequest("Order ID is missing")


def fetch_identifier_value(obj: Namespace, identifier: str) -> str:
    for each in obj.identifier:
        if identifier in each.system:
            return each.value


class NphDaoMixin:

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )


class NphActivityDao(BaseDao):
    def __init__(self):
        super(NphActivityDao, self).__init__(Activity)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphParticipantEventActivityDao(BaseDao, NphDaoMixin):
    def __init__(self):
        super(NphParticipantEventActivityDao, self).__init__(ParticipantEventActivity)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_activity_event_intake(self, *, participant_id, resource_identifier, activity_id):
        with self.session() as session:
            return session.query(
                ParticipantEventActivity
            ).filter(
                ParticipantEventActivity.participant_id == participant_id,
                ParticipantEventActivity.resource["bundle_identifier"] == resource_identifier,
                ParticipantEventActivity.activity_id == activity_id
            ).first()


class NphEventTypeMixin(NphDaoMixin):

    def get_event_by_source_name(self, source_name):
        if not hasattr(self.model_type, 'source_name'):
            return []

        with self.session() as session:
            records = session.query(
                self.model_type
            ).filter(
                self.model_type.source_name == source_name
            )
            return records.first()


class NphDefaultBaseDao(BaseDao, NphDaoMixin):
    def __init__(self, model_type):
        super().__init__(model_type)

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id


class NphEnrollmentEventTypeDao(BaseDao, NphEventTypeMixin):
    def __init__(self):
        super().__init__(EnrollmentEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphEnrollmentEventDao(BaseDao):
    def __init__(self):
        super().__init__(EnrollmentEvent)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphPairingEventTypeDao(BaseDao, NphEventTypeMixin):
    def __init__(self):
        super().__init__(PairingEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphPairingEventDao(BaseDao, NphDaoMixin):
    def __init__(self):
        super().__init__(PairingEvent)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_participant_paired_site(self, participant_id):
        with self.session() as session:
            return session.query(PairingEvent).join(
                Site,
                Site.id == PairingEvent.site_id
            ).filter(
                PairingEvent.participant_id == participant_id
            ).order_by(PairingEvent.event_authored_time.desc()).first()


class NphConsentEventTypeDao(BaseDao, NphEventTypeMixin):
    def __init__(self):
        super().__init__(ConsentEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphConsentEventDao(BaseDao, NphDaoMixin):
    def __init__(self):
        super().__init__(ConsentEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphIntakeDao(BaseDao):
    def __init__(self):
        super().__init__(BaseDao)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def to_client_json(self, payload):
        return payload


class NphSampleUpdateDao(BaseDao):
    def __init__(self):
        super(NphSampleUpdateDao, self).__init__(SampleUpdate)

    def get_id(self, obj: SampleUpdate):
        return obj.id


class NphBiobankFileExportDao(BaseDao):
    def __init__(self):
        super(NphBiobankFileExportDao, self).__init__(BiobankFileExport)

    def get_id(self, obj: BiobankFileExport):
        return obj.id


class NphSampleExportDao(BaseDao):
    def __init__(self):
        super(NphSampleExportDao, self).__init__(SampleExport)

    def get_id(self, obj: SampleExport):
        return obj.id


class NphStoredSampleDao(BaseDao):
    def __init__(self):
        super(NphStoredSampleDao, self).__init__(StoredSample)

    def get_id(self, obj: StoredSample):
        return obj.id

    def get_biobank_status_and_lims_id(
        self, nph_participant: Participant, ordered_sample: OrderedSample
    ) -> Iterable[Tuple[str]]:
        participant_biobank_id = nph_participant.biobank_id
        with self.session() as session:
            stored_samples: Iterable[StoredSample] = session.query(StoredSample)\
                .order_by(StoredSample.id.desc())\
                .filter(
                    StoredSample.biobank_id == participant_biobank_id,
                    StoredSample.sample_id == ordered_sample.nph_sample_id,
                )\
                .all()

        return [
            {
                "limsID": stored_sample.lims_id,
                "biobankModified": _format_timestamp(stored_sample.biobank_modified),
                "status": stored_sample.status.name if stored_sample.status else None,
            } for stored_sample in stored_samples
        ]


class NphIncidentDao(UpdatableDao):
    def __init__(self):
        super(NphIncidentDao, self).__init__(Incident)

    def get_id(self, obj: Incident) -> int:
        return obj.id

    @staticmethod
    def truncate_value(value, max_length):
        is_truncated = False
        if len(value) > max_length:
            is_truncated = True
            value = value[:max_length]

        return is_truncated, value

    def get_by_message(self, message: Optional[str]) -> Optional[Incident]:
        maximum_message_length = Incident.message.type.length
        _, truncated_value = self.truncate_value(
            message,
            maximum_message_length,
        )
        with self.session() as session:
            return session.query(
                    Incident
                ).filter(
                    Incident.message == truncated_value
                ).first()

    def insert(self, obj: Incident) -> Incident:
        maximum_message_length = Incident.message.type.length
        is_truncated, truncated_value = self.truncate_value(
            obj.message,
            maximum_message_length,
        )
        if is_truncated:
            _logger.warning('Truncating incident message when storing (too many characters for database column)')
        obj.message = truncated_value

        with self.session() as session:
            return self.insert_with_session(session, obj)

    def _validate_update(self, session, obj, existing_obj):
        # NPH Incidents aren't versioned; suppress the normal check here.
        pass

    def update(self, obj: Incident) -> Incident:
        maximum_message_length = Incident.message.type.length
        is_truncated, truncated_value = self.truncate_value(
            obj.message,
            maximum_message_length,
        )
        if is_truncated:
            _logger.warning('Truncating incident message when storing (too many characters for database column)')
        obj.message = truncated_value
        with self.session() as session:
            return self.update_with_session(session, obj)
