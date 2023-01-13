from typing import Tuple, Dict, List, Any
import json
from types import SimpleNamespace as Namespace
from protorpc import messages
from werkzeug.exceptions import BadRequest, NotFound

from sqlalchemy.orm import Query, aliased
from sqlalchemy import exc

from rdr_service.model.study_nph import (
    StudyCategory, Participant, Site, Order, OrderedSample,
    Activity, ParticipantEventActivity, EnrollmentEventType, EnrollmentEvent, PairingEventType, PairingEvent,
    ConsentEventType, ConsentEvent)
from rdr_service.dao.base_dao import BaseDao, UpdatableDao


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
        participant_id = self.convert_id(nph_participant_id)
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == participant_id).first()
        if result:
            return result.id
        else:
            raise NotFound(f"Participant ID not found : {participant_id}")

    def get_participant(self, nph_participant_id: str, session) -> Participant:
        participant_id = self.convert_id(nph_participant_id)
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == participant_id).first()
        if result:
            return result
        else:
            raise NotFound(f"Participant not found : {participant_id}")

    def check_participant_exist(self, nph_participant_id: str, session) -> bool:
        participant_id = self.convert_id(nph_participant_id)
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == participant_id).first()
        if result:
            return True
        else:
            return False

    @staticmethod
    def convert_id(nph_participant_id: str) -> int:
        return int(nph_participant_id[4:])

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

    def insert_with_session(self, session, order: Namespace):
        # Insert the study category payload values to the db table
        module_exist, module = self.module_exist(order, session)
        visit_exist, visit = self.visit_type_exist(order, module, session)
        if not module_exist:
            module = StudyCategory(name=order.module, type_label="module")
        if not visit_exist:
            visit = StudyCategory(name=order.visitType, type_label="visitType")
            module.children.append(visit)
        time = self.insert_time_point_record(order)
        visit.children.append(time)
        session.add(module)
        session.commit()
        return module, time.id

    @staticmethod
    def insert_time_point_record(order: Namespace):
        return StudyCategory(name=order.timepoint, type_label="timepoint")

    @staticmethod
    def validate_model(obj):
        if obj.__dict__.get("module") is None:
            raise BadRequest("Module is missing")
        if obj.__dict__.get("visitType") is None:
            raise BadRequest("Visit Type is missing")
        if obj.__dict__.get("timepoint") is None:
            raise BadRequest("Time Point ID is missing")

    @staticmethod
    def module_exist(order: Namespace, session):

        query = Query(StudyCategory)
        query.session = session
        result = query.filter(StudyCategory.type_label == "module", StudyCategory.name == order.module).first()
        if result:
            return True, result
        else:
            return False, None

    @staticmethod
    def visit_type_exist(order: Namespace, module: StudyCategory, session):

        query = Query(StudyCategory)
        query.session = session
        if module:
            result = query.filter(StudyCategory.type_label == "visitType", StudyCategory.name == order.visitType,
                                  StudyCategory.parent_id == module.id).first()
            if result:
                return True, result

        return False, None


class NphSiteDao(BaseDao):
    def __init__(self):
        super(NphSiteDao, self).__init__(Site)

    @staticmethod
    def _fetch_site_id(session, site_name) -> int:
        query = Query(Site)
        query.session = session
        result = query.filter(Site.name == site_name).first()
        if result is None:
            raise NotFound(f"Site is not found -- {site_name}")
        return result.id

    def get_id(self, session, site_name: str) -> int:
        try:
            return self._fetch_site_id(session, site_name)
        except NotFound:
            raise

    @staticmethod
    def site_exist(session, site_name: str) -> bool:
        query = Query(Site)
        query.session = session
        result = query.filter(Site.name == site_name).first()
        if result is None:
            return False
        return True

    def from_client_json(self):
        pass


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
        try:
            if order.status.upper() == "RESTORED":
                site_name = order.restoredInfo.site.value
                amended_author = order.restoredInfo.author.value
            elif order.status.upper() == "CANCELED":
                site_name = order.cancelledInfo.site.value
                amended_author = order.cancelledInfo.author.value
            else:
                raise BadRequest(f"Invalid status value: {order.status}")
            site_id = self.site_dao.get_id(session, site_name)
            amended_reason = order.amendedReason
            db_order = self.get_order(rdr_order_id, session)
            if db_order.participant_id == self.participant_dao.convert_id(nph_participant_id):
                db_order.amended_author = amended_author
                db_order.amended_site = site_id
                db_order.amended_reason = amended_reason
                db_order.status = order.status
            else:
                raise BadRequest("Participant ID does not match the corresponding Order ID.")
            return db_order
        except exc.SQLAlchemyError as ex:
            raise ex
        except NotFound as not_found:
            raise not_found
        except BadRequest as bad_request:
            raise bad_request
        except Exception as exp:
            raise exp

    def update_order(self, rdr_order_id: int, nph_participant_id: str, session) -> Order:
        create_site = self.site_dao.get_id(session, self.order_cls.createdInfo.site.value)
        collected_site = self.site_dao.get_id(session, self.order_cls.collectedInfo.site.value)
        finalized_site = self.site_dao.get_id(session, self.order_cls.finalizedInfo.site.value)
        db_order = self.get_order(rdr_order_id, session)
        if db_order.participant_id == self.participant_dao.convert_id(nph_participant_id):
            db_order.nph_order_id = fetch_identifier_value(self.order_cls, "order-id")
            db_order.created_author = self.order_cls.createdInfo.author.value
            db_order.created_site = create_site
            db_order.collected_author = self.order_cls.collectedInfo.author.value
            db_order.collected_site = collected_site
            db_order.finalized_author = self.order_cls.finalizedInfo.author.value
            db_order.finalized_site = finalized_site
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

    def insert_study_category_with_session(self, order: Namespace, session):
        return self.study_category_dao.insert_with_session(order, session)

    def insert_ordered_sample_dao_with_session(self, session, order: Namespace):
        return self.order_sample_dao.insert_with_session(session, order)

    def insert_with_session(self, session, order: Order) -> Order:
        # Adding record(s) to nph.order table

        try:
            session.add(order)
            session.commit()
            session.refresh(order)
            return order
        except exc.SQLAlchemyError as ex:
            raise ex
        except NotFound as not_found:
            raise not_found
        except BadRequest as bad_request:
            raise bad_request
        except Exception as exp:
            raise exp


class NphOrderedSampleDao(UpdatableDao):
    def __init__(self):
        super(NphOrderedSampleDao, self).__init__(OrderedSample)

    def get_id(self, obj: OrderedSample):
        return obj.id

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
                             volume=aliquot.volume
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
        try:
            nph_sample_id = fetch_identifier_value(order, "sample-id")
            os = self.from_client_json(order, order.id, nph_sample_id)
            if order.__dict__.get("aliquots"):
                for aliquot in order.aliquots:
                    oa = self.from_aliquot_client_json(aliquot, order.id, nph_sample_id)
                    os.children.append(oa)
            session.add(os)
            session.commit()
            return os
        except exc.SQLAlchemyError as sql:
            raise sql
        except NotFound as not_found:
            raise not_found
        except BadRequest as bad_request:
            raise bad_request

    def update_order_sample(self, order: Namespace, rdr_order_id: int, session):
        try:
            db_parent_order_sample = self._get_parent_order_sample(rdr_order_id, session)
            self._update_parent_order(order, db_parent_order_sample)
            db_child_order_sample = self._get_child_order_sample(db_parent_order_sample.id, rdr_order_id, session)
            if len(db_child_order_sample) > 0:
                co_list = self._update_child_order(order, db_child_order_sample, db_parent_order_sample.nph_sample_id,
                                                   rdr_order_id)
                for co in co_list:
                    db_parent_order_sample.children.append(co)
        except exc.SQLAlchemyError as ex:
            raise ex
        except NotFound as not_found:
            raise not_found
        except BadRequest as bad_request:
            raise bad_request
        except Exception as exp:
            raise exp

    def _update_child_order(self, payload: Namespace, order_sample: List[OrderedSample], nph_sample_id: str,
                            rdr_order_id: int) -> List[OrderedSample]:
        try:
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
        except exc.SQLAlchemyError as ex:
            raise ex
        except NotFound as not_found:
            raise not_found
        except BadRequest as bad_request:
            raise bad_request
        except Exception as exp:
            raise exp

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
        order_sample.nph_sample_id = nph_sample_id
        order_sample.identifier = obj.identifier
        order_sample.container = obj.container
        order_sample.volume = obj.volume
        order_sample.description = obj.description
        order_sample.collected = obj.collected
        order_sample.status = "restored"
        return order_sample

    @staticmethod
    def _update_canceled_child_order(order_sample: OrderedSample) -> OrderedSample:
        order_sample.status = "canceled"
        return order_sample

    def _validate_model(self, obj):
        if obj.order_id is None:
            raise BadRequest("Order ID is missing")


def fetch_identifier_value(obj: Namespace, identifier: str) -> str:
    for each in obj.identifier:
        if each.system == f"http://www.pmi-ops.org/{identifier}":
            return each.value


class NphActivityDao(BaseDao):
    def __init__(self):
        super(NphActivityDao, self).__init__(Activity)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphParticipantEventActivityDao(BaseDao):
    def __init__(self):
        super(NphParticipantEventActivityDao, self).__init__(ParticipantEventActivity)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphEnrollmentEventTypeDao(BaseDao):
    def __init__(self):
        super(NphEnrollmentEventTypeDao, self).__init__(EnrollmentEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphEnrollmentEventDao(BaseDao):
    def __init__(self):
        super(NphEnrollmentEventDao, self).__init__(EnrollmentEvent)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphPairingEventTypeDao(BaseDao):
    def __init__(self):
        super(NphPairingEventTypeDao, self).__init__(PairingEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphPairingEventDao(BaseDao):
    def __init__(self):
        super(NphPairingEventDao, self).__init__(PairingEvent)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphConsentEventTypeDao(BaseDao):
    def __init__(self):
        super(NphConsentEventTypeDao, self).__init__(ConsentEventType)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass


class NphConsentEventDao(BaseDao):
    def __init__(self):
        super(NphConsentEventDao, self).__init__(ConsentEvent)

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

