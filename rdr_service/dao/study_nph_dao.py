from protorpc import messages
from typing import Tuple, Dict
from werkzeug.exceptions import BadRequest, Forbidden, NotFound
import json
from types import SimpleNamespace as Namespace
from sqlalchemy.orm import Query, aliased

from rdr_service.dao.base_dao import BaseDao, UpdatableDao
from rdr_service.model.study_nph import (
    Participant, StudyCategory, Site, Order, OrderedSample, SampleUpdate, BiobankFileExport, SampleExport
)


class OrderStatus(messages.Enum):
    """A status reflecting the NPH order of the participant"""

    RESTORED = 1
    CANCELED = 2


class NphParticipantDao(BaseDao):
    def __init__(self):
        super(NphParticipantDao, self).__init__(Participant)

    def get_id(self, session, nph_participant_id: str) -> int:
        participant_id = self.convert_id(nph_participant_id)
        query = Query(Participant)
        query.session = session
        result = query.filter(Participant.id == participant_id).first()
        if result.id:
            return result.id
        else:
            raise NotFound(f"Participant ID not found : {participant_id}")

    @staticmethod
    def convert_id(nph_participant_id: str) -> int:
        return int(nph_participant_id[3:])

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
    def _get_study_category_sample(category_id, session) -> Tuple[StudyCategory, StudyCategory, StudyCategory]:
        # Fetching study category values from the db table
        query = Query(StudyCategory)
        query.session = session
        time_point_record = query.filter(StudyCategory.id == category_id).first()
        visit_type_record = query.filter(StudyCategory.id == time_point_record.parent_id,
                                         StudyCategory.type_label == "visitType").first()
        module_record = query.filter(StudyCategory.id == visit_type_record.parent_id,
                                     StudyCategory.type_label == "module").first()
        return time_point_record, visit_type_record, module_record

    def insert_with_session(self, order: Namespace, session):
        # Insert the study category payload values to the db table
        module = StudyCategory(name=order.module, type_label="module")
        visit = StudyCategory(name=order.visitType, type_label="visitType")
        module.children.append(visit)
        time = StudyCategory(name=order.timepoint, type_label="timepoint")
        visit.children.append(time)
        session.add(module)
        session.commit()
        return time.id

    def insert(self, session, study_category: StudyCategory):
        session.add(study_category)
        session.commit()

    def _validate_model(self, obj):
        if obj.module is None:
            raise BadRequest("Module is missing")
        if obj.visitType is None:
            raise BadRequest("Visit Type is missing")
        if obj.timpoint is None:
            raise BadRequest("Time Point ID is missing")


class NphSiteDao(BaseDao):
    def __init__(self):
        super(NphSiteDao, self).__init__(Site)

    @staticmethod
    def _fetch_site_id(session, site_name) -> int:
        query = Query(Site)
        query.session = session
        result = query.filter(Site.name == site_name).first()
        return result.id

    def get_id(self, session, site_name: str) -> int:
        return self._fetch_site_id(session, site_name)

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

    def compare(self, order_id: int, session):
        pass

    @staticmethod
    def _get_order(order_id: int, session) -> Order:
        query = Query(Order)
        query.session = session
        return query.filter(Order.id == order_id).first()

    def get_study_category_id(self, session):
        return self.study_category_dao.get_id(session, self.order_cls)

    def set_order_cls(self, resource_data):
        self.order_cls = json.loads(resource_data, object_hook=lambda d: Namespace(**d))

    def from_client_json(self, session, nph_participant_id, category_id):
        create_site = self.site_dao.get_id(session, self.order_cls.createdInfo.site.value)
        collected_site = self.site_dao.get_id(session, self.order_cls.collectedInfo.site.value)
        finalized_site = self.site_dao.get_id(session, self.order_cls.finalizedInfo.site.value)
        if not create_site and not collected_site and not finalized_site:
            raise BadRequest("Site has not been populated in Site Table")
        order = Order()
        for order_model_field, resource_value in [("nph_order_id", fetch_identifier_value(self.order_cls, "order-id")),
                                                  ("order_created", self.order_cls.created),
                                                  ("category_id", category_id),
                                                  ("participant_id",
                                                   self.participant_dao.convert_id(nph_participant_id)),
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

    def insert_with_session(self, order: Namespace, time_order_id: int, nph_participant_id: int, session):
       # Adding record(s) to nph.order table

        o = self.from_client_json(session, nph_participant_id, time_order_id)
        session.add(o)
        session.commit()
        session.refresh(o)
        order_id = o.id
        self.order_sample_dao.insert_with_session(order, order_id, session)
        order.id = order_id

    def insert(self, session, order: Order):
        session.add(order)
        session.commit()


class NphOrderedSampleDao(UpdatableDao):
    def __init__(self):
        super(NphOrderedSampleDao, self).__init__(OrderedSample)

    def get_id(self, obj: OrderedSample):
        return obj.id

    @staticmethod
    def _get_parent_order_sample(order_id, session) -> OrderedSample:
        query = Query(OrderedSample)
        query.session = session
        result = query.filter(OrderedSample.id == order_id).first()
        return result

    @staticmethod
    def _get_child_order_sample(order_id, session) -> OrderedSample:
        query = Query(OrderedSample)
        query.session = session
        result = query.filter(OrderedSample.parent_sample_id == order_id).all()
        return result

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
                             container=aliquot.container,
                             volume=aliquot.volume
                             )

    @staticmethod
    def _fetch_supplemental_fields(order_cls) -> Dict:
        keys = ["test", "description", "collected", "finalized"]
        result = {k: v for k, v in order_cls.sample.__dict__.items() if k not in keys}
        return result

    def insert_with_session(self, order: Namespace, order_id: int, session):
        self._insert_order_sample(order, order_id, session)

    def _insert_order_sample(self, order: Namespace, order_id: int, session):
        # Adding record(s) to nph.order_sample table
        nph_sample_id = fetch_identifier_value(order, "sample-id")
        os = self.from_client_json(order, order_id, nph_sample_id)
        if order.__dict__.get("aliquots"):
            for aliquot in order.aliquots:
                oa = self.from_aliquot_client_json(aliquot, order_id, nph_sample_id)
                os.children.append(oa)
        session.add(os)
        session.commit()

    def insert(self, session, order_sample: OrderedSample):
        session.add(order_sample)
        session.commit()

    def _validate_model(self, obj):
        if obj.order_id is None:
            raise BadRequest("Order ID is missing")


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


def raise_if_invalid_status(obj):
    if obj.status != OrderStatus.RESTORED or obj.status != OrderStatus.CANCELED:
        raise Forbidden(f"Invalid order status: {obj.status}")


def fetch_identifier_value(obj: Namespace, identifier: str) -> str:
    for each in obj.identifier:
        if each.system == f"http://www.pmi-ops.org/{identifier}":
            return each.value
