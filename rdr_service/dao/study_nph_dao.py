from protorpc import messages
from typing import Tuple, Dict
from werkzeug.exceptions import BadRequest, Forbidden
import json
from types import SimpleNamespace as Namespace
from itertools import zip_longest
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

    def get_id(self, participant_id: int) -> int:
        query = Query(Participant)
        query.session = self.session()
        result = query.filter(Participant.id == participant_id).first()
        return result.id

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
                              module.name == order.module).first()
        if not result:
            return False, ""
        else:
            return True, result.id

    def insert_with_session(self, order: Namespace, session):
        module_insert_stmt = StudyCategory(name=order.module, type_label="module")
        session.add(module_insert_stmt)
        session.commit()
        session.refresh(module_insert_stmt)
        visit_insert_stmt = StudyCategory(parent_id=module_insert_stmt.id, name=order.visitType,
                                          type_label="visitType")
        session.add(visit_insert_stmt)
        session.commit()
        session.refresh(visit_insert_stmt)
        time_point_insert_stmt = StudyCategory(parent_id=visit_insert_stmt.id,
                                               name=order.timepoint, type_label="timepoint")
        session.add(time_point_insert_stmt)
        session.commit()
        session.refresh(time_point_insert_stmt)
        return time_point_insert_stmt.id


class NphSiteDao(BaseDao):
    def __init__(self):
        super(NphSiteDao, self).__init__(Site)

    @staticmethod
    def _fetch_site_id(session, obj) -> Dict:
        keys = ["created", "collected", "finalized"]
        query = Query(Site)
        query.session = session
        response = {}
        for (each, key) in zip_longest([obj.createdInfo.site.system, obj.collectedInfo.site.system,
                                        obj.finalizedInfo.site.system], keys):
            result = query.filter(Site.name == each).first()
            response[key] = result.id
        return response

    def get_id(self, session, obj: Namespace) -> Dict:
        return self._fetch_site_id(session, obj)

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

    def get_study_category_id(self, session):
        return self.study_category_dao.get_id(session, self.order_cls)

    def set_order_cls(self, resource_data):
        self.order_cls = json.loads(resource_data, object_hook=lambda d: Namespace(**d))

    def _fetch_identifier_value(self, identifier: str) -> str :
        for each in self.order_cls.identifier:
            if each.system == f"http://www.pmi-ops.org/{identifier}":
                return each.value

    def from_client_json(self, session, nph_participant_id, category_id):
        site_response = self.site_dao.get_id(session, self.order_cls)
        create_site = site_response.get("created")
        collected_site = site_response.get("collected")
        finalized_site = site_response.get("finalized")
        if not create_site and not collected_site and not finalized_site:
            raise BadRequest("Site has not been populated in Site Table")
        order = Order()
        for order_model_field, resource_value in [("nph_order_id", self._fetch_identifier_value("order-id")),
                                                  ("order_created", self.order_cls.created),
                                                  ("category_id", category_id),
                                                  ("participant_id", int(nph_participant_id[1:])),
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

    def check_order(self, obj) -> Tuple[bool, str]:
        module = aliased(StudyCategory)
        visit_type = aliased(StudyCategory)
        time_point = aliased(StudyCategory)
        query = Query(time_point)
        query.session = self.session()
        result = query.filter(module.id == visit_type.parent_id, visit_type.id == time_point.parent_id,
                              module.name == obj.module).first()
        if not result:
            return False, ""
        else:
            return True, result.id

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


class NphOrderedSampleDao(UpdatableDao):
    def __init__(self):
        super(NphOrderedSampleDao, self).__init__(OrderedSample)

    def get_id(self, obj: OrderedSample):
        return obj.id

    def from_client_json(self, obj: Namespace, order_id: int, nph_sample_id: str) -> OrderedSample:
        if self._fetch_supplemental_fields(obj):
            return OrderedSample(nph_sample_id=nph_sample_id,
                                 order_id=order_id,
                                 test=obj.sample.test,
                                 description=obj.sample.description,
                                 collected=obj.sample.collected,
                                 finalized=obj.sample.finalized,
                                 supplemental_fields=self._fetch_supplemental_fields(obj)
                                 )
        else:
            return OrderedSample(nph_sample_id=nph_sample_id,
                                 order_id=order_id,
                                 test=obj.sample.test,
                                 description=obj.sample.description,
                                 collected=obj.sample.collected,
                                 finalized=obj.sample.finalized
                                 )

    @staticmethod
    def from_aliquot_client_json(aliquot, order_id: int, nph_sample_id: str, parent_sample_id: int) -> OrderedSample:
        return OrderedSample(nph_sample_id=nph_sample_id,
                             order_id=order_id,
                             parent_sample_id=parent_sample_id,
                             aliquot_id=aliquot.id,
                             container=aliquot.container,
                             volume=aliquot.volume
                             )

    @staticmethod
    def _fetch_supplemental_fields(order_cls) -> Dict:
        keys = ["test", "description", "collected", "finalized"]
        result = {k: v for k, v in order_cls.sample.__dict__.items() if k not in keys}
        if len(result) == 0:
            return {}
        else:
            return result

    def insert_with_session(self, order: Namespace, order_id: int, session):
        self._insert_order_sample(order, order_id, session)

    def _insert_order_sample(self, order: Namespace, order_id: int, session):
        # Adding record(s) to nph.order_sample table
        nph_sample_id = fetch_identifier_value(order, "sample-id")
        os = self.from_client_json(order, order_id, nph_sample_id)
        session.add(os)
        session.commit()
        session.refresh(os)
        parent_sample_id = os.id
        if order.aliquots:
            for aliquot in order.aliquots:
                oa = self.from_aliquot_client_json(aliquot, order_id, nph_sample_id, parent_sample_id)
                session.add(oa)
            session.commit()

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
    if obj.status != OrderStatus.RESTORED or obj.withdrawalStatus != OrderStatus.CANCELED:
        raise Forbidden(f"Invalid order status: {obj.status}")


def fetch_identifier_value(obj: Namespace, identifier: str) -> str:
    for each in obj.identifier:
        if each.system == f"http://www.pmi-ops.org/{identifier}":
            return each.value
