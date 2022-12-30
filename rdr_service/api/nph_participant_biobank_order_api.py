from types import SimpleNamespace as Namespace
from typing import Tuple, Dict
import json
from flask import request
from flask_restful import Resource
from sqlalchemy.orm import Query, aliased
import logging

from rdr_service.dao import database_factory
from rdr_service.model.study_nph import StudyCategory, Order, OrderedSample, Site


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


def construct_response(order):
    # Construct Response payload
    return json.loads(json.dumps(order, indent=4, cls=CustomEncoder))


def construct_order_class():
    # Construct Request payload to order class
    return json.loads(request.get_data(), object_hook=lambda d: Namespace(**d))


class OrderDetail:

    def __init__(self, order: Dict):
        self.subject = order.get('subject', None)
        self.identifier = order.get("identifier", [])
        self.created_info = order.get("createdInfo", [])
        self.collected_info = order.get("collectedInfo", [])
        self.finalized_info = order.get("finalizedInfo", [])
        self.created = order.get("created", None)
        self.visit_type = order.get("visitType", None)
        self.module = order.get("module", None)
        self.sample = order.get("sample", {})
        self.aliquots = order.get("aliquots", [])
        self.notes = order.get("notes", {})


class FetchOrderDetail:

    def __init__(self, order):
        self.identifier = order.identifier
        self.created_site_name = order.createdInfo.site.system
        self.collected_site_name = order.collectedInfo.site.system
        self.finalized_site_name = order.finalizedInfo.site.system
        self.keys = ["test", "description", "collected", "finalized"]
        self.sample = order.sample

    def fetch_identifier_value(self, identifier: str) -> str :
        for each in self.identifier:
            if each.system == f"http://www.pmi-ops.org/{identifier}":
                return each.value

    def fetch_site_id(self, session, site_name: str) -> int:
        query = Query(Site)
        query.session = session
        if site_name == 'created':
            site = self.created_site_name
        elif site_name == 'collected':
            site = self.created_site_name
        else:
            site = self.finalized_site_name
        result = query.filter(Site.name == site).first()
        return result.id

    def fetch_supplemental_fields(self):
        result = {k: v for k, v in self.sample.__dict__.items() if k not in self.keys}
        if len(result) == 0:
            return None
        else:
            return result


class NphOrderApi(Resource):

    def put(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            order = construct_order_class()
            order.subject = nph_participant_id
            return construct_response(order)
        else:
            return

    def post(self, nph_participant_id):
        order = construct_order_class()
        order_detail = FetchOrderDetail(order)
        with database_factory.get_database().session() as sessions:
            exist, time_point_id = self._module_exist_in_db(order, sessions)
            if not exist:
                logging.warning(f'Inserting new order to study_category table: module = {order.module}, '
                                f'visitType: {order.visitType}, timePoint: {order.timepoint}')
                time_point_id = self._insert_new_order(order, sessions)
            order_sample_id = self._insert_order(order_detail, order, time_point_id, nph_participant_id, sessions)
            self._insert_order_sample(order_detail, order, order_sample_id, sessions)
            return construct_response(order), 201

    def patch(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            order = construct_order_class()
            print(order)

    @staticmethod
    def _module_exist_in_db(order: Namespace, sessions) -> Tuple[bool, str]:
        # Compare the module, vistType and time point using self join
        # return False and empty string if module not exist
        # otherwise, return True and time point id
        module = aliased(StudyCategory)
        visit_type = aliased(StudyCategory)
        time_point = aliased(StudyCategory)
        query = Query(time_point)
        query.session = sessions
        result = query.filter(module.id == visit_type.parent_id, visit_type.id == time_point.parent_id,
                              module.name == order.module).first()
        if not result:
            return False, ""
        else:
            return True, result.id

    @staticmethod
    def _insert_new_order(order: Namespace, session) -> int:
        # Function to add order payload when no matching categorization exists yet
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

    @staticmethod
    def _insert_order(order_detail: FetchOrderDetail, order: Namespace, time_order_id: int,
                      nph_participant_id: str, session) -> int:
        # Adding record(s) to nph.order table
        order_insert_stmt = Order(nph_order_id=order_detail.fetch_identifier_value("order-id"),
                                  order_created=order.created,
                                  category_id=time_order_id,
                                  participant_id=int(nph_participant_id[1:]),
                                  created_author=order.createdInfo.author.value,
                                  created_site=order_detail.fetch_site_id(session, "created"),
                                  collected_author=order.collectedInfo.author.value,
                                  collected_site=order_detail.fetch_site_id(session, "collected"),
                                  finalized_author=order.finalizedInfo.author.value,
                                  finalized_site=order_detail.fetch_site_id(session, "finalized"),
                                  notes=order.notes.__dict__
                                  )
        session.add(order_insert_stmt)
        session.commit()
        session.refresh(order_insert_stmt)
        return order_insert_stmt.id

    @staticmethod
    def _insert_order_sample(order_detail: FetchOrderDetail, order: Namespace, order_id: int, session):
        # Adding record(s) to nph.order_sample table
        order_sample_insert_stmt = OrderedSample(nph_sample_id=order_detail.fetch_identifier_value("sample-id"),
                                                 order_id=order_id,
                                                 test=order.sample.test,
                                                 description=order.sample.description,
                                                 collected=order.sample.collected,
                                                 finalized=order.sample.finalized,
                                                 supplemental_fields=order_detail.fetch_supplemental_fields()
                                                 )
        session.add(order_sample_insert_stmt)
        session.commit()
        session.refresh(order_sample_insert_stmt)
        sample_id = order_sample_insert_stmt.id
        for each in order.aliquots:
            order_aliquots_insert_stmt = OrderedSample(nph_sample_id=order_detail.fetch_identifier_value("sample-id"),
                                                       order_id=order_id,
                                                       parent_sample_id=sample_id,
                                                       aliquot_id=each.id,
                                                       container=each.container,
                                                       volume=each.volume
                                                       )
            session.add(order_aliquots_insert_stmt)
        session.commit()
