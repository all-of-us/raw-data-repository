import json
from flask import request
import logging

from rdr_service.api.base_api import UpdatableApi
from rdr_service.dao import database_factory
from rdr_service.dao.study_nph_dao import NphOrderDao


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


def construct_response(order):
    # Construct Response payload
    return json.loads(json.dumps(order, indent=4, cls=CustomEncoder))


class NphOrderApi(UpdatableApi):
    def __init__(self):
        super(NphOrderApi, self).__init__(NphOrderDao())

    def put(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            self.dao.set_order_cls(request.get_data())
            order = self.dao.order_cls
            order.subject = nph_participant_id
            return construct_response(order)
        else:
            return

    def update_with_patch(self, id_, resource, expected_version):
        pass

    def post(self, nph_participant_id: str):
        with database_factory.get_database().session() as session:
            self.dao.set_order_cls(request.get_data())
            order = self.dao.order_cls
            exist, time_point_id = self.dao.get_study_category_id(session)
            if not exist:
                logging.warning(f'Inserting new order to study_category table: module = {order.module}, '
                                f'visitType: {order.visitType}, timePoint: {order.timepoint}')
                time_point_id = self.dao.insert_study_category_with_session(order, session)
            self.dao.insert_with_session(order, time_point_id, nph_participant_id, session)
            return construct_response(order), 201

    def patch(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            self.dao.set_order_cls(request.get_data())
            order = self.dao.order_cls
            print(order)
