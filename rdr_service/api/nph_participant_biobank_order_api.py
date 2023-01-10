import json
from flask import request
from werkzeug.exceptions import BadRequest, NotFound
import logging

from rdr_service.api.base_api import UpdatableApi
from rdr_service.dao import database_factory
from rdr_service.dao.study_nph_dao import NphOrderDao
from sqlalchemy import exc


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


def construct_response(order):
    # Construct Response payload
    return json.loads(json.dumps(order, indent=4, cls=CustomEncoder))


class NphOrderApi(UpdatableApi):

    def __init__(self):
        super(NphOrderApi, self).__init__(NphOrderDao())

    def update_with_patch(self, id_, resource, expected_version):
        pass

    def put(self, nph_participant_id, rdr_order_id):
        try:
            with database_factory.get_database().session() as session:
                self.dao.set_order_cls(request.get_data())
                order = self.dao.order_cls
                self.dao.validate(rdr_order_id, nph_participant_id, session)
                self.dao.order_sample_dao.update_order_sample(order, rdr_order_id, session)
                self.dao.update_order(rdr_order_id, nph_participant_id, session)
                session.commit()
            return construct_response(order), 201
        except NotFound as not_found:
            logging.error(not_found)
            return {"error": not_found}, 400
        except BadRequest as bad_request:
            logging.error(bad_request)
            return {"error": bad_request}, 400
        except exc.SQLAlchemyError as sql:
            logging.error(sql)
            return {"error": sql}, 400

    def post(self, nph_participant_id: str):
        with database_factory.get_database().session() as session:
            try:
                self.dao.set_order_cls(request.get_data())
                order = self.dao.order_cls
                exist, time_point_id = self.dao.get_study_category_id(session)
                if not exist:
                    logging.warning(f'Inserting new order to study_category table: module = {order.module}, '
                                    f'visitType: {order.visitType}, timePoint: {order.timepoint}')
                    time_point_id = self.dao.insert_study_category_with_session(order, session)
                order_id = self.dao.insert_with_session(time_point_id, nph_participant_id, session)
                self.dao.insert_ordered_sample_dao_with_session(order, order_id, session)
                order.id = order_id
                return construct_response(order), 201
            except NotFound as not_found:
                logging.error(not_found)
                return {"error": not_found}, 400
            except BadRequest as bad_request:
                logging.error(bad_request)
                return {"error": bad_request}, 400
            except exc.SQLAlchemyError as sql:
                logging.error(sql)
                return {"error": sql}, 400

    def patch(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            try:
                with database_factory.get_database().session() as session:
                    self.dao.set_order_cls(request.get_data())
                    order = self.dao.order_cls
                    self.dao.patch_update(order, rdr_order_id, nph_participant_id, session)
                    session.commit()
                    return construct_response(order), 200
            except NotFound as not_found:
                logging.error(not_found)
                return {"error": not_found}, 400
            except BadRequest as bad_request:
                logging.error(bad_request)
                return {"error": bad_request}, 400
            except exc.SQLAlchemyError as sql:
                logging.error(sql)
                return {"error": sql}, 400
