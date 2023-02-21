import json
import logging
from flask import request

from werkzeug.exceptions import BadRequest, NotFound
from sqlalchemy import exc

from rdr_service.api.base_api import UpdatableApi
from rdr_service.dao import database_factory
from rdr_service.dao.study_nph_dao import NphOrderDao
from rdr_service.api_util import RTI_AND_HEALTHPRO
from rdr_service.app_util import auth_required


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

    @auth_required(RTI_AND_HEALTHPRO)
    def put(self, nph_participant_id, rdr_order_id):
        try:
            if len(nph_participant_id) < 4:
                message = f"Invalid NPH Participant ID. Must be at least 5 characters in length. {nph_participant_id}"
                logging.error(message)
                return {"error": message}, 400
            with database_factory.get_database().session() as session:
                self.dao.set_order_cls(request.get_data())
                order = self.dao.order_cls
                self.dao.validate(rdr_order_id, nph_participant_id, session)
                self.dao.order_sample_dao.update_order_sample(order, rdr_order_id, session)
                self.dao.update_order(rdr_order_id, nph_participant_id, session)
                order.id = rdr_order_id
            return construct_response(order), 201
        except NotFound as not_found:
            logging.error(not_found.description)
            return construct_response(order), 404
        except BadRequest as bad_request:
            logging.error(bad_request.description)
            return construct_response(order), 404
        except exc.SQLAlchemyError as sql:
            logging.error(sql)
            return construct_response(order), 400

    @auth_required(RTI_AND_HEALTHPRO)
    def post(self, nph_participant_id: str):
        if len(nph_participant_id) < 4:
            message = f"Invalid NPH Participant ID. Must be at least 5 characters in length. {nph_participant_id}"
            logging.error(message)
            return {"error": message}, 400
        with database_factory.get_database().session() as session:
            try:
                self.dao.set_order_cls(request.get_data())
                order = self.dao.order_cls
                exist, time_point_id = self.dao.get_study_category_id(session)
                if not exist:
                    logging.warning(f'Inserting new order to study_category table: module = {order.module}, '
                                    f'visitType: {order.visitType}, timePoint: {order.timepoint}')
                    time_point_id = self.dao.insert_study_category_with_session(order, session)[1]
                new_order = self.dao.from_client_json(session, nph_participant_id, time_point_id)
                new_order = self.dao.insert_with_session(session, new_order)
                order.id = new_order.id
                self.dao.insert_ordered_sample_dao_with_session(session, order)
                return construct_response(order), 201
            except NotFound as not_found:
                logging.error(not_found)
                return construct_response(order), 404
            except BadRequest as bad_request:
                logging.error(bad_request)
                return construct_response(order), 400
            except exc.SQLAlchemyError as sql:
                logging.error(sql)
                return construct_response(order), 400

    @auth_required(RTI_AND_HEALTHPRO)
    def patch(self, nph_participant_id, rdr_order_id):
        if len(nph_participant_id) < 4:
            message = f"Invalid NPH Participant ID. Must be at least 5 characters in length. {nph_participant_id}"
            logging.error(message)
            return {"error": message}, 400
        if rdr_order_id and nph_participant_id:
            try:
                with database_factory.get_database().session() as session:
                    self.dao.set_order_cls(request.get_data())
                    order = self.dao.order_cls
                    self.dao.patch_update(order, rdr_order_id, nph_participant_id, session)
                    session.commit()
                    order.id = rdr_order_id
                    return construct_response(order), 200
            except NotFound as not_found:
                logging.error(not_found.description, exc_info=True)
                return construct_response(order), 404
            except BadRequest as bad_request:
                logging.error(bad_request.description, exc_info=True)
                return construct_response(order), 400
