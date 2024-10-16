import json
import logging
from flask import request

from werkzeug.exceptions import BadRequest, NotFound
from sqlalchemy import exc

from rdr_service.api.base_api import UpdatableApi, log_api_request
from rdr_service.dao import database_factory
from rdr_service.dao.study_nph_dao import NphOrderDao, DlwDosageDao
from rdr_service.api_util import RTI_AND_HEALTHPRO, RDR_AND_HEALTHPRO
from rdr_service.app_util import auth_required


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


def construct_response(order):
    return json.loads(json.dumps(order, indent=4, cls=CustomEncoder))


class NphOrderApi(UpdatableApi):

    def __init__(self):
        super(NphOrderApi, self).__init__(NphOrderDao())

    def update_with_patch(self, id_, resource, expected_version):
        pass

    @classmethod
    def check_nph_participant(cls, nph_participant_id):
        if len(nph_participant_id) < 4:
            message = f"Invalid NPH Participant ID. Must be at least 5 characters in length. {nph_participant_id}"
            logging.error(message)
            return {"error": message}, 400

    @auth_required(RTI_AND_HEALTHPRO)
    def put(self, nph_participant_id, rdr_order_id):
        self.check_nph_participant(nph_participant_id)
        try:
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
        self.check_nph_participant(nph_participant_id)
        try:
            with database_factory.get_database().session() as session:
                self.dao.set_order_cls(request.get_data())
                order = self.dao.order_cls
                exist, time_point_id = self.dao.get_study_category_id(session)
                if not exist:
                    visit_name = order.visitPeriod if hasattr(order, 'visitPeriod') else order.visitType
                    logging.warning(f'Inserting new order to study_category table: module = {order.module}, '
                                    f'visitType: {visit_name}, timePoint: {order.timepoint}')
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
        self.check_nph_participant(nph_participant_id)
        try:
            if rdr_order_id and nph_participant_id:
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


class DlwDosageApi(UpdatableApi):

    def __init__(self):
        super().__init__(DlwDosageDao())

    def _make_response(self, obj):
        return self.dao.to_client_json(model=obj)

    @auth_required(RDR_AND_HEALTHPRO)
    def post(self, nph_participant_id):
        resource = self.get_request_json()
        m = self._get_model_to_insert(resource, nph_participant_id)
        result = self._do_insert(m)

        log_api_request(log=request.log_record, model_obj=result)
        self._archive_request_log()
        return self._make_response(result), 200

    @auth_required(RDR_AND_HEALTHPRO)
    def put(self, nph_participant_id: str, dlw_dosage_id: int):
        try:
            resource = self.get_request_json()
            m = self._get_model_to_update(
                resource=resource, id_=dlw_dosage_id, expected_version=None, participant_id=nph_participant_id
            )
            self._do_update(m)
            return 200
        except NotFound as not_found:
            logging.error(not_found.description, exc_info=True)
            return not_found.description, 404
        except BadRequest as bad_request:
            logging.error(bad_request.description, exc_info=True)
            return bad_request.description, 400
