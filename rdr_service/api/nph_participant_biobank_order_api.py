from types import SimpleNamespace as Namespace
from typing import Union
import json
from flask import request
from flask_restful import Resource


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__


class NphOrderApi(Resource):

    def put(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            order = self._construct_order_class()
            order.subject = nph_participant_id
            return self._construct_response(order)
        else:
            return

    def post(self, nph_participant_id):
        if nph_participant_id:
            order = self._construct_order_class()
            order.subject = nph_participant_id
            exist, time_point_id = self._module_exist_in_db(order)
            print(time_point_id)
            if not exist:
                pass
            else:
                pass
            return self._construct_response(order)
        else:
            return

    def patch(self, nph_participant_id, rdr_order_id):
        if rdr_order_id and nph_participant_id:
            order = self._construct_order_class()
            print(order)

    @staticmethod
    def _construct_response(order):
        return json.loads(json.dumps(order, indent=4, cls=CustomEncoder))

    @staticmethod
    def _construct_order_class():
        return json.loads(request.get_data(), object_hook=lambda d: Namespace(**d))

    @staticmethod
    def _module_exist_in_db(order: Namespace) -> Union[bool, str]:
        # Compare the module, vistType and time point using self join
        # If module exist, return True and time point id
        pass
