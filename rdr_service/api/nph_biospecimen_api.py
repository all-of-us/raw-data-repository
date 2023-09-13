from flask import request

from werkzeug.exceptions import BadRequest
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api.nph_participant_api_schemas.util import NphParticipantData
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphBiospecimenDao, NphParticipantDao


class NphBiospecimenAPI(BaseApi):
    def __init__(self):
        super().__init__(NphBiospecimenDao())
        self.nph_participant_dao = NphParticipantDao()
        self.validate_biospecimen_params()

    @auth_required([RTI, RDR])
    def get(self, nph_participant_id=None):
        log_api_request(log=request.log_record)
        payload_response = []
        if nph_participant_id:
            payload = self.nph_participant_dao.get_orders_samples_subquery(
                nph_participant_id=nph_participant_id
            )
            for participant_obj in payload:
                updated_payload = {
                    'nph_participant_id': participant_obj.orders_samples_pid,
                    'biospecimens': NphParticipantData.update_nph_participant_biospeciman_samples(
                        order_samples=participant_obj.orders_sample_status,
                        order_biobank_samples=participant_obj.orders_sample_biobank_status,
                    )}
                payload_response.append(updated_payload)
            return self._make_response(payload_response)

    @staticmethod
    def validate_biospecimen_params():
        valid_params, params_sent = [], []
        if request.method == 'GET':
            valid_params = ['last_modified']
            params_sent = list(request.args.keys())
            if any(arg for arg in params_sent if arg not in valid_params):
                raise BadRequest(f"NPH Biospecimen GET accepted params: {' | '.join(valid_params)}")
