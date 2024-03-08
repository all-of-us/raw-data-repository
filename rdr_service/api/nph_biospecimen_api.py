from flask import request

from werkzeug.exceptions import NotFound
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RTI, RDR_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphBiospecimenDao, NphParticipantDao


class NphBiospecimenAPI(BaseApi):
    def __init__(self):
        super().__init__(NphBiospecimenDao())

    @auth_required(RDR_AND_HEALTHPRO + [RTI])
    def get(self, nph_participant_id=None):
        log_api_request(log=request.log_record)
        if nph_participant_id:
            nph_participant_dao = NphParticipantDao()
            with nph_participant_dao.session() as session:
                if nph_participant_dao.get_participant_by_id(
                    nph_participant_id,
                    session
                ):
                    biospecimen_data = self.dao.get_orders_samples_subquery(
                        nph_participant_id=nph_participant_id
                    )
                    return self._make_response(biospecimen_data)
                raise NotFound(f'NPH participant {nph_participant_id} was not found')
        return self._query("nph_participant_id")

    def _make_response(self, payload):
        payload_response, payload = [], [payload] if type(payload) is not list else payload
        for participant_obj in payload:
            updated_payload = {
                'nph_participant_id': participant_obj.orders_samples_pid,
                'biospecimens': self.dao.update_biospeciman_stored_samples(
                    order_samples=participant_obj.orders_sample_status,
                    order_biobank_samples=participant_obj.orders_sample_biobank_status,
                )}
            payload_response.append(updated_payload)
        return self.dao.to_client_json(payload_response)

    @classmethod
    def _make_resource_url(cls, response_json, id_field, participant_id):
        from rdr_service import main
        return main.api.url_for(cls, nph_participant_id=response_json[0][id_field], _external=True)

