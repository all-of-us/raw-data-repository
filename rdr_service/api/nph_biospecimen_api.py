from flask import request

from werkzeug.exceptions import NotFound
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api.nph_participant_api_schemas.util import NphParticipantData
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphBiospecimenDao, NphParticipantDao


class NphBiospecimenAPI(BaseApi):
    def __init__(self):
        super().__init__(NphBiospecimenDao())
        self.nph_participant_dao = NphParticipantDao()

    @auth_required([RTI, RDR])
    def get(self, nph_participant_id=None):
        log_api_request(log=request.log_record)
        if nph_participant_id:
            with self.nph_participant_dao.session() as session:
                if self.nph_participant_dao.get_participant_by_id(
                    nph_participant_id,
                    session
                ):
                    biospecimen_data = self.nph_participant_dao.get_orders_samples_subquery(
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
                'biospecimens': NphParticipantData.update_nph_participant_biospeciman_samples(
                    order_samples=participant_obj.orders_sample_status,
                    order_biobank_samples=participant_obj.orders_sample_biobank_status,
                )}
            payload_response.append(updated_payload)
        return self.dao.to_client_json(payload_response)

    def _make_bundle(self, results, id_field, participant_id):
        from rdr_service import main
        bundle_dict, entries = {"resourceType": "Bundle", "type": "searchset"}, []
        if results.pagination_token:
            query_params = request.args.copy()
            query_params["_token"] = results.pagination_token
            next_url = main.api.url_for(self.__class__, _external=True, **query_params.to_dict(flat=False))
            bundle_dict["link"] = [{"relation": "next", "url": next_url}]
        for item in results.items:
            entries.append({"resource": self._make_response(item)})
        bundle_dict["entry"] = entries
        if results.total:
            bundle_dict["total"] = results.total
        return bundle_dict

