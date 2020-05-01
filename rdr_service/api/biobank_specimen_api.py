from flask import request

from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from rdr_service.model.utils import from_client_participant_id


class BiobankSpecimenApi(UpdatableApi):
    def __init__(self):
        super().__init__(BiobankSpecimenDao(), get_returns_children=True)

    @auth_required(HEALTHPRO)
    def post(self, p_id):
        return super().post(participant_id=p_id)

    @auth_required(HEALTHPRO)
    def get(self, p_id=None, bo_id=None):  # pylint: disable=unused-argument
        return super().get(id_=bo_id, participant_id=p_id)

    @auth_required(HEALTHPRO)
    def put(self, *args, **kwargs):  # pylint: disable=unused-argument
        resource = request.get_json(force=True)
        if self.dao.exists(resource):
            # TODO: rlims_id is unique
            return super().put(kwargs['rlims_id'])
        else:
            participant_id = from_client_participant_id(resource['participantId'])
            return super().post(participant_id=participant_id)

