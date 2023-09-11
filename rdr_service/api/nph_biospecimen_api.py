

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphBiospecimenDao


class NphBiospecimenAPI(BaseApi):
    def __init__(self):
        super().__init__(NphBiospecimenDao())

    @auth_required([RTI, RDR])
    def get(self, nph_participant_id=None):
        print(nph_participant_id)
        return super().get('')

    def validate_biospecimen_params(self):
        ...
        # if request.method == 'GET':
        #     valid_params = ['last_modified']
        #     request_keys = list(request.args.keys())
