from flask import request

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required
from rdr_service.dao.ppsc_dao import SiteDao, PPSCDefaultBaseDao
from rdr_service.model.ppsc import PartnerActivity


class PPSCSiteAPI(BaseApi):
    def __init__(self):
        super().__init__(SiteDao())
        self.current_activities = PPSCDefaultBaseDao(model_type=PartnerActivity).get_all()
        # self.site_dao = SiteDao()

    @auth_required([PPSC, RDR])
    def post(self):
        # Adding request log here so if exception is raised
        # per validation fail the payload is stored
        log_api_request(log=request.log_record)
        print('Darryl')

    @auth_required([PPSC, RDR])
    def delete(self):
        log_api_request(log=request.log_record)
        ...
