import logging

from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required
from rdr_service.dao.ppsc_dao import SiteDao, PPSCDefaultBaseDao
from rdr_service.model.ppsc import PartnerActivity, Site


class PPSCSiteAPI(BaseApi):

    def __init__(self):
        super().__init__(SiteDao())
        self.current_activities = PPSCDefaultBaseDao(model_type=PartnerActivity).get_all()

    @auth_required([PPSC, RDR])
    def post(self):
        # Adding request log here so if exception is raised
        # per validation fail the payload is stored
        log_api_request(log=request.log_record)
        req_data, site_record = self.get_request_json(), None

        try:
            site_record = self.handle_site_updates(req_data=req_data)
        except Exception as e:
            logging.warning(f'Error when creating/updating site record: {e}')
            raise BadRequest('Error when creating/updating site record')

        return self.dao.to_client_json(obj=site_record, action_type='created/updated')

    @auth_required([PPSC, RDR])
    def delete(self):
        log_api_request(log=request.log_record)

        req_data, site_record = self.get_request_json(), None
        req_data['active'] = False

        try:
            site_record = self.handle_site_updates(req_data=req_data)
        except Exception as e:
            logging.warning(f'Error when deactivating site record: {e}')
            raise BadRequest('Error when deactivating site record')

        return self.dao.to_client_json(obj=site_record, action_type='deactivated')

    def handle_site_updates(self, *, req_data: dict) -> Site:
        site_record = self.dao.upsert(self.dao.model_type(**req_data))
        return site_record

