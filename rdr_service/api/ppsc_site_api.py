import logging
import datetime

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
            site_record = self.dao.get_site_by_identifier(site_identifier=req_data.get('site_identifier'))
            action_type = 'created'

            if site_record:
                req_data['id'] = site_record.id
                action_type = 'updated'

            site_record = self.handle_site_updates(site_data=req_data)
            return self.dao.to_client_json(obj=site_record, action_type=action_type)

        except Exception as e:
            logging.warning(f'Error when creating/updating site record: {e}')
            raise BadRequest('Error when creating/updating site record')

    @auth_required([PPSC, RDR])
    def delete(self):
        log_api_request(log=request.log_record)

        req_data, site_record = self.get_request_json(), None

        try:
            site_record = self.dao.get_site_by_identifier(site_identifier=req_data.get('site_identifier'))

            if site_record:
                site_record = site_record.asdict()
                site_record['active'] = False
                site_record = self.handle_site_updates(site_data=site_record)
                return self.dao.to_client_json(obj=site_record, action_type='deactivated')

            raise BadRequest(f'Cannot find site record with identifier'
                             f' {req_data.get("site_identifier")} for deactivation')

        except Exception as e:
            logging.warning(f'Error when deactivating site record: {e}')
            raise BadRequest('Error when deactivating site record')

    def handle_site_updates(self, *, site_data: dict) -> Site:
        # site data
        site_data['modified'] = datetime.datetime.now()
        site_record = self.dao.upsert(self.dao.model_type(**site_data))
        # event tracking
        return site_record

