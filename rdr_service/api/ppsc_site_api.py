import logging

from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required
from rdr_service.dao.ppsc_dao import SiteDao, PPSCDefaultBaseDao
from rdr_service.model.ppsc import PartnerActivity, Site, PartnerEventActivity
from rdr_service.services.ppsc.ppsc_site_sync import SiteDataSync


# pylint: disable=broad-except
class PPSCSiteAPI(BaseApi):

    def __init__(self):
        super().__init__(SiteDao())
        self.current_activities = PPSCDefaultBaseDao(model_type=PartnerActivity).get_all()
        self.partner_event_activity_dao = PPSCDefaultBaseDao(model_type=PartnerEventActivity)

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
        site_record = self.dao.upsert(self.dao.model_type(**site_data))

        # event tracking
        site_data.pop("created", None)
        site_data.pop("modified", None)
        current_partner_activity = list(filter(lambda x: x.name.lower() == 'site update', self.current_activities))
        current_partner_activity = current_partner_activity[0]
        participant_event_activity_dict = {
            'activity_id': current_partner_activity.id,
            'resource': site_data
        }
        self.partner_event_activity_dao.insert(
            self.partner_event_activity_dao.model_type(
                **participant_event_activity_dict
            )
        )

        self.sync_to_rdr_schema(site_data=site_data)
        return site_record

    @classmethod
    def sync_to_rdr_schema(cls, *, site_data):
        try:
            SiteDataSync(site_data=site_data).run_site_sync()
        except Exception as e:
            logging.warning(f'Error when syncing data to RDR schema: {e}')
