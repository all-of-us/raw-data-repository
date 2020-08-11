from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service.api.base_api import BaseApi, UpdatableApi
from rdr_service.api_util import HEALTHPRO, PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.deceased_report_dao import DeceasedReportDao
from rdr_service.model.utils import from_client_participant_id


class DeceasedReportApiMixin:
    def __init__(self):
        super().__init__(DeceasedReportDao())


class DeceasedReportApi(DeceasedReportApiMixin, BaseApi):
    def list(self, participant_id=None):
        search_kwargs = {key: value for key, value in request.args.items()}
        reports = []
        for report in self.dao.load_reports(participant_id=participant_id, **search_kwargs):
            reports.append(self.dao.to_client_json(report))

        return reports

    @auth_required(PTC_AND_HEALTHPRO)
    def post(self, participant_id=None):
        resource = request.get_json(force=True)
        if 'code' not in resource or\
                'text' not in resource['code'] or\
                resource['code']['text'] != 'DeceasedReport':
            raise BadRequest('Must be a DeceasedReport observation')

        participant_id = from_client_participant_id(participant_id)
        return super(DeceasedReportApi, self).post(participant_id=participant_id)


class DeceasedReportReviewApi(DeceasedReportApiMixin, UpdatableApi):
    @auth_required(HEALTHPRO)
    def post(self, participant_id, report_id):
        participant_id = from_client_participant_id(participant_id)

        # Using super's PUT to make use of update functionality in DAO while it looks like they're POSTing a review
        return super(DeceasedReportReviewApi, self).put(report_id, participant_id=participant_id, skip_etag=True)
