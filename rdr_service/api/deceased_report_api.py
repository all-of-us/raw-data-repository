from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service.api.base_api import BaseApi, UpdatableApi
from rdr_service.api_util import HEALTHPRO, PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required, check_auth
from rdr_service.dao.deceased_report_dao import DeceasedReportDao
from rdr_service.model.utils import from_client_participant_id


class DeceasedReportApiMixin:
    def __init__(self):
        super().__init__(DeceasedReportDao())


class DeceasedReportApi(DeceasedReportApiMixin, BaseApi):
    """
    If a participant passes away, the deceased report API is how the RDR is updated with the relevant information.

    Endpoints are available for creating and reviewing deceased reports, as well as retrieving lists of reports for
    one or more participants.
    """

    @auth_required(PTC_AND_HEALTHPRO)
    def list(self, participant_id=None):
        search_kwargs = {key: value for key, value in request.args.items()}

        if participant_id is not None:
            participant_id = from_client_participant_id(participant_id)
            found_reports = self.dao.load_reports(participant_id=participant_id, **search_kwargs)
        else:
            # Only HEALTHPRO should be able to pull reports for multiple participants
            check_auth([HEALTHPRO])
            found_reports = self.dao.load_reports(**search_kwargs)

        response = []
        for report in found_reports:
            response.append(self.dao.to_client_json(report))

        return response

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
