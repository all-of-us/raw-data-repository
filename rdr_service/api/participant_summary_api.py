from flask import request
from werkzeug.exceptions import BadRequest, Forbidden, InternalServerError, NotFound

from rdr_service.api.base_api import BaseApi, make_sync_results_for_request
from rdr_service.api_util import AWARDEE, DEV_MAIL, PTC_HEALTHPRO_AWARDEE, RDR_AND_PTC
from rdr_service.app_util import auth_required, get_validated_user_info
from rdr_service.dao.base_dao import _MIN_ID, _MAX_ID
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.hpo import HPO
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.config import getSettingList, HPO_LITE_AWARDEE
from rdr_service.code_constants import UNSET
from rdr_service.participant_enums import ParticipantSummaryRecord


class ParticipantSummaryApi(BaseApi):
    def __init__(self):
        super(ParticipantSummaryApi, self).__init__(ParticipantSummaryDao(), get_returns_children=True)

    @auth_required(PTC_HEALTHPRO_AWARDEE)
    def get(self, p_id=None):
        # Make sure participant id is in the correct range of possible values.
        if isinstance(p_id, int) and not _MIN_ID <= p_id <= _MAX_ID:
            raise NotFound(f"Participant with ID {p_id} is not found.")
        auth_awardee = None
        user_email, user_info = get_validated_user_info()
        if AWARDEE in user_info["roles"]:
            # if `user_email == DEV_MAIL and user_info.get("awardee") is not None` is True,
            # that means the value of `awardee` is mocked in the test cases, we need to read it from user_info
            if user_email == DEV_MAIL and user_info.get("awardee") is None:
                auth_awardee = request.args.get("awardee")
            else:
                try:
                    auth_awardee = user_info["awardee"]

                except KeyError:
                    raise InternalServerError("Config error for awardee")

        # data only for user_awardee, assert that query has same awardee
        if p_id is not None:
            if auth_awardee and user_email != DEV_MAIL:
                raise Forbidden
            return super(ParticipantSummaryApi, self).get(p_id)
        else:
            if auth_awardee:
                # make sure request has awardee
                requested_awardee = request.args.get("awardee")
                hpo_lite_awardees = getSettingList(HPO_LITE_AWARDEE, default=[])
                if requested_awardee == UNSET and auth_awardee in hpo_lite_awardees:
                    # allow hpo lite awardee to access UNSET participants
                    pass
                elif requested_awardee != auth_awardee:
                    raise Forbidden
            return super(ParticipantSummaryApi, self)._query("participantId")

    def _make_query(self):
        query = super(ParticipantSummaryApi, self)._make_query()
        query.always_return_token = self._get_request_arg_bool("_sync")
        query.backfill_sync = self._get_request_arg_bool("_backfill", True)
        # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
        # query.options = self.dao.get_eager_child_loading_query_options()
        return query

    def _make_bundle(self, results, id_field, participant_id):
        if self._get_request_arg_bool("_sync"):
            return make_sync_results_for_request(self.dao, results)
        return super(ParticipantSummaryApi, self)._make_bundle(results, id_field, participant_id)


class ParticipantSummaryModifiedApi(BaseApi):
    """
  API to return participant_id and last_modified fields
  """

    def __init__(self):
        super(ParticipantSummaryModifiedApi, self).__init__(ParticipantSummaryDao())

    @auth_required(PTC_HEALTHPRO_AWARDEE)
    def get(self):
        """
    Return participant_id and last_modified for all records or a subset based
    on the awardee parameter.
    """
        response = list()
        user_email, user_info = get_validated_user_info()
        request_awardee = None

        with self.dao.session() as session:

            # validate parameter when passed an awardee.
            if "awardee" in request.args:
                request_awardee = request.args.get("awardee")
                hpo = session.query(HPO.hpoId).filter(HPO.name == request_awardee).first()
                if not hpo:
                    raise BadRequest("invalid awardee")

            # verify user has access to the requested awardee.
            if AWARDEE in user_info["roles"] and user_email != DEV_MAIL:
                try:
                    if not request_awardee or user_info["awardee"] != request_awardee:
                        raise Forbidden
                except KeyError:
                    raise InternalServerError("config error for awardee")

            query = session.query(ParticipantSummary.participantId, ParticipantSummary.lastModified)
            query = query.order_by(ParticipantSummary.participantId)
            if request_awardee:
                query = query.filter(ParticipantSummary.hpoId == hpo.hpoId)

            items = query.all()
            for item in items:
                response.append(
                    {"participantId": "P{0}".format(item.participantId), "lastModified": item.lastModified.isoformat()}
                )

        return response


class ParticipantSummaryCheckLoginApi(BaseApi):
    """
  API to return status if data is found / not found on participant summary
  """

    def __init__(self):
        super(ParticipantSummaryCheckLoginApi, self).__init__(ParticipantSummaryDao())

    @auth_required(RDR_AND_PTC)
    def post(self):
        """
        Return status of IN_USE / NOT_IN_USE if participant found / not found
    """
        req_data = request.get_json()
        accepted_map = {
            'email': 'email',
            'login_phone_number': 'loginPhoneNumber'
        }

        if req_data:
            if len(req_data.keys() - accepted_map.keys()):
                raise BadRequest("Only email or login_phone_number are allowed in request")

            if any([key in req_data for key in accepted_map]) \
                    and all([val for val in req_data.values() if val is not None]):

                status = ParticipantSummaryRecord.NOT_IN_USE
                for key, value in req_data.items():
                    found_result = self.dao.get_record_from_attr(
                        attr=accepted_map[key],
                        value=value
                    )
                    if found_result:
                        status = ParticipantSummaryRecord.IN_USE
                        break

                return {'status': status.name}

        raise BadRequest("Missing email or login_phone_number in request")
