import logging
from typing import Optional, List

from flask import request
from werkzeug.exceptions import BadRequest, Forbidden, InternalServerError, NotFound

from rdr_service import config
from rdr_service.api.base_api import BaseApi, make_sync_results_for_request
from rdr_service.api_util import AWARDEE, DEV_MAIL, RDR_AND_PTC, PTC_HEALTHPRO_AWARDEE_CURATION, SUPPORT
from rdr_service.app_util import auth_required, get_validated_user_info, restrict_to_gae_project
from rdr_service.dao.base_dao import _MIN_ID, _MAX_ID
from rdr_service.dao.hpro_consent_dao import HealthProConsentDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.hpo import HPO
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.config import getSettingList, HPO_LITE_AWARDEE
from rdr_service.code_constants import UNSET
from rdr_service.participant_enums import ParticipantSummaryRecord

PTC_ALLOWED_ENVIRONMENTS = [
    'all-of-us-rdr-sandbox',
    'all-of-us-rdr-stable',
    'all-of-us-rdr-ptsc-1-test',
    'localhost'
]


class ParticipantSummaryApi(BaseApi):
    def __init__(self):
        super(ParticipantSummaryApi, self).__init__(ParticipantSummaryDao(), get_returns_children=True)
        self.user_info = None
        self.query_definition = None
        self.site_dao = None

        self.participant_dao = ParticipantDao()
        self.hpro_consent_dao = HealthProConsentDao()
        self.incentives_dao = ParticipantIncentivesDao()

    @auth_required(PTC_HEALTHPRO_AWARDEE_CURATION + [SUPPORT])
    def get(self, p_id=None):
        # Make sure participant id is in the correct range of possible values.
        if isinstance(p_id, int) and not _MIN_ID <= p_id <= _MAX_ID:
            raise NotFound(f"Participant with ID {p_id} is not found.")
        auth_awardee = None
        user_email, user_info = get_validated_user_info()
        self.user_info = user_info

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
            self._filter_by_user_site(participant_id=p_id)
            # self._filter_payload_for_role()

            if any(role in ['healthpro'] for role in self.user_info.get('roles')):
                self._fetch_hpro_consents(pids=p_id)
                self._fetch_participant_incentives(pids=p_id)
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
            return self._query("participantId")

    @auth_required(RDR_AND_PTC)
    @restrict_to_gae_project(PTC_ALLOWED_ENVIRONMENTS)
    def post(self, p_id):
        participant = self.participant_dao.get(p_id)
        if not participant:
            raise NotFound(f"Participant P{p_id} was not found")

        participant_summary = self.dao.get_by_participant_id(p_id)
        if participant_summary:
            raise BadRequest(f"Participant Summary for P{p_id} already exists, updates are not allowed.")

        return super(ParticipantSummaryApi, self).post(p_id)

    def _make_query(self, check_invalid=True):
        constraint_failed, message = self._check_constraints()
        if constraint_failed:
            raise BadRequest(f"{message}")

        self.query_definition = super(ParticipantSummaryApi, self)._make_query(check_invalid)
        self.query_definition.always_return_token = self._get_request_arg_bool("_sync")
        self.query_definition.backfill_sync = self._get_request_arg_bool("_backfill", True)
        self.query_definition.attributes = self._filter_payload_for_role()
        self._filter_by_user_site()
        return self.query_definition

    def _make_response(self, obj):
        filter_payload = True if self.query_definition and self.query_definition.attributes else False
        return self.dao.to_client_json(obj, filter_payload)

    def _make_bundle(self, results, id_field, participant_id):
        if self._get_request_arg_bool("_sync"):
            return make_sync_results_for_request(self.dao, results)
        return super(ParticipantSummaryApi, self)._make_bundle(results, id_field, participant_id)

    def _check_constraints(self):
        message = None
        invalid = False
        valid_roles = ['healthpro']

        if not any(role in valid_roles for role in self.user_info.get('roles')):
            return invalid, message

        pair_config = {
            'lastName': {
                'fields': ['lastName', 'dateOfBirth'],
                'bypass_check_args': ['hpoId']
            },
            'dateOfBirth': {
                'fields': ['lastName', 'dateOfBirth'],
                'bypass_check_args': ['hpoId']
            }
        }

        for arg in request.args:
            if arg in pair_config.keys():
                constraint = pair_config[arg]
                bypass = [val for val in constraint['bypass_check_args'] if val in request.args]
                missing = [val for val in constraint['fields'] if val not in request.args]
                if not bypass and missing:
                    invalid = True
                    message = f'Argument {missing[0]} is required with {arg}'
                    break

        return invalid, message

    def _query(self, id_field, participant_id=None):
        logging.info(f"Preparing query for {self.dao.model_type}.")

        query_definition = self._make_query()
        results = self.dao.query(query_definition)
        participant_ids = [obj.participantId for obj in results.items if hasattr(obj, 'participantId')]

        if any(role in ['healthpro'] for role in self.user_info.get('roles')) and participant_ids:
            self._fetch_hpro_consents(participant_ids)
            self._fetch_participant_incentives(participant_ids)

        logging.info("Query complete, bundling results.")

        # handle
        response = self._make_bundle(results, id_field, participant_id)
        logging.info("Returning response.")

        return response

    def _fetch_hpro_consents(self, pids: Optional[List[int]]):
        self.dao.hpro_consents = self.hpro_consent_dao.get_by_participant(pids)

    def _fetch_participant_incentives(self, pids: Optional[List[int]]):
        self.dao.participant_incentives = self.incentives_dao.get_by_participant(pids)

    def _filter_payload_for_role(self) -> Optional[dict]:
        role_payload_config = config.getSettingJson(config.OPS_DATA_PAYLOAD_ROLES, {})
        if not role_payload_config:
            return

        for role in self.user_info.get('roles'):
            if role in role_payload_config:
                return role_payload_config.get(role)['fields']
        return None

    def _filter_by_user_site(self, participant_id=None):
        if not self.user_info.get('site'):
            return

        user_site = self.user_info.get('site')
        if type(user_site) is list:
            user_site = user_site[0]

        self.site_dao = SiteDao()
        site_obj = self.site_dao.get_by_google_group(user_site)
        if not site_obj:
            raise BadRequest(f"No site found with google group {user_site}, that is attached to request user")

        if not participant_id:
            user_info_site_filter = self.dao.make_query_filter('site', user_site)
            if user_info_site_filter:
                current_site_filter = list(filter(lambda x: x.field_name == 'siteId',
                                                  self.query_definition.field_filters))
                if current_site_filter:
                    self.query_definition.field_filters.remove(current_site_filter[0])
                self.query_definition.field_filters.append(user_info_site_filter)
            return

        participant_summary = self.dao.get_by_participant_id(participant_id)
        if not participant_summary:
            return

        if participant_summary.siteId and \
                participant_summary.siteId != site_obj.siteId:
            raise Forbidden(f"Site attached to the request user, "
                            f"{user_site} is forbidden from accessing this participant")
        return


class ParticipantSummaryModifiedApi(BaseApi):
    """
  API to return participant_id and last_modified fields
  """

    def __init__(self):
        super(ParticipantSummaryModifiedApi, self).__init__(ParticipantSummaryDao())

    @auth_required(PTC_HEALTHPRO_AWARDEE_CURATION)
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
                    {
                        "participantId": "P{0}".format(item.participantId),
                        "lastModified": item.lastModified.isoformat()
                    }
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
