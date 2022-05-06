import pytz
from dateutil import parser

from flask import request
from werkzeug.exceptions import NotFound, BadRequest

from rdr_service import clock, config
from rdr_service.api.base_api import BaseApi, log_api_request, UpdatableApi
from rdr_service.api_util import GEM, RDR_AND_PTC, RDR
from rdr_service.app_util import auth_required, restrict_to_gae_project
from rdr_service.dao.genomics_dao import GenomicPiiDao, GenomicSetMemberDao, GenomicOutreachDao, GenomicOutreachDaoV2
from rdr_service.dao.participant_dao import ParticipantDao

PTC_ALLOWED_ENVIRONMENTS = [
    'all-of-us-rdr-sandbox',
    'all-of-us-rdr-stable',
    'all-of-us-rdr-ptsc-1-test',
    'localhost'
]


class GenomicPiiApi(BaseApi):
    def __init__(self):
        super(GenomicPiiApi, self).__init__(GenomicPiiDao())

    @auth_required([GEM, RDR])
    def get(self, mode=None, p_id=None):
        if mode not in ('GP', 'RHP'):
            raise BadRequest("GenomicPII Mode required to be \"GP\" or \"RHP\".")

        if p_id is not None:
            pii = self.dao.get_by_pid(p_id)

            if not pii:
                raise NotFound(f"Participant with ID {p_id} not found")

            proto_payload = {
                'mode': mode,
                'data': pii
            }

            return self._make_response(proto_payload)

        raise BadRequest


class GenomicOutreachApi(BaseApi):
    def __init__(self):
        super(GenomicOutreachApi, self).__init__(GenomicOutreachDao())
        self.member_dao = GenomicSetMemberDao()

    @auth_required([GEM] + RDR_AND_PTC)
    def get(self, mode=None):
        self._check_mode(mode)

        if mode.lower() == "gem":
            return self.get_gem_outreach()

        return BadRequest

    @auth_required(RDR_AND_PTC)
    @restrict_to_gae_project(PTC_ALLOWED_ENVIRONMENTS)
    def post(self, p_id, mode=None):
        """
        Generates a genomic test participant from payload
        Overwrites BaseAPI.post()
        :param p_id:
        :param mode:
        :return:
        """
        self._check_mode(mode)

        if mode.lower() == "gem":
            return self.post_gem_outreach(p_id)

        return BadRequest

    def get_gem_outreach(self):
        """
        Returns the GEM outreach resource based on the request parameters
        :return:
        """
        _start_date = request.args.get("start_date")
        _end_date = request.args.get("end_date")

        _pid = request.args.get("participant_id")

        if _pid is not None and _start_date is not None:
            raise BadRequest('Start date not supported with participant lookup.')

        # Set the return timestamp
        if _end_date is None:
            _end_date = clock.CLOCK.now()
        else:
            _end_date = parser.parse(_end_date)

        participant_report_states = None

        # If this is a participant lookup
        if _pid is not None:
            if _pid.startswith("P"):
                _pid = _pid[1:]

            participant_report_states = self.dao.participant_state_lookup(_pid)

            if len(participant_report_states) == 0:
                raise NotFound(f'Participant P{_pid} does not exist in the Genomic system.')

        # If this is a date lookup
        if _start_date is not None:
            _start_date = parser.parse(_start_date)

            participant_report_states = self.dao.date_lookup(_start_date, end_date=_end_date)

        if participant_report_states is not None:
            proto_payload = {
                'date': clock.CLOCK.now(),
                'data': participant_report_states
            }

            return self._make_response(proto_payload)

        return BadRequest

    def post_gem_outreach(self, p_id):
        """
        Creates the genomic participant
        :return: response
        """
        resource = request.get_json(force=True)

        # Create GenomicSetMember with report state
        model = self.dao.from_client_json(resource, participant_id=p_id, mode='gem')
        m = self._do_insert(model)
        self.member_dao.update_member_wf_states(m)

        response_data = {
            'date': m.genomicWorkflowStateModifiedTime,
            'data': [
                (m.participantId, m.genomicWorkflowState),
            ]
        }

        # Log to requests_log
        log_api_request(log=request.log_record)
        return self._make_response(response_data)

    @staticmethod
    def _check_mode(mode):
        """
        Checks that the mode in the endpoint is valid
        :param mode: "GEM" or "RHP"
        """
        modes = ['gem', 'rhp']
        if mode.lower() not in modes:
            raise BadRequest(f"GenomicOutreach Mode required to be one of {modes}.")


class GenomicOutreachApiV2(UpdatableApi):
    def __init__(self):
        super(GenomicOutreachApiV2, self).__init__(GenomicOutreachDaoV2())
        self.validate_params()

    @auth_required(RDR_AND_PTC)
    def get(self):
        self._check_global_args(
            request.args.get('module'),
            request.args.get('type')
        )
        return self.get_outreach()

    @auth_required(RDR_AND_PTC)
    @restrict_to_gae_project(PTC_ALLOWED_ENVIRONMENTS)
    def post(self):
        participant_id, request_data = self.validate_post_data()
        return self.set_ready_loop(
            participant_id,
            request_data
        )

    @auth_required(RDR_AND_PTC)
    @restrict_to_gae_project(PTC_ALLOWED_ENVIRONMENTS)
    def put(self):
        participant_id, request_data = self.validate_post_data()
        return self.set_ready_loop(
            participant_id,
            request_data
        )

    def get_outreach(self):
        """
        Returns the outreach resource based on the request parameters
        :return:
        """
        start_date = request.args.get("start_date", None)
        pid = request.args.get("participant_id", None)
        end_date = clock.CLOCK.now() \
            if not request.args.get("end_date") \
            else parser.parse(request.args.get("end_date"))

        payload = {
            'date': clock.CLOCK.now()
        }

        if not pid and not start_date:
            raise BadRequest('Participant ID or Start Date is required for GenomicOutreach lookup.')

        if pid:
            if pid.startswith("P"):
                pid = pid[1:]
            participant_data = self.dao.outreach_lookup(pid=pid)
            if participant_data:
                payload['data'] = participant_data
                return self._make_response(payload)

            raise NotFound(f'Participant P{pid} does not exist in the Genomic system.')

        if start_date:
            start_date = parser.parse(start_date)
            participant_data = self.dao.outreach_lookup(start_date=start_date, end_date=end_date)
            payload['data'] = participant_data
            return self._make_response(payload)

        raise BadRequest

    @staticmethod
    def set_ready_loop(participant_id, req_data):
        member_dao = GenomicSetMemberDao()

        def _build_ready_response():
            report_statuses = []
            ready_loop = member_dao.get_ready_loop_by_participant_id(participant_id)
            if ready_loop:
                for module in ['hdr', 'pgx']:
                    report_statuses.append({
                        "module": module,
                        "type": 'informingLoop',
                        "status": 'ready',
                        "participant_id": f'P{participant_id}',
                    })

            return {
                "data": report_statuses,
                "timestamp": pytz.utc.localize(clock.CLOCK.now())
            }

        convert_bool_map = {
            'yes': 1,
            'no': 0
        }

        if request.method == 'PUT':
            current_member = member_dao.get_member_by_participant_id(
                participant_id,
                genome_type=config.GENOME_TYPE_WGS
            )
            if not current_member:
                raise NotFound(f'Participant with id P{participant_id} was not found in genomics system')

            member_dao.update_loop_ready_attrs(
                current_member,
                informing_loop_ready_flag=convert_bool_map[
                        req_data['informing_loop_eligible'].lower()
                    ],
                informing_loop_ready_flag_modified=parser.parse(
                    req_data['eligibility_date_utc'])
            )

            log_api_request(log=request.log_record)
            return _build_ready_response()

        log_api_request(log=request.log_record)
        return _build_ready_response()

    def _check_global_args(self, module, req_type):
        """
        Checks that the mode in the endpoint is valid
        :param module: "GEM" / "PGX" / "HDR"
        :param req_type: "result" / "informingLoop" / "appointment"
        """
        current_module, current_type = None, None

        if module:
            if module.lower() not in self.dao.allowed_modules:
                raise BadRequest(
                    f"GenomicOutreachV2 GET accepted modules: {' | '.join(self.dao.allowed_modules)}")
            current_module = module.lower()
        if req_type:
            if req_type not in self.dao.req_allowed_types:
                raise BadRequest(f"GenomicOutreachV2 GET accepted types: {' | '.join(self.dao.req_allowed_types)}")
            current_type = req_type

        self.dao.set_globals(
            module=current_module,
            req_type=current_type
        )

    @staticmethod
    def validate_post_data():
        request_data = request.get_json()
        request_args = request.args

        if not request_data or not request_args:
            raise BadRequest(f"Missing request data/params in {request.method}")

        participant_id = request.args.get("participant_id", None)

        if not participant_id:
            raise BadRequest(f"Missing participant id {request.method} params")

        participant_dao = ParticipantDao()
        participant_id = participant_id[1:] if participant_id.startswith("P") else participant_id
        participant = participant_dao.get(int(participant_id))

        if not participant:
            raise NotFound(f"Participant with id P{participant_id} was not found")

        return participant_id, request_data

    @staticmethod
    def validate_params():
        def _check_error(*,
                         params_sent,
                         accepted_params,
                         message
                         ):
            if any(arg for arg in params_sent if arg not in accepted_params):
                raise BadRequest(f"{message}: {' | '.join(accepted_params)}")

        if request.method == 'GET':
            valid_params = ['start_date', 'end_date', 'participant_id', 'module', 'type']
            request_keys = list(request.args.keys())
            return _check_error(
                params_sent=request_keys,
                accepted_params=valid_params,
                message='GenomicOutreachV2 GET accepted params'
            )
        if request.method in ['POST', 'PUT']:
            valid_params = ['informing_loop_eligible', 'eligibility_date_utc', 'participant_id']
            request_keys = list(request.get_json().keys())
            request_keys += list(request.args.keys())
            return _check_error(
                params_sent=request_keys,
                accepted_params=valid_params,
                message=f'GenomicOutreachV2 {request.method} accepted data/params'
            )
