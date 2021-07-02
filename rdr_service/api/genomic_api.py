from dateutil import parser

from flask import request
from werkzeug.exceptions import NotFound, BadRequest

from rdr_service import clock
from rdr_service import config
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import GEM, RDR_AND_PTC, RDR
from rdr_service.app_util import auth_required, restrict_to_gae_project
from rdr_service.dao.genomics_dao import GenomicPiiDao, GenomicOutreachDao, GenomicOutreachDaoV2

ALLOWED_ENVIRONMENTS = ['all-of-us-rdr-sandbox',
                        'all-of-us-rdr-stable',
                        'all-of-us-rdr-ptsc-1-test',
                        'localhost']


class GenomicPiiApi(BaseApi):
    def __init__(self):
        super(GenomicPiiApi, self).__init__(GenomicPiiDao())

    @auth_required([GEM, RDR])
    def get(self, mode=None, p_id=None):
        if mode not in ('GEM', 'RHP'):
            raise BadRequest("GenomicPII Mode required to be \"GEM\" or \"RHP\".")

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

    @auth_required(RDR_AND_PTC)
    def get(self, mode=None):
        self._check_mode(mode)

        if mode.lower() == "gem":
            return self.get_gem_outreach()

        return BadRequest

    @auth_required(RDR_AND_PTC)
    @restrict_to_gae_project(ALLOWED_ENVIRONMENTS)
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


class GenomicOutreachApiV2(BaseApi):
    def __init__(self):
        super(GenomicOutreachApiV2, self).__init__(GenomicOutreachDaoV2())
        self.validate_params()

    @auth_required(RDR_AND_PTC)
    def get(self):
        if not request.args.get('participant_id'):
            self._check_global_args(
                request.args.get('module'),
                request.args.get('type')
            )
        return self.get_outreach()

    def get_outreach(self):
        """
        Returns the outreach resource based on the request parameters
        :return:
        """
        _start_date = request.args.get("start_date")
        _pid = request.args.get("participant_id")
        _end_date = clock.CLOCK.now() \
            if request.args.get("end_date") is None \
            else parser.parse(request.args.get("end_date"))
        participant_states = []

        if not _pid and not _start_date:
            raise BadRequest('Participant ID or Start Date is required for GenomicOutreach lookup.')

        if _pid:
            if _pid.startswith("P"):
                _pid = _pid[1:]
            participant_states = self.dao.outreach_lookup(pid=_pid)
            if not participant_states:
                raise NotFound(f'Participant P{_pid} does not exist in the Genomic system.')

        if _start_date:
            _start_date = parser.parse(_start_date)
            participant_states = self.dao.outreach_lookup(start_date=_start_date, end_date=_end_date)

        if participant_states:
            payload = {
                'date': clock.CLOCK.now(),
                'data': participant_states
            }

            return self._make_response(payload)

        raise BadRequest

    def _check_global_args(self, module, _type):
        """
        Checks that the mode in the endpoint is valid
        :param module: "GEM" / "RHP" / "PGX" / "HDR"
        :param _type: "result" / "informingLoop" / "appointment"
        """
        current_module = None
        current_type = None

        if module:
            if module.lower() not in config.GENOMIC_API_MODES:
                raise BadRequest(
                    f"GenomicOutreach accepted modules: {' | '.join(config.GENOMIC_API_MODES)}")
            else:
                current_module = module.lower()
        if _type:
            if _type and _type.lower() not in self.dao.allowed_types:
                raise BadRequest(f"GenomicOutreach accepted types: {' | '.join(self.dao.allowed_types)}")
            else:
                current_type = _type

        self.dao.set_globals(
            module=current_module,
            _type=current_type
        )

    @staticmethod
    def validate_params():
        valid_params = ['start_date', 'end_date', 'participant_id', 'module', 'type']
        request_keys = list(request.args.keys())
        if any(arg for arg in request_keys if arg not in valid_params):
            raise BadRequest(f"GenomicOutreach accepted params: {' | '.join(valid_params)}")
