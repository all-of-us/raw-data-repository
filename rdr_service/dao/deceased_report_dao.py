import pytz
from werkzeug.exceptions import BadRequest, NotFound, Conflict

from rdr_service.api_util import parse_date
from rdr_service.dao.api_user_dao import ApiUserDao
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportDenialReason, DeceasedReportStatus,\
    DeceasedStatus


class DeceasedReportDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(DeceasedReport)

    @staticmethod
    def _read_report_status(resource):
        if 'status' not in resource:
            raise BadRequest('Missing required field: status')

        status_map = {
            'preliminary': DeceasedReportStatus.PENDING,
            'final': DeceasedReportStatus.APPROVED,
            'cancelled': DeceasedReportStatus.DENIED
        }
        status_string = resource['status']
        if status_string not in status_map:
            raise BadRequest(f'Invalid status "{status_string}"')

        return status_map[status_string]

    @staticmethod
    def _read_api_request_author(resource):
        if 'performer' not in resource:
            raise BadRequest('Performer user information is required for submitting a report')
        api_user_dao = ApiUserDao()
        return api_user_dao.load_or_init_from_client_json(resource['performer'])

    @staticmethod
    def _read_authored_timestamp(resource):
        if 'issued' not in resource:
            raise BadRequest('Report issued date is required')
        return parse_date(resource['issued'])

    @staticmethod
    def _read_single_extension(resource):
        if 'extension' not in resource:
            raise BadRequest('Missing required extension')
        extension = resource['extension']
        if not isinstance(extension, list) or len(extension) != 1:
            raise BadRequest('Invalid report extension')
        return extension[0]

    @staticmethod
    def _read_encounter(resource, report):  # Get notification data
        if 'encounter' not in resource:
            raise BadRequest('Encounter information required for deceased report')
        encounter_data = resource['encounter']
        if 'reference' not in encounter_data:
            raise BadRequest('Invalid encounter information')

        report.notification = DeceasedNotification(encounter_data['reference'])
        if report.notification == DeceasedNotification.OTHER:
            if 'display' not in encounter_data:
                raise BadRequest('Encounter display description text required when OTHER is set')
            report.notificationOther = encounter_data['display']

    def _read_reporter_extension(self, resource, report):
        extension = self._read_single_extension(resource)
        if 'valueHumanName' not in extension:
            raise BadRequest('Reporter data missing from extension')
        reporter_data = extension['valueHumanName']
        if 'text' not in reporter_data:
            raise BadRequest('Missing reporter name')
        report.reporterName = reporter_data['text']

        if 'extension' not in reporter_data:
            raise BadRequest('Missing reporter extensions')
        reporter_extensions = reporter_data['extension']
        if not isinstance(reporter_extensions, list):
            raise BadRequest('Invalid reporter extensions')
        for reporter_extension in reporter_extensions:
            if reporter_extension['url'] == 'https://www.pmi-ops.org/association':
                report.reporterRelationship = reporter_extension['valueCode']
            elif reporter_extension['url'] == 'https://www.pmi-ops.org/email-address':
                report.reporterEmail = reporter_extension['valueString']
            elif reporter_extension['url'] == 'https://www.pmi-ops.org/phone-number':
                report.reporterPhone = reporter_extension['valueString']

        if report.reporterRelationship is None:  # If this is unset still then it must have not been provided
            raise BadRequest('Reporter association is required')

    def _read_denial_extension(self, resource, report):
        extension = self._read_single_extension(resource)
        if 'valueReference' not in extension:
            raise BadRequest('Invalid report denial extension')

        denial_data = extension['valueReference']
        report.denialReason = DeceasedReportDenialReason(denial_data['reference'])

        if report.denialReason == DeceasedReportDenialReason.OTHER:
            report.denialReasonOther = denial_data['display']

    def _load_participant(self, participant_id):
        with self.session() as session:
            participant = session.query(Participant).filter(
                Participant.participantId == participant_id
            ).one_or_none()
            if participant is None:
                raise NotFound(f'Participant P{participant_id} not found.')
            return participant

    @staticmethod
    def _update_participant_summary(session, report: DeceasedReport):
        participant_summary = session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == report.participantId
        ).one_or_none()
        if participant_summary:
            if report.status == DeceasedReportStatus.DENIED:
                participant_summary.deceasedStatus = DeceasedStatus.UNSET
                participant_summary.deceasedAuthored = None
                participant_summary.dateOfDeath = None
            else:
                participant_summary.deceasedStatus = DeceasedStatus(str(report.status))
                participant_summary.dateOfDeath = report.dateOfDeath

                if report.status == DeceasedReportStatus.APPROVED:
                    participant_summary.deceasedAuthored = report.reviewed
                else:
                    participant_summary.deceasedAuthored = report.authored

    def load_model(self, id_):
        with self.session() as session:
            report = session.query(DeceasedReport).filter(DeceasedReport.id == id_).one_or_none()
        if report is None:
            raise NotFound(f'DeceasedReport with id "{id_}" not found')
        else:
            return report

    # pylint: disable=unused-argument
    def from_client_json(self, resource, participant_id, id_=None, expected_version=None, client_id=None):
        requested_report_status = self._read_report_status(resource)

        if id_ is None:  # No report was referenced with the request, so the request is to create a new one
            if requested_report_status != DeceasedReportStatus.PENDING:
                raise BadRequest('Status field should be "preliminary" when creating deceased report')
            report = DeceasedReport(participantId=participant_id)

            # Should auto-approve reports for unpaired participants
            participant = self._load_participant(participant_id)
            if participant.hpoId == 0:
                requested_report_status = DeceasedReportStatus.APPROVED
                report.reviewed = self._read_authored_timestamp(resource)

            self._read_encounter(resource, report)

            if report.notification in [DeceasedNotification.ATTEMPTED_CONTACT,
                                       DeceasedNotification.NEXT_KIN_HPO,
                                       DeceasedNotification.NEXT_KIN_SUPPORT]:
                self._read_reporter_extension(resource, report)

            report.author = self._read_api_request_author(resource)
            report.authored = self._read_authored_timestamp(resource)
        else:
            report = self.load_model(id_)
            if report.status != DeceasedReportStatus.PENDING:
                raise BadRequest('Can only approve or deny a PENDING deceased report')

            report.reviewer = self._read_api_request_author(resource)
            report.reviewed = self._read_authored_timestamp(resource)

            if requested_report_status == DeceasedReportStatus.DENIED:
                self._read_denial_extension(resource, report)

        report.status = requested_report_status

        if 'effectiveDateTime' in resource:
            report.dateOfDeath = parse_date(resource['effectiveDateTime'])

        return report

    def to_client_json(self, model: DeceasedReport):
        status_map = {
            DeceasedReportStatus.PENDING: 'preliminary',
            DeceasedReportStatus.APPROVED: 'final',
            DeceasedReportStatus.DENIED: 'cancelled'
        }
        api_user_dao = ApiUserDao()

        authored_timestamp = pytz.utc.localize(model.authored)
        json = {
            'code': {
                'text': 'DeceasedReport'
            },
            'identifier': {
                'value': model.id
            },
            'status': status_map[model.status],
            'performer': api_user_dao.to_client_json(model.author),
            'issued': authored_timestamp.isoformat()
        }

        if model.dateOfDeath:
            json['effectiveDateTime'] = model.dateOfDeath.strftime('%Y-%m-%d')

        # Add notification data
        encounter_json = {
            'reference': str(model.notification)
        }
        if model.notification == DeceasedNotification.OTHER:
            encounter_json['display'] = model.notificationOther
        json['encounter'] = encounter_json

        report_extensions = []
        # Add reporter data
        if model.notification in [DeceasedNotification.ATTEMPTED_CONTACT,
                                  DeceasedNotification.NEXT_KIN_HPO,
                                  DeceasedNotification.NEXT_KIN_SUPPORT]:
            reporter_extensions = [{
                'url': 'https://www.pmi-ops.org/association',
                'valueCode': model.reporterRelationship
            }]
            if model.reporterPhone:
                reporter_extensions.append({
                    'url': 'https://www.pmi-ops.org/phone-number',
                    'valueString': model.reporterPhone
                })
            if model.reporterEmail:
                reporter_extensions.append({
                    'url': 'https://www.pmi-ops.org/email-address',
                    'valueString': model.reporterEmail
                })

            report_extensions.append({
                'url': 'https://www.pmi-ops.org/deceased-reporter',
                'valueHumanName': {
                    'text': model.reporterName,
                    'extension': reporter_extensions
                }
            })

        # Add denial reason extension
        if model.status == DeceasedReportStatus.DENIED:
            denial_reason_reference = {
                'reference': str(model.denialReason)
            }
            if model.denialReason == DeceasedReportDenialReason.OTHER:
                denial_reason_reference['display'] = model.denialReasonOther

            report_extensions.append({
                'url': 'https://www.pmi-ops.org/observation-denial-reason',
                'valueReference': denial_reason_reference
            })

        if report_extensions:
            json['extension'] = report_extensions

        return json

    def insert_with_session(self, session, obj: DeceasedReport):
        existing_reports = session.query(DeceasedReport).filter(DeceasedReport.participantId == obj.participantId)
        if any([report.status != DeceasedReportStatus.DENIED for report in existing_reports]):
            raise Conflict(f'Participant P{obj.participantId} already has a preliminary or approved deceased report')

        self._update_participant_summary(session, obj)
        return super(DeceasedReportDao, self).insert_with_session(session, obj)

    def update_with_session(self, session, obj: DeceasedReport):
        self._update_participant_summary(session, obj)
        return super(DeceasedReportDao, self).update_with_session(session, obj)

    def get_id(self, obj: DeceasedReport):
        return obj.id

    def get_etag(self, id_, participant_id):  # pylint: disable=unused-argument
        return None
