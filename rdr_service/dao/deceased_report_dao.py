import pytz
from werkzeug.exceptions import BadRequest, NotFound, Conflict

from rdr_service.api_util import parse_date
from rdr_service.dao.api_user_dao import ApiUserDao
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportDenialReason, DeceasedReportStatus


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
    def _read_authored_timestamp(resource):
        if 'issued' not in resource:
            raise BadRequest('Report issued date is required')
        return parse_date(resource['issued'])

    # pylint: disable=unused-argument
    def from_client_json(self, resource, participant_id, id_=None, expected_version=None, client_id=None):
        status = self._read_report_status(resource)

        if id_ is None:
            report = DeceasedReport(participantId=participant_id)

            if status != DeceasedReportStatus.PENDING:
                raise BadRequest('Status field should be "preliminary" when creating deceased report')

            # Should auto-approve reports for unpaired participants
            with self.session() as session:
                participant = session.query(Participant).filter(
                    Participant.participantId == participant_id
                ).one_or_none()
                if participant is None:
                    raise NotFound(f'Participant P{participant_id} not found.')
                elif participant.hpoId == 0:  # Unpaired participant
                    status = DeceasedReportStatus.APPROVED

            if 'encounter' not in resource:
                raise BadRequest('Encounter information required for deceased report')
            report.notification = getattr(DeceasedNotification, resource['encounter']['reference'])
            if report.notification == DeceasedNotification.OTHER:
                if 'display' not in resource['encounter']:
                    raise BadRequest('Encounter description required when OTHER is set')
                report.notificationOther = resource['encounter']['display']

            if report.notification in [DeceasedNotification.ATTEMPTED_CONTACT,
                                       DeceasedNotification.NEXT_KIN_HPO,
                                       DeceasedNotification.NEXT_KIN_SUPPORT]:
                if 'extension' not in resource:
                    raise BadRequest('Missing report extension')
                extension = resource['extension']
                if not isinstance(extension, list) or len(extension) != 1:
                    raise BadRequest('Invalid report extension')
                first_extension = extension[0]
                if 'valueHumanName' not in first_extension:
                    raise BadRequest('Missing reporter data')
                reporter_data = first_extension['valueHumanName']
                if 'text' not in reporter_data:
                    raise BadRequest('Missing reporter name')
                report.reporterName = reporter_data['text']

                for extension in reporter_data['extension']:
                    if extension['url'] == 'https://www.pmi-ops.org/association':
                        report.reporterRelationship = extension['valueCode']
                    elif extension['url'] == 'https://www.pmi-ops.org/email-address':
                        report.reporterEmail = extension['valueString']
                    elif extension['url'] == 'https://www.pmi-ops.org/phone-number':
                        report.reporterPhone = extension['valueString']

                if report.reporterRelationship is None:
                    raise BadRequest('Reporter association is required')

            if 'performer' not in resource:
                raise BadRequest('Performer user information is required for submitting a report')
            api_user_dao = ApiUserDao()
            report.author = api_user_dao.load_or_init_from_client_json(resource['performer'])

            report.authored = self._read_authored_timestamp(resource)
        else:
            with self.session() as session:
                report = session.query(DeceasedReport).filter(DeceasedReport.id == id_).one_or_none()
            if report is None:
                raise NotFound(f'DeceasedReport with id "{id_}" not found')

            if 'performer' not in resource:
                raise BadRequest('Performer user information is required for reviewing a report')
            api_user_dao = ApiUserDao()
            report.reviewer = api_user_dao.load_or_init_from_client_json(resource['performer'])
            report.reviewed = self._read_authored_timestamp(resource)

            if status == DeceasedReportStatus.DENIED:
                denial_data = resource['extension'][0]['valueReference']
                report.denialReason = DeceasedReportDenialReason(denial_data['reference'])

                if report.denialReason == DeceasedReportDenialReason.OTHER:
                    report.denialReasonOther = denial_data['display']

            if report.status != DeceasedReportStatus.PENDING:
                raise BadRequest('Can only approve or deny a PENDING deceased report')

        report.status = status

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

        encounter_json = {
            'reference': str(model.notification)
        }
        if model.notification == DeceasedNotification.OTHER:
            encounter_json['display'] = model.notificationOther
        json['encounter'] = encounter_json

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

            json['extension'] = [{
                'url': 'https://www.pmi-ops.org/deceased-reporter',
                'valueHumanName': {
                    'text': model.reporterName,
                    'extension': reporter_extensions
                }
            }]

        return json

    def insert_with_session(self, session, obj: DeceasedReport):
        existing_reports = session.query(DeceasedReport).filter(DeceasedReport.participantId == obj.participantId)
        if any([report.status != DeceasedReportStatus.DENIED for report in existing_reports]):
            raise Conflict(f'Participant P{obj.participantId} already has a preliminary or approved deceased report')

        return super(DeceasedReportDao, self).insert_with_session(session, obj)

    def get_id(self, obj: DeceasedReport):
        return obj.id

    def get_etag(self, id_, participant_id):  # pylint: disable=unused-argument
        return None
