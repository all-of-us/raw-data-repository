from datetime import datetime, timedelta
from dateutil import parser
import logging
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import BadRequest, HTTPException

from rdr_service.clock import CLOCK
from rdr_service.dao.api_user_dao import ApiUserDao
from rdr_service.dao.deceased_report_dao import DeceasedReportDao
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.deceased_report_import_record import DeceasedReportImportRecord
from rdr_service.model.utils import from_client_participant_id
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportStatus
from rdr_service.services.redcap_client import RedcapClient

PROJECT_TOKEN_CONFIG_KEY = 'dr_import_key'
USER_SYSTEM = 'https://www.pmi-ops.org/redcap'


class DeceasedReportImporter:
    """Functionality for importing deceased reports from REDCap"""

    def __init__(self, config):
        """
        :param config: Dictionary of config data
        """
        self.redcap_api_key = config[PROJECT_TOKEN_CONFIG_KEY]

        self.api_user_dao = ApiUserDao()
        self.deceased_report_dao = DeceasedReportDao()

    def _parse_other_type_report(self, record: dict, report: DeceasedReport):
        report.notification = DeceasedNotification.OTHER
        report.notificationOther = 'HPO contacted support center before Sept. 2020'

        reporter_email = record.get('reportperson_email')
        if not reporter_email:
            reporter_email = 'scstaff@pmi-ops.org'
        report.author = self.api_user_dao.load_or_init(USER_SYSTEM, reporter_email)

    def _parse_kin_type_report(self, record, report: DeceasedReport):
        report.author = self.api_user_dao.load_or_init(USER_SYSTEM, 'scstaff@pmi-ops.org')

        report.notification = DeceasedNotification.NEXT_KIN_SUPPORT

        first_name = record['reportperson_firstname']
        last_name = record['reportperson_lastname']
        if (not first_name) or (not last_name):
            raise BadRequest('Missing reporter name')
        report.reporterName = f"{first_name} {last_name}"
        relationship_map = {
            '1': 'PRN',
            '2': 'CHILD',
            '3': 'SIB',
            '4': 'SPS',
            '5': 'O'
        }
        provided_relationship = record['reportperson_relationship']
        report.reporterRelationship = relationship_map[provided_relationship]

        report.reporterEmail = record.get('reportperson_email')
        report.reporterPhone = record.get('reportperson_phone')

    @staticmethod
    def _get_report_authored_data(record):
        provided_date_of_report_str = record.get('reportdeath_date')
        if provided_date_of_report_str:
            return parser.parse(provided_date_of_report_str)
        else:
            return CLOCK.now()

    @staticmethod
    def _retrieve_import_record(participant_id, session):
        import_record = session.query(DeceasedReportImportRecord).filter(
            DeceasedReportImportRecord.participantId == participant_id
        ).one_or_none()

        if not import_record:
            import_record = DeceasedReportImportRecord(
                participantId=participant_id,
                created=CLOCK.now()
            )
            session.add(import_record)

        import_record.lastSeen = CLOCK.now()
        session.commit()

        return import_record

    def import_reports(self, since: datetime = None):
        """
        :param since: DateTime to use as start of date range request. Will import all reports created or modified
            after the given date. Defaults to the start (midnight) of yesterday.
        """
        if since is None:
            now_yesterday = datetime.now() - timedelta(days=1)
            since = datetime(now_yesterday.year, now_yesterday.month, now_yesterday.day)

        redcap = RedcapClient()
        records = redcap.get_records(self.redcap_api_key, since)

        with self.deceased_report_dao.session() as session:
            for record in records:
                participant_id = None
                try:
                    participant_id = from_client_participant_id(record['recordid'])
                    import_record = self._retrieve_import_record(participant_id, session)

                    if import_record.deceasedReport:
                        logging.warning(
                            f'Skipping record for {participant_id} since deceased report has already been generated"'
                        )
                        continue

                    if record['reportdeath_identityconfirm'] == 0:
                        # Skip any records that don't have the participant's identity confirmed
                        continue

                    report = DeceasedReport(
                        status=DeceasedReportStatus.PENDING,  # auto-approved in DAO if needed
                        participantId=participant_id,
                        causeOfDeath=record.get('death_cause')
                    )

                    notification_type = record['reportperson_type']
                    if notification_type == '1':
                        self._parse_other_type_report(record, report)
                    elif notification_type == '2':
                        self._parse_kin_type_report(record, report)
                    else:
                        logging.error(
                            f'Record for {participant_id} has an unrecognized notification value: "{notification_type}"'
                        )
                        continue

                    date_of_death_str = record.get('death_date')
                    if date_of_death_str:
                        report.dateOfDeath = parser.parse(date_of_death_str)

                    report.authored = self._get_report_authored_data(record)

                    self.deceased_report_dao.insert_with_session(session, report)

                    # Need to set the deceased report on the record after inserting to avoid the sessions conflicting
                    import_record.deceasedReport = report
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    logging.error(f'Record for {participant_id} encountered a database error', exc_info=True)
                except (HTTPException, KeyError, ValueError):
                    logging.error(f'Record for {participant_id} encountered an error', exc_info=True)

        logging.info('Deceased report import complete')
