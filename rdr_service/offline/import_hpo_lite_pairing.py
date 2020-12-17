from datetime import datetime, timedelta
from dateutil import parser
import logging
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import HTTPException
from copy import deepcopy

from rdr_service.clock import CLOCK
from rdr_service.dao.api_user_dao import ApiUserDao
from rdr_service.model.hpo_lite_pairing_import_record import HpoLitePairingImportRecord
from rdr_service.model.participant import Participant
from rdr_service.model.hpo import HPO
from rdr_service.model.utils import from_client_participant_id
from rdr_service.services.redcap_client import RedcapClient
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service import config

USER_SYSTEM = 'https://www.pmi-ops.org/redcap'


class HpoLitePairingImporter:
    """Functionality for importing hpo lite pairing info from REDCap"""

    def __init__(self):
        self.redcap_api_key = config.getSettingJson(config.HPO_LITE_REDCAP_PROJECT_TOKEN)
        self.base_dao = BaseDao(HpoLitePairingImportRecord)
        self.hpo_dao = HPODao()
        self.org_dao = OrganizationDao()
        self.api_user_dao = ApiUserDao()
        self.participant_dao = ParticipantDao()

    @staticmethod
    def _find_latest_import_record(participant_id, session):
        return session.query(HpoLitePairingImportRecord)\
            .filter(HpoLitePairingImportRecord.participantId == participant_id)\
            .order_by(HpoLitePairingImportRecord.pairedDate.desc()).first()

    @staticmethod
    def _is_hpo_unset(session, participant_id):
        query = (
            session.query(HPO.name).filter(Participant.participantId == participant_id, HPO.hpoId == Participant.hpoId)
        )
        result = query.first()

        if result and result.name == 'UNSET':
            return True
        else:
            return False

    def _update_pairing_with_session(self, session, participant_id, hpo_id, org_id):
        existing_obj = self.participant_dao.get_for_update(session, participant_id)

        new_obj = deepcopy(existing_obj)
        new_obj.hpoId = hpo_id
        new_obj.organizationId = org_id

        self.participant_dao.update_with_session(session, new_obj)

    def import_pairing_data(self, since: datetime = None):
        """
        :param since: DateTime to use as start of date range request. Will import all records created or modified
            after the given date. Defaults to the start (midnight) of yesterday.
        """
        if since is None:
            now_yesterday = datetime.now() - timedelta(days=1)
            since = datetime(now_yesterday.year, now_yesterday.month, now_yesterday.day)

        redcap = RedcapClient()
        records = redcap.get_records(self.redcap_api_key, since)

        with self.base_dao.session() as session:
            for record in records:
                try:
                    if record.get('hpo_lite_pairing_complete') != '2':
                        continue

                    participant_id = from_client_participant_id(record.get('recordid'))

                    org_name_mapping = config.getSettingJson(config.HPO_LITE_ORG_NAME_MAPPING)
                    org_external_id = org_name_mapping.get(record.get('hpo_name'), None)
                    org = self.org_dao.get_by_external_id(org_external_id)
                    if not org:
                        logging.error(f'Organization {org_external_id} not found for {participant_id}')
                        continue

                    upload_user_email = record.get('user_email')
                    if upload_user_email and upload_user_email != '':
                        upload_user = self.api_user_dao.load_or_init(USER_SYSTEM, upload_user_email)

                    pair_date_str = record.get('paired_date')
                    if pair_date_str:
                        pair_date = parser.parse(pair_date_str)

                    import_record = self._find_latest_import_record(participant_id, session)

                    if import_record and import_record.orgId == org.organizationId:
                        continue

                    if not self._is_hpo_unset(session, participant_id):
                        logging.error(f'{participant_id} is not exist or already paired with other HPO')
                        continue

                    # update participant pair information
                    self._update_pairing_with_session(session, participant_id, org.hpoId, org.organizationId)

                    # save the import record
                    import_record = HpoLitePairingImportRecord(
                        participantId=participant_id,
                        pairedDate=pair_date,
                        orgId=org.organizationId,
                        created=CLOCK.now()
                    )
                    import_record.uploadingUser = upload_user

                    self.base_dao.insert_with_session(session, import_record)
                    session.commit()

                except IntegrityError:
                    session.rollback()
                    logging.error(f'Record for {participant_id} encountered a database error', exc_info=True)
                except (HTTPException, KeyError, ValueError):
                    logging.error(f'Record for {participant_id} encountered an error', exc_info=True)
