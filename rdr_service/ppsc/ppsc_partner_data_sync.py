import logging

from rdr_service.dao.ppsc_dao import PPSCNphOptEventInDao
from rdr_service.dao.ppsc_partner_transfer_dao import RTIDataTransferBaseDao
from rdr_service.dao.study_nph_dao import EligibleParticipantsDao
from rdr_service.model.ppsc_partner_data_transfer import RTINphOptIn


class NphOptInSync:

    def __init__(self):
        self.nph_opt_in_dao = RTIDataTransferBaseDao(RTINphOptIn)
        self.eligible_dao = EligibleParticipantsDao()
        self.nph_opt_in_event_dao = PPSCNphOptEventInDao()
        self.usable_nph_objects = None
        self.items_ready_for_sync = []

    def get_items_for_sync(self):
        self.usable_nph_objects = self.eligible_dao.get_usable_participant_data()
        if not self.usable_nph_objects:
            logging.warning('No eligible NPH participant data records found')
            return

        self.items_ready_for_sync = self.nph_opt_in_event_dao.get_eligible_participant_records()
        if not self.items_ready_for_sync:
            logging.info('No NPH Opt In records for sync found')
            return

        logging.info(f'Syncing {len(self.items_ready_for_sync)} for NPH Opt In Sync')

    def get_nph_obj_from_list(self):
        current_nph_obj = self.usable_nph_objects[0]
        return current_nph_obj

    def sync_items(self):
        self.get_items_for_sync()
        for item in self.items_ready_for_sync:
            usable_nph_obj = self.get_nph_obj_from_list()
            self.nph_opt_in_dao.insert(self.nph_opt_in_dao.model_type(**{
                'nph_participant_id': usable_nph_obj.participant_id,
                'first_name': item.first_name,
                'last_name': item.last_name,
                'email': item.email,
                'phone': item.phone,
                'zip_code': item.zip_code,
                'language_preference': 1 if item.language_preference.lower() == 'english' else '2'
            }))
            self.eligible_dao.update(self.eligible_dao.model_type(**{
                'id': usable_nph_obj.id,
                'primary_participant_id': item.participant_id,
                'active': 1
            }))
            self.usable_nph_objects.pop(0)

    def run_sync(self):
        self.sync_items()
        logging.info(f'{len(self.items_ready_for_sync)} objects have been synced for NPH Opt In')
