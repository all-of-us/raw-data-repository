from rdr_service.dao.ppsc_dao import PPSCNphOptEventInDao
from rdr_service.dao.study_nph_dao import EligibleParticipantsDao


class NphOptInSync:

    def __init__(self):
        self.eligible_dao = EligibleParticipantsDao()
        self.nph_opt_in_event_dao = PPSCNphOptEventInDao()
        self.usable_nph_objects = None
        self.items_ready_for_sync = []

    def get_items_for_sync(self):
        self.usable_nph_objects = self.eligible_dao.get_usable_participant_data()
        if not self.usable_nph_objects:
            raise RuntimeError(f'No eligible NPH participant data records found')

        self.items_ready_for_sync = self.nph_opt_in_event_dao.get_eligible_participant_records()
        if not self.items_ready_for_sync:
            raise RuntimeError(f'No items for NPH Opt In Sync found')

    def run_sync(self):
        self.get_items_for_sync()
