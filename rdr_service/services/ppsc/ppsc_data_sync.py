from abc import ABC, abstractmethod

from rdr_service.dao.organization_hierarchy_sync_dao import OrganizationHierarchySyncDao
from rdr_service.services.genomic_datagen import ParticipantGenerator


class DataSync(ABC):

    def __init__(self):
        self.legacy_site_dao = OrganizationHierarchySyncDao()

    @abstractmethod
    def run_sync(self):
        ...


class SiteDataSync(DataSync):

    def __init__(self, *, site_data: dict):
        super().__init__()
        self.site_data_struct = {}
        self.site_data = site_data

    def add_conditional_site_data(self) -> None:
        ...

    def build_site_data_dict(self) -> None:
        self.site_data_struct['awardee'] = self.legacy_site_dao.update_awardee
        self.site_data_struct['organization'] = self.legacy_site_dao.update_organization
        self.site_data_struct['site'] = self.legacy_site_dao.update_site

    def send_site_data_elements(self) -> None:
        self.build_site_data_dict()
        for update_value in self.site_data_struct.values():
            update_value(self.site_data)

    def run_sync(self):
        self.send_site_data_elements()


class CreateParticipantSync(DataSync):

    def __init__(self, participant_data: dict) -> None:
        super().__init__()
        self.participant_data = participant_data

    def generate_participant_data(self) -> None:
        with ParticipantGenerator(
            project='ppsc_sync'
        ) as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type='ppsc_participant',
                external_values={
                    'participant_id': self.participant_data.get('id'),
                    'biobank_id': self.participant_data.get('biobank_id')
                },
                skip_genomic_member_store=False
            )

    def run_sync(self):
        self.generate_participant_data()
