import uuid

from rdr_service.dao.organization_hierarchy_sync_dao import OrganizationHierarchySyncDao


class SiteDataSync:

    def __init__(self, *, site_data: dict):
        self.legacy_site_dao = OrganizationHierarchySyncDao()
        self.site_data_struct = {}
        self.site_data = site_data

    # remove when payload confirmed
    def generate_append_resource_id(self) -> None:
        resource_id = uuid.uuid4()
        self.site_data['resource_id'] = resource_id

    # remove when payload confirmed
    def add_temp_els(self):
        self.site_data['awardee_name'] = 'Test Display Name'
        self.site_data['awardee_type'] = 'HPO'
        self.site_data['organization_name'] = 'The organization name'

    def build_site_data_dict(self) -> None:
        self.generate_append_resource_id() # remove when payload confirmed
        self.add_temp_els() # remove when payload confirmed
        self.site_data_struct['awardee'] = self.legacy_site_dao.update_awardee
        self.site_data_struct['organization'] = self.legacy_site_dao.update_organization
        self.site_data_struct['site'] = self.legacy_site_dao.update_site

    def send_site_data_elements(self) -> None:
        self.build_site_data_dict()
        for update_value in self.site_data_struct.values():
            update_value(self.site_data)

    def run_site_sync(self):
        self.send_site_data_elements()
