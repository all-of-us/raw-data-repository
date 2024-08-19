
from rdr_service.dao.organization_hierarchy_sync_dao import OrganizationHierarchySyncDao


class SiteDataSync:

    def __init__(self, *, site_data: dict):
        self.legacy_site_dao = OrganizationHierarchySyncDao()
        self.site_data_struct = {}
        self.site_data = site_data

    def build_site_data_dict(self) -> None:
        self.site_data_struct['awardee'] = self.legacy_site_dao.update_awardee
        self.site_data_struct['organization'] = self.legacy_site_dao.update_organization
        self.site_data_struct['site'] = self.legacy_site_dao.update_site

    def send_site_data_elements(self) -> None:
        self.build_site_data_dict()
        for update_value in self.site_data_struct.values():
            update_value(self.site_data)

    def run_site_sync(self):
        self.send_site_data_elements()
