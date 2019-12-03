from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.workbench_researcher import (
    WorkbenchResearcher
)


class WorkbenchResearcherDao(UpdatableDao):
    def __init__(self):
        super(WorkbenchResearcherDao, self).__init__(WorkbenchResearcher, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

