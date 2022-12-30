from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.study_nph import (
    Participant, StudyCategory, Site, Order, OrderedSample, SampleUpdate, BiobankFileExport, SampleExport
)


class NphParticipantDao(BaseDao):

    def __init__(self):
        super(NphParticipantDao, self).__init__(Participant)

    def get_id(self, obj: Participant):
        return obj.id


class NphStudyCategoryDao(BaseDao):
    def __init__(self):
        super(NphStudyCategoryDao, self).__init__(StudyCategory)

    def get_id(self, obj: StudyCategory):
        return obj.id


class NphSiteDao(BaseDao):
    def __init__(self):
        super(NphSiteDao, self).__init__(Site)

    def get_id(self, obj: Site):
        return obj.id


class NphOrderDao(BaseDao):
    def __init__(self):
        super(NphOrderDao, self).__init__(Order)

    def get_id(self, obj: Order):
        return obj.id


class NphOrderedSampleDao(BaseDao):
    def __init__(self):
        super(NphOrderedSampleDao, self).__init__(OrderedSample)

    def get_id(self, obj: OrderedSample):
        return obj.id


class NphSampleUpdateDao(BaseDao):
    def __init__(self):
        super(NphSampleUpdateDao, self).__init__(SampleUpdate)

    def get_id(self, obj: SampleUpdate):
        return obj.id


class NphBiobankFileExportDao(BaseDao):
    def __init__(self):
        super(NphBiobankFileExportDao, self).__init__(BiobankFileExport)

    def get_id(self, obj: BiobankFileExport):
        return obj.id


class NphSampleExportDao(BaseDao):
    def __init__(self):
        super(NphSampleExportDao, self).__init__(SampleExport)

    def get_id(self, obj: SampleExport):
        return obj.id
