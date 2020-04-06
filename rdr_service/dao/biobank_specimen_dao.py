from rdr_service.dao.base_dao import FhirMixin, FhirProperty, UpdatableDao


class BiobankSpecimenDao(UpdatableDao):
    def __init__(self):
        super().__init__()
        #super().__init__(BiobankOrder)
