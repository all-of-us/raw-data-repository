from rdr_service.dao.base_dao import BaseDao


class ExposomicsDefaultBaseDao(BaseDao):
    def __init__(self, model_type):
        super().__init__(
            model_type, order_by_ending=['id']
        )

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass
