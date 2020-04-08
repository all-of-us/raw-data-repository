from api_util import format_json_date
from model.utils import to_client_participant_id
from rdr_service.dao.base_dao import FhirMixin, FhirProperty, UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen
from rdr_service.model.utils import from_client_participant_id


class BiobankSpecimenDao(UpdatableDao):
    def __init__(self):
        super().__init__(BiobankSpecimen)

    def put(self, specimen):
        print(specimen)

    def to_client_json(self, model):
        result = model.asdict()
        result["participantId"] = to_client_participant_id(model.participantId)
        format_json_date(result, 'collectionDate')
        format_json_date(result, 'confirmedDate')
        result = {k: v for k, v in list(result.items()) if v is not None}
        return result

    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None):
        pid = from_client_participant_id(participant_id)
        order = BiobankSpecimen(rlimsId=resource['rlimsId'], participantId=pid, orderId=resource['orderId'],
                                testCode=resource['testCode'], repositoryId=resource['repositoryId'],
                                studyId=resource['studyId'], cohortId=resource['cohortId'],
                                collectionDate=resource['collectionDate'], confirmedDate=resource['confirmedDate'])
        return order





