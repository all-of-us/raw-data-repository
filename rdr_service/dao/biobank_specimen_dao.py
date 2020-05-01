from rdr_service.api_util import format_json_date
from rdr_service.model.utils import to_client_participant_id
from rdr_service import clock
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen


class BiobankSpecimenDao(UpdatableDao):
    def __init__(self):
        super().__init__(BiobankSpecimen)

    def to_client_json(self, model):
        result = model.asdict()

        result['cohortID'] = result.pop('cohortId')
        result['orderID'] = result.pop('orderId')
        result['confirmationDate'] = result.pop('confirmedDate')
        result['studyID'] = result.pop('studyId')
        result['repositoryID'] = result.pop('repositoryId')
        result['rlimsID'] = result.pop('rlimsId')
        result['testcode'] = result.pop('testCode')
        del result['participantId']

        result["participantID"] = to_client_participant_id(model.participantId)
        format_json_date(result, 'collectionDate')
        format_json_date(result, 'confirmationDate')
        format_json_date(result, 'processingCompleteDate')
        format_json_date(result, 'created')

        result_status = result['status']
        result['status'] = {}
        for status_field in ['deviations', 'freezeThawCount', 'location', 'processingCompleteDate', 'quantity',
                             'quantityUnits']:
            if status_field in result:
                result['status'][status_field] = result.pop(status_field)
        result['status']['status'] = result_status

        return result

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None):
        order = BiobankSpecimen(rlimsId=resource['rlimsID'], participantId=participant_id, orderId=resource['orderID'],
                                testCode=resource['testcode'])

        if not self.exists(resource):
            order.created = clock.CLOCK.now()

        # order.status = resource['status']
        # for key, value in resource['status'].items():
        #     setattr(order, key, value)
        # order.collectionDate = parse_date(order.collectionDate)
        # order.confirmedDate = parse_date(order.confirmedDate)
        # order.processingCompleteDate = parse_date(order.processingCompleteDate)
        order.version = 1
        return order

    @staticmethod
    def exists(resource):
        return True if resource.get('id') else False

