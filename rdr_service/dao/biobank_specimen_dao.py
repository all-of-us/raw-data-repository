from rdr_service.api_util import format_json_date
from rdr_service.model.utils import to_client_participant_id
from rdr_service import clock
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen


class BiobankSpecimenDao(UpdatableDao):

    validate_version_match = False

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

        for client_field, model_field, parser in [('repositoryID', 'repositoryId', None),
                                                  ('studyID', 'studyId', None),
                                                  ('cohortID', 'cohortId', None),
                                                  ('sampleType', 'sampleType', None),
                                                  ('collectionDate', 'collectionDate', parse_date),
                                                  ('confirmationDate', 'confirmedDate', parse_date)]:
            self.map_optional_json_field_to_object(resource, order, client_field, model_field, parser)

        if 'status' in resource:
            for status_field_name, parser in [('status', None),
                                              ('freezeThawCount', None),
                                              ('location', None),
                                              ('quantity', None),
                                              ('quantityUnits', None),
                                              ('deviations', None),
                                              ('processingCompleteDate', parse_date)]:
                self.map_optional_json_field_to_object(resource['status'], order, status_field_name, parser=parser)

        order.version = 1
        return order

    @staticmethod
    def map_optional_json_field_to_object(json, obj, json_field_name, object_field_name=None, parser=None):
        if object_field_name is None:
            object_field_name = json_field_name

        if json_field_name in json:
            value = json[json_field_name]
            if parser is not None:
                value = parser(value)

            setattr(obj, object_field_name, value)

    def exists(self, resource):
        with self.session() as session:
            return session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == resource['rlimsID']).count() > 0

    def get_id(self, obj):
        with self.session() as session:
            order = session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == obj.rlimsId).one()
            return order.id, order.orderId

    def _do_update(self, session, obj, existing_obj):
        # Id isn't sent by client request (just rlimsId)
        obj.id = existing_obj.id
        super(BiobankSpecimenDao, self)._do_update(session, obj, existing_obj)
