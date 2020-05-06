from rdr_service.api_util import format_json_date
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute, BiobankAliquot


class BiobankSpecimenDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(BiobankSpecimen)

    def get_etag(self, id_, pid):  # pylint: disable=unused-argument
        return None

    def to_client_json(self, model):
        result = model.asdict()

        for client_field_name, model_field_name in [('cohortID', 'cohortId'),
                                                    ('orderID', 'orderId'),
                                                    ('confirmationDate', 'confirmedDate'),
                                                    ('studyID', 'studyId'),
                                                    ('repositoryID', 'repositoryId'),
                                                    ('rlimsID', 'rlimsId'),
                                                    ('testcode', 'testCode')]:
            result[client_field_name] = result.pop(model_field_name)

        # Translate biobankId
        result['participantID'] = to_client_biobank_id(result.pop('biobankId'))

        # Format dates
        for date_field in ['collectionDate', 'confirmationDate', 'processingCompleteDate', 'created', 'disposalDate']:
            format_json_date(result, date_field)

        result_status = result['status']
        result['status'] = {}
        for status_field in ['deviations', 'freezeThawCount', 'location', 'processingCompleteDate', 'quantity',
                             'quantityUnits']:
            if status_field in result:
                result['status'][status_field] = result.pop(status_field)
        result['status']['status'] = result_status

        result['disposalStatus'] = {}
        for disposal_client_field, disposal_model_field in [('reason', 'disposalReason'),
                                                            ('disposalDate', 'disposalDate')]:
            if disposal_model_field in result:
                result['disposalStatus'][disposal_client_field] = result.pop(disposal_model_field)

        with self.session() as session:
            attributes = session.query(BiobankSpecimenAttribute).filter(
                BiobankSpecimenAttribute.specimen_id == model.id)
            if attributes.count() > 0:
                result['attributes'] = []
                attribute_dao = BiobankSpecimenAttributeDao(BiobankSpecimenAttribute)
                for attribute in attributes:
                    result['attributes'].append(attribute_dao.to_client_json(attribute))

            aliquots = session.query(BiobankAliquot).filter(BiobankAliquot.specimen_id == model.id)
            if aliquots.count() > 0:
                result['aliquots'] = []
                aliquot_dao = BiobankAliquotDao(BiobankAliquot)
                for aliquot in aliquots:
                    result['aliquots'].append(aliquot_dao.to_client_json(aliquot))

        return result

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None):
        order = BiobankSpecimen(rlimsId=resource['rlimsID'], orderId=resource['orderID'], testCode=resource['testcode'],
                                biobankId=from_client_biobank_id(resource['participantID']))

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

        if 'disposalStatus' in resource:
            for disposal_client_field_name, disposal_model_field_name, parser in\
                        [('reason', 'disposalReason', None),
                         ('disposalDate', 'disposalDate', parse_date)]:
                self.map_optional_json_field_to_object(resource['disposalStatus'], order, disposal_client_field_name,
                                                       disposal_model_field_name, parser=parser)

        if 'attributes' in resource:
            attribute_dao = BiobankSpecimenAttributeDao(BiobankSpecimenAttribute)
            order.attributes = [attribute_dao.from_client_json(attr_json, specimen_rlims_id=order.rlimsId)
                                for attr_json in resource['attributes']]

        if 'aliquots' in resource:
            aliquot_dao = BiobankAliquotDao(BiobankAliquot)
            order.aliquots = [aliquot_dao.from_client_json(attr_json, specimen_rlims_id=order.rlimsId)
                              for attr_json in resource['aliquots']]

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
            return order.id

    def _do_update(self, session, obj, existing_obj):
        # Id isn't sent by client request (just rlimsId)
        obj.id = existing_obj.id
        super(BiobankSpecimenDao, self)._do_update(session, obj, existing_obj)


class BiobankSpecimenAttributeDao(UpdatableDao):
    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None):
        #todo: make sure things like specimen_rlims_id, specimen_id,
        # and other keys that don't get returned to client are stored
        attribute = BiobankSpecimenAttribute(name=resource['name'], specimen_rlims_id=specimen_rlims_id)

        if 'value' in resource:
            attribute.value = resource['value']

        return attribute

    def to_client_json(self, model):
        result = model.asdict()

        # Remove fields internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id']:
            result.pop(field_name)

        return result

class BiobankAliquotDao(UpdatableDao):
    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None):
        aliquot = BiobankAliquot(rlimsId=resource['rlimsID'], specimen_rlims_id=specimen_rlims_id)

        return aliquot

    def to_client_json(self, model):
        result = model.asdict()

        return result

