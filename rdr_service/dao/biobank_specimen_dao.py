from rdr_service.api_util import format_json_date
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute, BiobankAliquot,\
    BiobankAliquotDataset, BiobankAliquotDatasetItem
from werkzeug.exceptions import BadRequest


class BiobankJsonParser:
    @staticmethod
    def to_client_status(source_dict):
        status_json = {}
        for status_field in ['deviations', 'freezeThawCount', 'location', 'processingCompleteDate', 'quantity',
                             'quantityUnits', 'status']:
            if status_field in source_dict:
                status_json[status_field] = source_dict.pop(status_field)

        format_json_date(status_json, 'processingCompleteDate')
        return status_json

    @staticmethod
    def to_client_disposal(source_dict):
        disposal_status = {}
        for disposal_client_field, disposal_model_field in [('reason', 'disposalReason'),
                                                            ('disposalDate', 'disposalDate')]:
            if disposal_model_field in source_dict:
                disposal_status[disposal_client_field] = source_dict.pop(disposal_model_field)

        format_json_date(disposal_status, 'disposalDate')
        return disposal_status

    @staticmethod
    def read_client_status(status_source, model):
        for status_field_name, parser in [('status', None),
                                          ('freezeThawCount', None),
                                          ('location', None),
                                          ('quantity', None),
                                          ('quantityUnits', None),
                                          ('deviations', None),
                                          ('processingCompleteDate', parse_date)]:
            BiobankJsonParser.map_optional_json_field_to_object(status_source, model, status_field_name, parser=parser)

    @staticmethod
    def read_client_disposal(status_source, model):
        for disposal_client_field_name, disposal_model_field_name, parser in [('reason', 'disposalReason', None),
                                                                              ('disposalDate', None, parse_date)]:
            BiobankJsonParser.map_optional_json_field_to_object(status_source, model, disposal_client_field_name,
                                                                 disposal_model_field_name, parser=parser)

    @staticmethod
    def map_optional_json_field_to_object(json, obj, json_field_name, object_field_name=None, parser=None):
        if object_field_name is None:
            object_field_name = json_field_name

        if json_field_name in json:
            value = json[json_field_name]
            if parser is not None:
                value = parser(value)

            setattr(obj, object_field_name, value)


class BiobankSpecimenDao(UpdatableDao, BiobankJsonParser):

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
        for date_field in ['collectionDate', 'confirmationDate']:
            format_json_date(result, date_field)

        result['status'] = self.to_client_status(result)
        result['disposalStatus'] = self.to_client_disposal(result)

        attribute_dao = BiobankSpecimenAttributeDao()
        result['attributes'] = attribute_dao.collection_to_json(BiobankSpecimenAttribute.specimen_id == model.id)

        aliquot_dao = BiobankAliquotDao()
        result['aliquots'] = aliquot_dao.collection_to_json(BiobankAliquot.specimen_id == model.id)

        # Remove fields internal fields from output
        for field_name in ['created', 'modified']:
            del result[field_name]

        return result

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None):
        order = BiobankSpecimen(rlimsId=resource['rlimsID'], orderId=resource['orderID'], testCode=resource['testcode'],
                                biobankId=from_client_biobank_id(resource['participantID']))

        for client_field, model_field, parser in [('repositoryID', 'repositoryId', None),
                                                  ('studyID', 'studyId', None),
                                                  ('cohortID', 'cohortId', None),
                                                  ('sampleType', None, None),
                                                  ('collectionDate', None, parse_date),
                                                  ('confirmationDate', 'confirmedDate', parse_date)]:
            self.map_optional_json_field_to_object(resource, order, client_field, model_field, parser)

        if 'status' in resource:
            self.read_client_status(resource['status'], order)

        if 'disposalStatus' in resource:
            self.read_client_disposal(resource['disposalStatus'], order)

        if 'attributes' in resource:
            attribute_dao = BiobankSpecimenAttributeDao()
            order.attributes = attribute_dao.collection_from_json(resource['attributes'],
                                                                  specimen_rlims_id=order.rlimsId)

        if 'aliquots' in resource:
            aliquot_dao = BiobankAliquotDao()
            order.aliquots = aliquot_dao.collection_from_json(resource['aliquots'], specimen_rlims_id=order.rlimsId)

        order.id = self.get_id(order)
        return order

    def exists(self, resource):
        with self.session() as session:
            return session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == resource['rlimsID']).count() > 0

    def get_id(self, obj):
        with self.session() as session:
            order = session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == obj.rlimsId).one_or_none()
            if order is not None:
                return order.id
            else:
                return None


class BiobankSpecimenAttributeDao(UpdatableDao, BiobankJsonParser):

    def __init__(self):
        super().__init__(BiobankSpecimenAttribute)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None):

        if specimen_rlims_id is None:
            raise BadRequest("Specimen rlims id required for specimen attributes")

        attribute = BiobankSpecimenAttribute(name=resource['name'], specimen_rlims_id=specimen_rlims_id)

        if 'value' in resource:
            attribute.value = resource['value']

        attribute.id = self.get_id(attribute)
        return attribute

    def to_client_json(self, model):
        result = model.asdict()

        # Remove fields internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id']:
            del result[field_name]

        return result

    def get_id(self, obj):
        with self.session() as session:
            attribute = session.query(BiobankSpecimenAttribute).filter(
                BiobankSpecimenAttribute.specimen_rlims_id == obj.specimen_rlims_id,
                BiobankSpecimenAttribute.name == obj.name
            ).one_or_none()
            if attribute is not None:
                return attribute.id
            else:
                return None


class BiobankAliquotDao(UpdatableDao, BiobankJsonParser):

    def __init__(self):
        super().__init__(BiobankAliquot)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None):

        if specimen_rlims_id is None:
            raise BadRequest("Specimen rlims id required for aliquots")

        aliquot = BiobankAliquot(rlimsId=resource['rlimsID'], specimen_rlims_id=specimen_rlims_id)

        for client_field, model_field in [('sampleType', None),
                                          ('childPlanService', None),
                                          ('initialTreatment', None),
                                          ('containerTypeID', 'containerTypeId')]:
            self.map_optional_json_field_to_object(resource, aliquot, client_field, model_field)

        if 'status' in resource:
            self.read_client_status(resource['status'], aliquot)

        if 'disposalStatus' in resource:
            self.read_client_disposal(resource['disposalStatus'], aliquot)

        if 'datasets' in resource:
            dataset_dao = BiobankAliquotDatasetDao()
            aliquot.datasets = dataset_dao.collection_from_json(resource['datasets'], aliquot_rlims_id=aliquot.rlimsId)

        aliquot.id = self.get_id(aliquot)
        return aliquot

    def to_client_json(self, model):
        result = model.asdict()

        for client_field_name, model_field_name in [('rlimsID', 'rlimsId'),
                                                    ('containerTypeID', 'containerTypeId')]:
            result[client_field_name] = result.pop(model_field_name)

        result['status'] = self.to_client_status(result)
        result['disposalStatus'] = self.to_client_disposal(result)

        dataset_dao = BiobankAliquotDatasetDao()
        result['datasets'] = dataset_dao.collection_to_json(BiobankAliquotDataset.aliquot_id == model.id)

        #todo: add created and modified listeners for everything

        # Remove fields internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id', 'parent_aliquot_id',
                           'parent_aliquot_rlims_id']:
            del result[field_name]

        return result

    def get_id(self, obj):
        with self.session() as session:
            aliquot = session.query(BiobankAliquot).filter(
                BiobankAliquot.rlimsId == obj.rlimsId,
            ).one_or_none()
            if aliquot is not None:
                return aliquot.id
            else:
                return None


class BiobankAliquotDatasetDao(UpdatableDao, BiobankJsonParser):

    def __init__(self):
        super().__init__(BiobankAliquotDataset)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         aliquot_rlims_id=None):

        if aliquot_rlims_id is None:
            raise BadRequest("Aliquot rlims id required for dataset")

        dataset = BiobankAliquotDataset(rlimsId=resource['rlimsID'], aliquot_rlims_id=aliquot_rlims_id)

        for field_name in ['name', 'status']:
            self.map_optional_json_field_to_object(resource, dataset, field_name)

        if 'datasetItems' in resource:
            item_dao = BiobankAliquotDatasetItemDao()
            dataset.datasetItems = item_dao.collection_from_json(resource['datasetItems'],
                                                                 dataset_rlims_id=dataset.rlimsId)

        dataset.id = self.get_id(dataset)
        return dataset

    def to_client_json(self, model):
        result = model.asdict()

        result['rlimsID'] = result.pop('rlimsId')

        item_dao = BiobankAliquotDatasetItemDao()
        result['datasetItems'] = item_dao.collection_to_json(BiobankAliquotDatasetItem.dataset_id == model.id)

        for field_name in ['id', 'created', 'modified', 'aliquot_id', 'aliquot_rlims_id']:
            del result[field_name]

        return result

    def get_id(self, obj):
        with self.session() as session:
            dataset = session.query(BiobankAliquot).filter(
                BiobankAliquotDataset.rlimsId == obj.rlimsId,
            ).one_or_none()
            if dataset is not None:
                return dataset.id
            else:
                return None


class BiobankAliquotDatasetItemDao(UpdatableDao, BiobankJsonParser):

    def __init__(self):
        super().__init__(BiobankAliquotDatasetItem)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         dataset_rlims_id=None):

        if dataset_rlims_id is None:
            raise BadRequest("Dataset rlims id required for dataset item")

        item = BiobankAliquotDatasetItem(paramId=resource['paramID'], dataset_rlims_id=dataset_rlims_id)

        for field_name in ['displayValue', 'displayUnits']:
            self.map_optional_json_field_to_object(resource, item, field_name)

        item.id = self.get_id(item)
        return item

    def to_client_json(self, model):
        result = model.asdict()

        result['paramID'] = result.pop('paramId')

        for field_name in ['id', 'created', 'modified', 'dataset_id', 'dataset_rlims_id']:
            del result[field_name]

        return result

    def get_id(self, obj):
        with self.session() as session:
            dataset_item = session.query(BiobankAliquotDatasetItem).filter(
                BiobankAliquotDatasetItem.dataset_rlims_id == obj.dataset_rlims_id,
                BiobankAliquotDatasetItem.paramId == obj.paramId
            ).one_or_none()
            if dataset_item is not None:
                return dataset_item.id
            else:
                return None
