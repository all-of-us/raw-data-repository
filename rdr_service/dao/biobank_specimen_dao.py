from rdr_service.api_util import format_json_date
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute, BiobankAliquot,\
    BiobankAliquotDataset, BiobankAliquotDatasetItem


class BiobankSpecimenDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(BiobankSpecimen)

    def get_etag(self, id_, pid):  # pylint: disable=unused-argument
        return None

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

        # Remove fields internal fields from output
        for field_name in ['created', 'modified']:
            del result[field_name]

        return result

    @staticmethod
    def read_client_status(status_source, model):
        for status_field_name, parser in [('status', None),
                                          ('freezeThawCount', None),
                                          ('location', None),
                                          ('quantity', None),
                                          ('quantityUnits', None),
                                          ('deviations', None),
                                          ('processingCompleteDate', parse_date)]:
            BiobankSpecimenDao.map_optional_json_field_to_object(status_source, model, status_field_name, parser=parser)

    @staticmethod
    def read_client_disposal(status_source, model):
        for disposal_client_field_name, disposal_model_field_name, parser in [('reason', 'disposalReason', None),
                                                                              ('disposalDate', None, parse_date)]:
            BiobankSpecimenDao.map_optional_json_field_to_object(status_source, model, disposal_client_field_name,
                                                                 disposal_model_field_name, parser=parser)

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
            del result[field_name]

        return result


class BiobankAliquotDao(UpdatableDao):
    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None):
        aliquot = BiobankAliquot(rlimsId=resource['rlimsID'], specimen_rlims_id=specimen_rlims_id)

        for client_field, model_field in [('sampleType', None),
                                          ('childPlanService', None),
                                          ('initialTreatment', None),
                                          ('containerTypeID', 'containerTypeId')]:
            BiobankSpecimenDao.map_optional_json_field_to_object(resource, aliquot, client_field, model_field)

        if 'status' in resource:
            BiobankSpecimenDao.read_client_status(resource['status'], aliquot)

        if 'disposalStatus' in resource:
            BiobankSpecimenDao.read_client_disposal(resource['disposalStatus'], aliquot)

        if 'datasets' in resource:
            dataset_dao = BiobankAliquotDatasetDao(BiobankAliquotDataset)
            aliquot.datasets = [dataset_dao.from_client_json(dataset_json, aliquot_rlims_id=aliquot.rlimsId)
                                for dataset_json in resource['datasets']]

        return aliquot

    def to_client_json(self, model):
        result = model.asdict()

        for client_field_name, model_field_name in [('rlimsID', 'rlimsId'),
                                                    ('containerTypeID', 'containerTypeId')]:
            result[client_field_name] = result.pop(model_field_name)

        result['status'] = BiobankSpecimenDao.to_client_status(result)
        result['disposalStatus'] = BiobankSpecimenDao.to_client_disposal(result)

        with self.session() as session:
            datasets = session.query(BiobankAliquotDataset).filter(BiobankAliquotDataset.aliquot_id == model.id)
            if datasets.count() > 0:
                result['datasets'] = []
                dataset_dao = BiobankAliquotDatasetDao(BiobankAliquotDataset)
                for dataset in datasets:
                    result['datasets'].append(dataset_dao.to_client_json(dataset))

        #todo: add created and modified listeners for everything

        # Remove fields internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id', 'parent_aliquot_id',
                           'parent_aliquot_rlims_id']:
            del result[field_name]

        return result


class BiobankAliquotDatasetDao(UpdatableDao):
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         aliquot_rlims_id=None):
        dataset = BiobankAliquotDataset(rlimsId=resource['rlimsID'], aliquot_rlims_id=aliquot_rlims_id)

        for field_name in ['name', 'status']:
            BiobankSpecimenDao.map_optional_json_field_to_object(resource, dataset, field_name)

        if 'datasetItems' in resource:
            item_dao = BiobankAliquotDatasetItemDao(BiobankAliquotDatasetItem)
            dataset.datasetItems = [item_dao.from_client_json(item_json, dataset_rlims_id=dataset.rlimsId)
                                    for item_json in resource['datasetItems']]

        return dataset

    def to_client_json(self, model):
        result = model.asdict()

        result['rlimsID'] = result.pop('rlimsId')

        with self.session() as session:
            items = session.query(BiobankAliquotDatasetItem).filter(
                BiobankAliquotDatasetItem.dataset_id == model.id)
            if items.count() > 0:
                result['datasetItems'] = []
                item_dao = BiobankAliquotDatasetItemDao(BiobankAliquotDatasetItem)
                for item in items:
                    result['datasetItems'].append(item_dao.to_client_json(item))

        for field_name in ['id', 'created', 'modified', 'aliquot_id', 'aliquot_rlims_id']:
            del result[field_name]

        return result


class BiobankAliquotDatasetItemDao(UpdatableDao):
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         dataset_rlims_id=None):
        item = BiobankAliquotDatasetItem(paramId=resource['paramID'], dataset_rlims_id=dataset_rlims_id)

        for field_name in ['displayValue', 'displayUnits']:
            BiobankSpecimenDao.map_optional_json_field_to_object(resource, item, field_name)

        return item

    def to_client_json(self, model):
        result = model.asdict()

        result['paramID'] = result.pop('paramId')

        for field_name in ['id', 'created', 'modified', 'dataset_id', 'dataset_rlims_id']:
            del result[field_name]

        return result
