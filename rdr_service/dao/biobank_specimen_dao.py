from rdr_service.api_util import format_json_date
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute, BiobankAliquot,\
    BiobankAliquotDataset, BiobankAliquotDatasetItem
from werkzeug.exceptions import BadRequest


class BiobankDaoBase(UpdatableDao):
    @staticmethod
    def parse_nullable_date(date_str):
        if date_str:  # Empty strings are falsy
            return parse_date(date_str)
        else:
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

    @staticmethod
    def read_client_status(status_source, model):
        for status_field_name, parser in [('status', None),
                                          ('freezeThawCount', None),
                                          ('location', None),
                                          ('quantity', None),
                                          ('quantityUnits', None),
                                          ('deviations', None),
                                          ('processingCompleteDate', BiobankDaoBase.parse_nullable_date)]:
            BiobankDaoBase.map_optional_json_field_to_object(status_source, model, status_field_name, parser=parser)

        if model.status and model.status.lower() != 'disposed':
            model.disposalDate = None
            model.disposalReason = ''

    @staticmethod
    def read_client_disposal(status_source, model):
        for disposal_client_field_name, disposal_model_field_name, parser in\
                [('reason', 'disposalReason', None),
                 ('disposalDate', None, BiobankSpecimenDao.parse_nullable_date)]:
            BiobankDaoBase.map_optional_json_field_to_object(status_source, model, disposal_client_field_name,
                                                             disposal_model_field_name, parser=parser)

        if model.disposalDate or model.disposalReason:
            model.status = 'Disposed'

    @staticmethod
    def map_optional_json_field_to_object(json, obj, json_field_name, object_field_name=None, parser=None):
        if object_field_name is None:
            object_field_name = json_field_name

        if json_field_name in json:
            value = json[json_field_name]
            if parser is not None:
                value = parser(value)

            setattr(obj, object_field_name, value)

    def collection_to_json(self, session, filter_expr=None):
        """
        Iterate any existing instances of a collection and create json output for them
        :param session: SQLAlchemy session for loading objects
        :param filter_expr: SQLAlchemy expression for determining the instances to add to the collection.
            Defaults to None.
        :return:
        """
        objects_found = session.query(self.model_type).filter(filter_expr).all()
        if len(objects_found) > 0:
            return [self.to_client_json_with_session(model, session) for model in objects_found]
        else:
            return None

    def collection_from_json(self, json_array, **constructor_kwargs):
        if json_array:
            return [self.from_client_json(item_json, **constructor_kwargs) for item_json in json_array]

        return []

    def to_client_json_with_session(self, model, session):
        raise NotImplementedError

    def to_client_json(self, model):
        with self.session() as session:
            json = self.to_client_json_with_session(model, session)
        return json

    @staticmethod
    def get_id_with_session(obj, session):
        raise NotImplementedError

    def get_id(self, obj):
        with self.session() as session:
            return self.get_id_with_session(obj, session)


class BiobankSpecimenDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self):
        super().__init__(BiobankSpecimen)

    def get_etag(self, id_, pid):  # pylint: disable=unused-argument
        return None

    def to_client_json_with_session(self, model, session):
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
        result['attributes'] = attribute_dao.collection_to_json(session,
                                                                BiobankSpecimenAttribute.specimen_id == model.id)

        aliquot_dao = BiobankAliquotDao()
        result['aliquots'] = aliquot_dao.collection_to_json(session, BiobankAliquot.specimen_id == model.id)

        # Remove internal fields from output
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
                                                  ('collectionDate', None, self.parse_nullable_date),
                                                  ('confirmationDate', 'confirmedDate', self.parse_nullable_date)]:
            self.map_optional_json_field_to_object(resource, order, client_field, model_field, parser)

        if 'status' in resource:
            self.read_client_status(resource['status'], order)

        if 'disposalStatus' in resource:
            self.read_client_disposal(resource['disposalStatus'], order)

        with self.session() as session:
            if 'attributes' in resource:
                attribute_dao = BiobankSpecimenAttributeDao()
                order.attributes = attribute_dao.collection_from_json(resource['attributes'],
                                                                      specimen_rlims_id=order.rlimsId, session=session)

            if 'aliquots' in resource:
                aliquot_dao = BiobankAliquotDao()
                order.aliquots = aliquot_dao.collection_from_json(resource['aliquots'],
                                                                  specimen_rlims_id=order.rlimsId, session=session)

            order.id = self.get_id_with_session(order, session)
        return order

    def exists(self, resource):
        with self.session() as session:
            return session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == resource['rlimsID']).count() > 0

    @staticmethod
    def get_with_rlims_id(rlims_id, session):
        return session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == rlims_id).one()

    @staticmethod
    def get_id_with_session(obj, session):
        order = session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == obj.rlimsId).one_or_none()
        if order is not None:
            return order.id
        else:
            return None


class BiobankSpecimenAttributeDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self):
        super().__init__(BiobankSpecimenAttribute)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None, session=None):

        if specimen_rlims_id is None:
            raise BadRequest("Specimen rlims id required for specimen attributes")

        attribute = BiobankSpecimenAttribute(name=resource['name'], specimen_rlims_id=specimen_rlims_id)

        if 'value' in resource:
            attribute.value = resource['value']

        if session is None:
            with self.session() as session:
                attribute.id = self.get_id_with_session(attribute, session)
        else:
            attribute.id = self.get_id_with_session(attribute, session)
        return attribute

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        # Remove internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id']:
            del result[field_name]

        return result

    @staticmethod
    def get_id_with_session(obj, session):
        attribute = session.query(BiobankSpecimenAttribute).filter(
            BiobankSpecimenAttribute.specimen_rlims_id == obj.specimen_rlims_id,
            BiobankSpecimenAttribute.name == obj.name
        ).one_or_none()
        if attribute is not None:
            return attribute.id
        else:
            return None


class BiobankAliquotDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self):
        super().__init__(BiobankAliquot)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         specimen_rlims_id=None, parent_aliquot_rlims_id=None, session=None):

        if specimen_rlims_id is None:
            raise BadRequest("Specimen rlims id required for aliquots")

        aliquot = BiobankAliquot(rlimsId=resource['rlimsID'], specimen_rlims_id=specimen_rlims_id,
                                 parent_aliquot_rlims_id=parent_aliquot_rlims_id)

        if session is None:
            with self.session() as session:
                self.read_aliquot_data(aliquot, resource, specimen_rlims_id, session)
        else:
            self.read_aliquot_data(aliquot, resource, specimen_rlims_id, session)

        return aliquot

    def read_aliquot_data(self, aliquot, resource, specimen_rlims_id, session):
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
            aliquot.datasets = dataset_dao.collection_from_json(resource['datasets'], aliquot_rlims_id=aliquot.rlimsId,
                                                                session=session)

        if 'aliquots' in resource:
            aliquot.aliquots = self.collection_from_json(resource['aliquots'], specimen_rlims_id=specimen_rlims_id,
                                                         parent_aliquot_rlims_id=aliquot.rlimsId, session=session)

        aliquot.id = self.get_id_with_session(aliquot, session)

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        for client_field_name, model_field_name in [('rlimsID', 'rlimsId'),
                                                    ('containerTypeID', 'containerTypeId')]:
            result[client_field_name] = result.pop(model_field_name)

        result['status'] = self.to_client_status(result)
        result['disposalStatus'] = self.to_client_disposal(result)

        dataset_dao = BiobankAliquotDatasetDao()
        result['datasets'] = dataset_dao.collection_to_json(session, BiobankAliquotDataset.aliquot_id == model.id)

        result['aliquots'] = self.collection_to_json(session, BiobankAliquot.parent_aliquot_id == model.id)

        # Remove internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id', 'parent_aliquot_id',
                           'parent_aliquot_rlims_id']:
            del result[field_name]

        return result

    @staticmethod
    def get_with_rlims_id(rlims_id, session):
        return session.query(BiobankAliquot).filter(BiobankAliquot.rlimsId == rlims_id).one()

    @staticmethod
    def get_id_with_session(obj, session):
        aliquot = session.query(BiobankAliquot).filter(
            BiobankAliquot.rlimsId == obj.rlimsId,
        ).one_or_none()
        if aliquot is not None:
            return aliquot.id
        else:
            return None


class BiobankAliquotDatasetDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self):
        super().__init__(BiobankAliquotDataset)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         aliquot_rlims_id=None, session=None):

        if aliquot_rlims_id is None:
            raise BadRequest("Aliquot rlims id required for dataset")

        dataset = BiobankAliquotDataset(rlimsId=resource['rlimsID'], aliquot_rlims_id=aliquot_rlims_id)

        for field_name in ['name', 'status']:
            self.map_optional_json_field_to_object(resource, dataset, field_name)

        if 'datasetItems' in resource:
            item_dao = BiobankAliquotDatasetItemDao()

            if session is None:
                with self.session() as session:
                    dataset.datasetItems = item_dao.collection_from_json(resource['datasetItems'],
                                                                         dataset_rlims_id=dataset.rlimsId,
                                                                         session=session)
            else:
                dataset.datasetItems = item_dao.collection_from_json(resource['datasetItems'],
                                                                     dataset_rlims_id=dataset.rlimsId, session=session)

        dataset.id = self.get_id_with_session(dataset, session)
        return dataset

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        result['rlimsID'] = result.pop('rlimsId')

        item_dao = BiobankAliquotDatasetItemDao()
        result['datasetItems'] = item_dao.collection_to_json(session, BiobankAliquotDatasetItem.dataset_id == model.id)

        for field_name in ['id', 'created', 'modified', 'aliquot_id', 'aliquot_rlims_id']:
            del result[field_name]

        return result

    @staticmethod
    def get_id_with_session(obj, session):
        dataset = session.query(BiobankAliquotDataset).filter(
            BiobankAliquotDataset.rlimsId == obj.rlimsId,
        ).one_or_none()
        if dataset is not None:
            return dataset.id
        else:
            return None


class BiobankAliquotDatasetItemDao(BiobankDaoBase):

    def __init__(self):
        super().__init__(BiobankAliquotDatasetItem)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         dataset_rlims_id=None, session=None):

        if dataset_rlims_id is None:
            raise BadRequest("Dataset rlims id required for dataset item")

        item = BiobankAliquotDatasetItem(paramId=resource['paramID'], dataset_rlims_id=dataset_rlims_id)

        for field_name in ['displayValue', 'displayUnits']:
            self.map_optional_json_field_to_object(resource, item, field_name)

        item.id = self.get_id_with_session(item, session)
        return item

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        result['paramID'] = result.pop('paramId')

        for field_name in ['id', 'created', 'modified', 'dataset_id', 'dataset_rlims_id']:
            del result[field_name]

        return result

    @staticmethod
    def get_id_with_session(obj, session):
        dataset_item = session.query(BiobankAliquotDatasetItem).filter(
            BiobankAliquotDatasetItem.dataset_rlims_id == obj.dataset_rlims_id,
            BiobankAliquotDatasetItem.paramId == obj.paramId
        ).one_or_none()
        if dataset_item is not None:
            return dataset_item.id
        else:
            return None
