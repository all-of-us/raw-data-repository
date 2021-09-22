from sqlalchemy import or_, and_

from rdr_service.api_util import format_json_date
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.model.participant import Participant
from rdr_service.api_util import parse_date
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.object_preloader import LoadingStrategy, ObjectPreloader
from rdr_service.model.biobank_order import BiobankSpecimen, BiobankSpecimenAttribute, BiobankAliquot,\
    BiobankAliquotDataset, BiobankAliquotDatasetItem
from werkzeug.exceptions import BadRequest, NotFound


class RlimsIdLoadingStrategy(LoadingStrategy):

    @staticmethod
    def get_key_from_object(obj):
        return obj.rlimsId

    @staticmethod
    def get_filtered_query(query, object_class, keys):
        return query.filter(object_class.rlimsId.in_(keys))


class SpecimenAttributeLoadingStrategy(LoadingStrategy):

    @staticmethod
    def get_key_from_object(obj):
        return obj.specimen_rlims_id, obj.name

    @staticmethod
    def get_filtered_query(query, object_class, keys):
        filter_list = [and_(
            BiobankSpecimenAttribute.specimen_rlims_id == rlims_id,
            BiobankSpecimenAttribute.name == name
        ) for rlims_id, name in keys]
        return query.filter(or_(*filter_list))


class AliquotDatasetItemLoadingStrategy(LoadingStrategy):

    @staticmethod
    def get_key_from_object(obj):
        return obj.dataset_rlims_id, obj.paramId

    @staticmethod
    def get_filtered_query(query, object_class, keys):
        filter_list = [and_(
            BiobankAliquotDatasetItem.dataset_rlims_id == dataset_rlims_id,
            BiobankAliquotDatasetItem.paramId == paramId
        ) for dataset_rlims_id, paramId in keys]
        return query.filter(or_(*filter_list))


class BiobankDaoBase(UpdatableDao):
    def __init__(self, object_type, preloader=None):
        super(BiobankDaoBase, self).__init__(object_type)
        self.preloader: ObjectPreloader = preloader

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

    def get_etag(self, *_):
        # Because the UpdatableApi class requires this when processing a PUT request
        return None


class BiobankSpecimenDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self, preloader=ObjectPreloader({
        BiobankSpecimen: RlimsIdLoadingStrategy,
        BiobankSpecimenAttribute: SpecimenAttributeLoadingStrategy,
        BiobankAliquot: RlimsIdLoadingStrategy,
        BiobankAliquotDataset: RlimsIdLoadingStrategy,
        BiobankAliquotDatasetItem: AliquotDatasetItemLoadingStrategy
    })):
        super().__init__(BiobankSpecimen, preloader=preloader)

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
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         session=None):
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

        if session is None:
            with self.session() as session:
                self._read_data_with_session(resource, order, session)
        else:
            self._read_data_with_session(resource, order, session)
        return order

    def _read_data_with_session(self, resource, order, session):
        if 'attributes' in resource:
            attribute_dao = BiobankSpecimenAttributeDao(preloader=self.preloader)
            order.attributes = attribute_dao.collection_from_json(resource['attributes'],
                                                                  specimen_rlims_id=order.rlimsId, session=session)

        if 'aliquots' in resource:
            aliquot_dao = BiobankAliquotDao(preloader=self.preloader)
            order.aliquots = aliquot_dao.collection_from_json(resource['aliquots'],
                                                              specimen_rlims_id=order.rlimsId, session=session)

        order.id = self.get_id_with_session(order, session)

    def ready_preloader(self, specimen_json):
        specimen_rlims_id = specimen_json['rlimsID']
        self.preloader.register_for_hydration(BiobankSpecimen(rlimsId=specimen_rlims_id))

        if specimen_json.get('attributes'):
            attribute_dao = BiobankSpecimenAttributeDao()
            for attribute_json in specimen_json['attributes']:
                attribute_dao.ready_preloader(self.preloader, attribute_json, specimen_rlims_id)

        if specimen_json.get('aliquots'):
            aliquot_dao = BiobankAliquotDao()
            for aliquot_json in specimen_json['aliquots']:
                aliquot_dao.ready_preloader(self.preloader, aliquot_json)

    def exists(self, resource):
        with self.session() as session:
            return session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == resource['rlimsID']).count() > 0

    @staticmethod
    def get_with_rlims_id(rlims_id, session):
        return session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == rlims_id).one()

    def get_id_with_session(self, obj, session):
        if self.preloader and self.preloader.is_hydrated:
            specimen = self.preloader.get_object(obj)
        else:
            specimen = session.query(BiobankSpecimen).filter(BiobankSpecimen.rlimsId == obj.rlimsId).one_or_none()

        if specimen is not None:
            return specimen.id
        else:
            return None

    @staticmethod
    def _check_participant_exists(session, biobank_id):
        participant_query = session.query(Participant).filter(Participant.biobankId == biobank_id)
        if not session.query(participant_query.exists()).scalar():
            raise BadRequest(f'Biobank id {to_client_biobank_id(biobank_id)} does not exist')

    def insert_with_session(self, session, obj: BiobankSpecimen):
        self._check_participant_exists(session, obj.biobankId)
        return super(BiobankSpecimenDao, self).insert_with_session(session, obj)

    def update_with_session(self, session, obj: BiobankSpecimen):
        self._check_participant_exists(session, obj.biobankId)
        return super(BiobankSpecimenDao, self).update_with_session(session, obj)


class BiobankSpecimenAttributeDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self, preloader=None):
        super().__init__(BiobankSpecimenAttribute, preloader=preloader)

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

    @staticmethod
    def ready_preloader(preloader: ObjectPreloader, attribute_json, specimen_rlims_id):
        attribute = BiobankSpecimenAttribute(name=attribute_json['name'])
        attribute.specimen_rlims_id = specimen_rlims_id
        preloader.register_for_hydration(attribute)

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        # Remove internal fields from output
        for field_name in ['id', 'created', 'modified', 'specimen_id', 'specimen_rlims_id']:
            del result[field_name]

        return result

    def get_id_with_session(self, obj, session):
        if self.preloader and self.preloader.is_hydrated:
            attribute = self.preloader.get_object(obj)
        else:
            attribute = session.query(BiobankSpecimenAttribute).filter(
                BiobankSpecimenAttribute.specimen_rlims_id == obj.specimen_rlims_id,
                BiobankSpecimenAttribute.name == obj.name
            ).one_or_none()

        if attribute is not None:
            return attribute.id
        else:
            return None

    def delete(self, specimen_rlims_id, attribute_name):
        with self.session() as session:
            session.query(BiobankSpecimenAttribute).filter(
                BiobankSpecimenAttribute.specimen_rlims_id == specimen_rlims_id,
                BiobankSpecimenAttribute.name == attribute_name
            ).delete()


class BiobankAliquotDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self, preloader=None):
        super().__init__(BiobankAliquot, preloader=preloader)

    def _get_parents(self, parent_rlims_id, session) -> (BiobankSpecimen, BiobankAliquot):
        # See if the given parent_rlims_id is a specimen
        if self.preloader:
            parent_specimen = self.preloader.get_object(BiobankSpecimen(rlimsId=parent_rlims_id))
        else:
            parent_specimen = session.query(BiobankSpecimen).filter(
                BiobankSpecimen.rlimsId == parent_rlims_id
            ).one_or_none()

        # If the direct parent is a specimen, give that back and no parent aliquot
        if parent_specimen:
            return parent_specimen, None
        else:
            # The given rlims should be an aliquot then. So find the aliquot and give back the aliquot and specimen
            if self.preloader:
                parent_aliquot = self.preloader.get_object(BiobankAliquot(rlimsId=parent_rlims_id))
                parent_specimen = self.preloader.get_object(BiobankSpecimen(rlimsId=parent_aliquot.specimen_rlims_id))
            else:
                parent_aliquot = session.query(BiobankAliquot).filter(
                    BiobankAliquot.rlimsId == parent_rlims_id
                ).one_or_none()
                if parent_aliquot is None:
                    raise NotFound(f'No parent specimen or aliquot found with ID {parent_rlims_id}')

                parent_specimen = session.query(BiobankSpecimen).filter(
                    BiobankSpecimen.rlimsId == parent_aliquot.specimen_rlims_id
                ).one_or_none()

            return parent_specimen, parent_aliquot

    def _from_client_json_with_session(self, resource, session, parent_rlims_id=None, specimen_rlims_id=None,
                                       parent_aliquot_rlims_id=None):
        specimen, parent_aliquot = None, None

        # If not given a parent_rlims_id, then the specimen_rlims_id is expected to be the parent
        if parent_rlims_id is not None:
            specimen, parent_aliquot = self._get_parents(parent_rlims_id, session)
            specimen_rlims_id = specimen.rlimsId if specimen else None
            parent_aliquot_rlims_id = parent_aliquot.rlimsId if parent_aliquot else None
        aliquot = BiobankAliquot(rlimsId=resource['rlimsID'], specimen_rlims_id=specimen_rlims_id,
                                 parent_aliquot_rlims_id=parent_aliquot_rlims_id)
        self.read_aliquot_data(aliquot, resource, specimen_rlims_id, session)

        if specimen:
            aliquot.specimen_id = specimen.id
        if parent_aliquot:
            aliquot.parent_aliquot_id = parent_aliquot.id
        return aliquot

    def from_client_json(self, resource, session=None, parent_rlims_id=None, specimen_rlims_id=None,
                         parent_aliquot_rlims_id=None, **_):
        if session is None:
            with self.session() as session:
                aliquot_from_json = self._from_client_json_with_session(
                    resource,
                    session,
                    parent_rlims_id=parent_rlims_id,
                    specimen_rlims_id=specimen_rlims_id,
                    parent_aliquot_rlims_id=parent_aliquot_rlims_id
                )
        else:
            aliquot_from_json = self._from_client_json_with_session(
                resource,
                session,
                parent_rlims_id=parent_rlims_id,
                specimen_rlims_id=specimen_rlims_id,
                parent_aliquot_rlims_id=parent_aliquot_rlims_id
            )

        return aliquot_from_json

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
            dataset_dao = BiobankAliquotDatasetDao(preloader=self.preloader)
            aliquot.datasets = dataset_dao.collection_from_json(resource['datasets'], aliquot_rlims_id=aliquot.rlimsId,
                                                                session=session)

        if 'aliquots' in resource:
            aliquot.aliquots = self.collection_from_json(resource['aliquots'], specimen_rlims_id=specimen_rlims_id,
                                                         parent_aliquot_rlims_id=aliquot.rlimsId, session=session)

        aliquot.id = self.get_id_with_session(aliquot, session)

    def ready_preloader(self, preloader: ObjectPreloader, aliquot_json):
        preloader.register_for_hydration(BiobankAliquot(rlimsId=aliquot_json['rlimsID']))

        if aliquot_json.get('datasets'):
            dataset_dao = BiobankAliquotDatasetDao()
            for dataset_json in aliquot_json['datasets']:
                dataset_dao.ready_preloader(preloader, dataset_json)

        if aliquot_json.get('aliquots'):
            for child_json in aliquot_json['aliquots']:
                self.ready_preloader(preloader, child_json)

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

    def get_id_with_session(self, obj, session):
        if self.preloader and self.preloader.is_hydrated:
            aliquot = self.preloader.get_object(obj)
        else:
            aliquot = session.query(BiobankAliquot).filter(
                BiobankAliquot.rlimsId == obj.rlimsId,
            ).one_or_none()

        if aliquot is not None:
            return aliquot.id
        else:
            return None


class BiobankAliquotDatasetDao(BiobankDaoBase):

    validate_version_match = False

    def __init__(self, preloader=None):
        super().__init__(BiobankAliquotDataset, preloader=preloader)

    #pylint: disable=unused-argument
    def from_client_json(self, resource, id_=None, expected_version=None, participant_id=None, client_id=None,
                         aliquot_rlims_id=None, session=None):

        if aliquot_rlims_id is None:
            raise BadRequest("Aliquot rlims id required for dataset")

        dataset = BiobankAliquotDataset(rlimsId=resource['rlimsID'], aliquot_rlims_id=aliquot_rlims_id)

        for field_name in ['name', 'status']:
            self.map_optional_json_field_to_object(resource, dataset, field_name)

        if 'datasetItems' in resource:
            item_dao = BiobankAliquotDatasetItemDao(preloader=self.preloader)

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

    def ready_preloader(self, preloader: ObjectPreloader, dataset_json):
        dataset_rlims_id = dataset_json['rlimsID']
        preloader.register_for_hydration(BiobankAliquotDataset(rlimsId=dataset_rlims_id))

        if dataset_json.get('datasetItems'):
            item_dao = BiobankAliquotDatasetItemDao()

            for dataset_item_json in dataset_json['datasetItems']:
                item_dao.ready_preloader(preloader, dataset_item_json, dataset_rlims_id)

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        result['rlimsID'] = result.pop('rlimsId')

        item_dao = BiobankAliquotDatasetItemDao()
        result['datasetItems'] = item_dao.collection_to_json(session, BiobankAliquotDatasetItem.dataset_id == model.id)

        for field_name in ['id', 'created', 'modified', 'aliquot_id', 'aliquot_rlims_id']:
            del result[field_name]

        return result

    def get_id_with_session(self, obj, session):
        if self.preloader and self.preloader.is_hydrated:
            dataset = self.preloader.get_object(obj)
        else:
            dataset = session.query(BiobankAliquotDataset).filter(
                BiobankAliquotDataset.rlimsId == obj.rlimsId,
            ).one_or_none()

        if dataset is not None:
            return dataset.id
        else:
            return None


class BiobankAliquotDatasetItemDao(BiobankDaoBase):

    def __init__(self, preloader=None):
        super().__init__(BiobankAliquotDatasetItem, preloader=preloader)

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

    def ready_preloader(self, preloader: ObjectPreloader, dataset_item_json, dataset_rlims_id):
        dataset_item = BiobankAliquotDatasetItem(paramId=dataset_item_json['paramID'])
        dataset_item.dataset_rlims_id = dataset_rlims_id
        preloader.register_for_hydration(dataset_item)

    def to_client_json_with_session(self, model, session):
        result = model.asdict()

        result['paramID'] = result.pop('paramId')

        for field_name in ['id', 'created', 'modified', 'dataset_id', 'dataset_rlims_id']:
            del result[field_name]

        return result

    def get_id_with_session(self, obj, session):
        if self.preloader and self.preloader.is_hydrated:
            dataset_item = self.preloader.get_object(obj)
        else:
            dataset_item = session.query(BiobankAliquotDatasetItem).filter(
                BiobankAliquotDatasetItem.dataset_rlims_id == obj.dataset_rlims_id,
                BiobankAliquotDatasetItem.paramId == obj.paramId
            ).one_or_none()

        if dataset_item is not None:
            return dataset_item.id
        else:
            return None
