from flask import request
import logging
from werkzeug.exceptions import BadRequest, NotFound
from sqlalchemy.orm.exc import NoResultFound

from rdr_service.api.base_api import UpdatableApi, log_api_request
from rdr_service.api_util import BIOBANK
from rdr_service.app_util import auth_required
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao, BiobankSpecimenAttributeDao, BiobankAliquotDao,\
    BiobankAliquotDatasetDao
from rdr_service.model.biobank_order import BiobankAliquot


class BiobankApiBase(UpdatableApi):
    def _make_response(self, obj):
        result = self.dao.to_client_json(obj)
        return result, 200


class BiobankSpecimenApi(BiobankApiBase):
    def __init__(self):
        super().__init__(BiobankSpecimenDao(), get_returns_children=True)

    @auth_required(BIOBANK)
    def put(self, *_, **kwargs):
        resource = request.get_json(force=True)

        if 'rlims_id' in kwargs:
            self._check_required_specimen_fields(resource)

            if self.dao.exists(resource):
                rlims_id = kwargs['rlims_id']
                return super(BiobankSpecimenApi, self).put(rlims_id, skip_etag=True)
            else:
                return super(BiobankSpecimenApi, self).post()
        else:
            success_count = 0
            total_count = 0
            errors = []

            for specimen_json in resource:
                try:
                    self.dao.ready_preloader(specimen_json)
                except KeyError:
                    # If there's a problem parsing something in the specimen json ignore it for now, when the
                    # request is fully processed (after loading) an error message will be added to the response
                    # for it.
                    logging.error('Error found when preprocessing specimen for migration', exc_info=True)

            with self.dao.session() as session:

                self.dao.preloader.hydrate(session)

                for specimen_json in resource:
                    rlims_id = specimen_json.get('rlimsID', '')

                    try:
                        self._check_required_specimen_fields(specimen_json)

                        m = self.dao.from_client_json(specimen_json)
                        if m.id is not None:
                            self.dao.update_with_session(session, m)
                        else:
                            self.dao.insert_with_session(session, m)
                        for obj in session.deleted:
                            logging.info(f'removing {obj} {getattr(obj, "rlimsId", "NA")}')
                        for obj in session.dirty:
                            logging.info(f'updating {obj} {getattr(obj, "rlimsId", "NA")}')
                        session.commit()
                    except BadRequest as e:
                        logging.error('RLIMS Migration: BadRequest encountered', exc_info=True)
                        errors.append({
                            'rlimsID': rlims_id,
                            'error': e.description
                        })
                        session.rollback()
                    except Exception: # pylint: disable=broad-except
                        logging.error('RLIMS Migration: Server error encountered', exc_info=True)
                        errors.append({
                            'rlimsID': rlims_id,
                            'error': 'Unknown error'
                        })
                        session.rollback()
                    else:
                        success_count += 1
                    finally:
                        total_count += 1

            self.dao.preloader.clear()

            log_api_request(log=request.log_record)
            result = {
                'summary': {
                    'total_received': total_count,
                    'success_count': success_count
                }
            }
            if errors:
                result['errors'] = errors
            return result

    @staticmethod
    def _check_required_specimen_fields(specimen_json):
        missing_fields = [required_field for required_field in ['rlimsID', 'orderID', 'testcode', 'participantID']
                          if required_field not in specimen_json]
        if missing_fields:
            raise BadRequest(f"Missing fields: {', '.join(missing_fields)}")


class BiobankTargetedUpdateBase(BiobankApiBase):
    @auth_required(BIOBANK)
    def put(self, *_, **kwargs):
        rlims_id = kwargs['rlims_id']
        with self.dao.session() as session:
            model = self.get_model_with_rlims_id(rlims_id, session)

            resource = request.get_json(force=True)
            self.update_model(model, resource, session)

            log_api_request(log=request.log_record, model_obj=model)
            return self._make_response(model)

    def get_model_with_rlims_id(self, rlims_id, session):
        raise NotImplementedError(f"get_model_with_rlims_id not implemented in {self.__class__}")

    def update_model(self, model, resource, session):
        raise NotImplementedError(f"update_model not implemented in {self.__class__}")


class BiobankStatusApiMixin:
    def update_model(self, model, resource, session):
        self.dao.read_client_status(resource, model)
        self.dao.update_with_session(session, model)


class BiobankDisposalApiMixin:
    def update_model(self, model, resource, session):
        self.dao.read_client_disposal(resource, model)
        self.dao.update_with_session(session, model)


class BiobankSpecimenTargetedUpdateBase(BiobankTargetedUpdateBase):
    def __init__(self):
        super(BiobankSpecimenTargetedUpdateBase, self).__init__(BiobankSpecimenDao())

    def get_model_with_rlims_id(self, rlims_id, session):
        try:
            return self.dao.get_with_rlims_id(rlims_id, session)
        except NoResultFound:
            raise NotFound(f'No specimen found for the given rlims_id: {rlims_id}')


class BiobankSpecimenStatusApi(BiobankStatusApiMixin, BiobankSpecimenTargetedUpdateBase):
    pass


class BiobankSpecimenDisposalApi(BiobankDisposalApiMixin, BiobankSpecimenTargetedUpdateBase):
    pass


class BiobankSpecimenAttributeApi(BiobankSpecimenTargetedUpdateBase):
    def __init__(self):
        super(BiobankSpecimenAttributeApi, self).__init__()
        self.attribute_name = None

    @auth_required(BIOBANK)
    def put(self, *args, **kwargs):
        self.attribute_name = kwargs['attribute_name']
        super(BiobankSpecimenAttributeApi, self).put(*args, **kwargs)

    def update_model(self, model, resource, session):
        resource['name'] = self.attribute_name

        attribute_dao = BiobankSpecimenAttributeDao()
        attribute = attribute_dao.from_client_json(resource, specimen_rlims_id=model.rlimsId, session=session)

        attribute.specimen_id = model.id
        if attribute.id is None:
            attribute_dao.insert_with_session(session, attribute)
        else:
            attribute_dao.update_with_session(session, attribute)

    @staticmethod
    @auth_required(BIOBANK)
    def delete(rlims_id, attribute_name):
        attribute_dao = BiobankSpecimenAttributeDao()
        attribute_dao.delete(rlims_id, attribute_name)
        return 200


class BiobankAliquotApi(BiobankApiBase):
    def __init__(self):
        super(BiobankAliquotApi, self).__init__(BiobankAliquotDao())
        self.aliquot_rlims_id = None
        self.parent_rlims_id = None

    @auth_required(BIOBANK)
    def put(self, *_, **kwargs):
        self.aliquot_rlims_id = kwargs['rlims_id']
        self.parent_rlims_id = kwargs['parent_rlims_id']

        aliquot_id = self.dao.get_id(BiobankAliquot(rlimsId=self.aliquot_rlims_id))
        if aliquot_id is None:
            super(BiobankAliquotApi, self).post()
        else:
            super(BiobankAliquotApi, self).put(aliquot_id, skip_etag=True)

    def _get_model_to_update(self, resource, id_, expected_version, participant_id=None):
        return self._parse_aliquot_json(resource)

    def _get_model_to_insert(self, resource, participant_id=None):
        return self._parse_aliquot_json(resource)

    def _parse_aliquot_json(self, resource):
        resource['rlimsID'] = self.aliquot_rlims_id
        return self.dao.from_client_json(resource, parent_rlims_id=self.parent_rlims_id)


class BiobankAliquotTargetedUpdateBase(BiobankTargetedUpdateBase):
    def __init__(self):
        super(BiobankAliquotTargetedUpdateBase, self).__init__(BiobankAliquotDao())

    def get_model_with_rlims_id(self, rlims_id, session):
        try:
            return self.dao.get_with_rlims_id(rlims_id, session)
        except NoResultFound:
            raise NotFound(f'No aliquot found for the given rlims_id: {rlims_id}')


class BiobankAliquotStatusApi(BiobankStatusApiMixin, BiobankAliquotTargetedUpdateBase):
    pass


class BiobankAliquotDisposalApi(BiobankDisposalApiMixin, BiobankAliquotTargetedUpdateBase):
    pass


class BiobankAliquotDatasetApi(BiobankAliquotTargetedUpdateBase):
    def __init__(self):
        super(BiobankAliquotDatasetApi, self).__init__()
        self.dataset_rlims_id = None

    @auth_required(BIOBANK)
    def put(self, *args, **kwargs):
        self.dataset_rlims_id = kwargs['dataset_rlims_id']
        super(BiobankAliquotDatasetApi, self).put(*args, **kwargs)

    def update_model(self, model, resource, session):
        resource['rlimsID'] = self.dataset_rlims_id

        dataset_dao = BiobankAliquotDatasetDao()
        dataset = dataset_dao.from_client_json(resource, aliquot_rlims_id=model.rlimsId, session=session)

        dataset.aliquot_id = model.id
        if dataset.id is None:
            dataset_dao.insert_with_session(session, dataset)
        else:
            dataset_dao.update_with_session(session, dataset)
