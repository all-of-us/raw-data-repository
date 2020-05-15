from flask import request

from rdr_service.api.base_api import UpdatableApi, log_api_request
from rdr_service.api_util import BIOBANK
from rdr_service.app_util import auth_required
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao, BiobankSpecimenAttributeDao, BiobankAliquotDao,\
    BiobankAliquotDatasetDao
from werkzeug.exceptions import BadRequest, NotFound
from sqlalchemy.orm.exc import NoResultFound


class BiobankApiBase(UpdatableApi):
    def _make_response(self, obj):
        result = self.dao.to_client_json(obj)
        return result, 200


class BiobankSpecimenApi(BiobankApiBase):
    def __init__(self):
        super().__init__(BiobankSpecimenDao(), get_returns_children=True)

    @auth_required(BIOBANK)
    def put(self, *args, **kwargs):  # pylint: disable=unused-argument
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
            with self.dao.session() as session:
                for specimen_json in resource:
                    if 'rlimsID' in specimen_json:
                        descriptor = specimen_json['rlimsID']
                    else:
                        descriptor = f'specimen #{total_count+1}'

                    try:
                        self._check_required_specimen_fields(specimen_json)

                        m = self.dao.from_client_json(specimen_json)
                        if m.id is not None:
                            self.dao.update_with_session(session, m)
                        else:
                            self.dao.insert_with_session(session, m)
                    except BadRequest as e:
                        errors.append(f'[{descriptor}] {e.description}')
                    except Exception: # pylint: disable=broad-except
                        # Handle most anything but continue with processing specimen anyway
                        errors.append(f'[{descriptor}] Unknown error')
                    else:
                        success_count += 1
                    finally:
                        total_count += 1

            log_api_request(log=request.log_record)
            result = {'summary': f'Added {success_count} of {total_count} specimen'}
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
    def put(self, *args, **kwargs):  # pylint: disable=unused-argument
        rlims_id = kwargs['rlims_id']
        model = self.get_model_with_rlims_id(rlims_id)

        resource = request.get_json(force=True)
        self.update_model(model, resource)

        log_api_request(log=request.log_record, model_obj=model)
        return self._make_response(model)

    def get_model_with_rlims_id(self, rlims_id):
        # pylint: disable=unused-argument
        raise NotImplementedError(f"get_model_with_rlims_id not implemented in {self.__class__}")

    def update_model(self, model, resource):
        # pylint: disable=unused-argument
        raise NotImplementedError(f"update_model not implemented in {self.__class__}")


class BiobankStatusApiMixin():
    def update_model(self, model, resource):
        self.dao.read_client_status(resource, model)
        self.dao.update(model)


class BiobankDisposalApiMixin():
    def update_model(self, model, resource):
        self.dao.read_client_disposal(resource, model)
        self.dao.update(model)


class BiobankSpecimenTargetedUpdateBase(BiobankTargetedUpdateBase):
    def __init__(self):
        super(BiobankSpecimenTargetedUpdateBase, self).__init__(BiobankSpecimenDao())

    def get_model_with_rlims_id(self, rlims_id):
        try:
            return self.dao.get_with_rlims_id(rlims_id)
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

    def update_model(self, model, resource):
        resource['name'] = self.attribute_name

        attribute_dao = BiobankSpecimenAttributeDao()
        attribute = attribute_dao.from_client_json(resource, specimen_rlims_id=model.rlimsId)

        attribute.specimen_id = model.id
        if attribute.id is None:
            attribute_dao.insert(attribute)
        else:
            attribute_dao.update(attribute)


class BiobankSpecimenAliquotApi(BiobankSpecimenTargetedUpdateBase):
    def __init__(self):
        super(BiobankSpecimenAliquotApi, self).__init__()
        self.aliquot_rlims_id = None

    @auth_required(BIOBANK)
    def put(self, *args, **kwargs):
        self.aliquot_rlims_id = kwargs['aliquot_rlims_id']
        super(BiobankSpecimenAliquotApi, self).put(*args, **kwargs)

    def update_model(self, model, resource):
        resource['rlimsID'] = self.aliquot_rlims_id

        aliquot_dao = BiobankAliquotDao()
        aliquot = aliquot_dao.from_client_json(resource, specimen_rlims_id=model.rlimsId)

        aliquot.specimen_id = model.id
        if aliquot.id is None:
            aliquot_dao.insert(aliquot)
        else:
            aliquot_dao.update(aliquot)


class BiobankAliquotTargetedUpdateBase(BiobankTargetedUpdateBase):
    def __init__(self):
        super(BiobankAliquotTargetedUpdateBase, self).__init__(BiobankAliquotDao())

    def get_model_with_rlims_id(self, rlims_id):
        try:
            return self.dao.get_with_rlims_id(rlims_id)
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

    def update_model(self, model, resource):
        resource['rlimsID'] = self.dataset_rlims_id

        dataset_dao = BiobankAliquotDatasetDao()
        dataset = dataset_dao.from_client_json(resource, aliquot_rlims_id=model.rlimsId)

        dataset.aliquot_id = model.id
        if dataset.id is None:
            dataset_dao.insert(dataset)
        else:
            dataset_dao.update(dataset)
