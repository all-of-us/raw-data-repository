from flask import request

from rdr_service.api.base_api import UpdatableApi, log_api_request
from rdr_service.api_util import BIOBANK
from rdr_service.app_util import auth_required
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from werkzeug.exceptions import BadRequest


class BiobankSpecimenApi(UpdatableApi):
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

    def _make_response(self, obj):
        result = self.dao.to_client_json(obj)
        return result, 200
