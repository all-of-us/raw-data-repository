from flask import request
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service import app_util
from rdr_service.api.base_api import UpdatableApi
from rdr_service.api.etm_api import ETM_OUTCOMES_EXT_URL, EtmApi
from rdr_service.api_util import PTC, PTC_AND_HEALTHPRO
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.questionnaire_dao import QuestionnaireDao


class QuestionnaireApi(UpdatableApi):
    def __init__(self):
        super(QuestionnaireApi, self).__init__(QuestionnaireDao())

    @app_util.auth_required(PTC_AND_HEALTHPRO)
    def get(self, id_=None):
        if id_:
            return super(QuestionnaireApi, self).get(id_)
        else:
            concept = request.args.get("concept")
            if not concept:
                raise BadRequest("Either questionnaire ID or concept must be specified in request.")
            concept_code = CodeDao().get_code(PPI_SYSTEM, concept)
            if not concept_code:
                raise BadRequest(f"Code not found: {concept}")
            questionnaire = self.dao.get_latest_questionnaire_with_concept(concept_code.codeId)
            if not questionnaire:
                raise NotFound(f"Could not find questionnaire with concept: {concept}")
            return self._make_response(questionnaire)

    @app_util.auth_required(PTC)
    def post(self):
        # Detect if this is an EtM Questionnaire
        request_json = request.json

        if (
            'extension' in request_json
            and any(
                extension.get('url') == ETM_OUTCOMES_EXT_URL
                for extension in request_json['extension']
            )
        ):
            return EtmApi.post_questionnaire(request_json)
        else:
            return super(QuestionnaireApi, self).post()

    @app_util.auth_required(PTC)
    def put(self, id_):
        return super(QuestionnaireApi, self).put(id_)

    def parse_etag(self, etag):
        if etag.startswith('W/"') and etag.endswith('"'):
            return etag.split('"')[1]
        raise BadRequest("Invalid ETag: {}".format(etag))

    def _make_response(self, obj):
        result = super(UpdatableApi, self)._make_response(obj)
        # use semantic version for questionnaire etag
        etag = self.make_etag(result['version'])
        result['meta'] = {'versionId': etag}
        return result, 200, {'ETag': etag}
