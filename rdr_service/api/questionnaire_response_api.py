from flask_restful import Resource, request
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service import app_util
from rdr_service.config import GAE_PROJECT
from rdr_service.dao.bq_questionnaire_dao import bq_questionnaire_update_task
from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import PTC, PTC_AND_HEALTHPRO
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.code import Code, CodeType
from rdr_service.model.participant import Participant
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask


class QuestionnaireResponseApi(BaseApi):
    def __init__(self):
        super(QuestionnaireResponseApi, self).__init__(QuestionnaireResponseDao())

    @app_util.auth_required(PTC_AND_HEALTHPRO)
    def get(self, p_id, id_):
        # pylint: disable=unused-argument
        return super(QuestionnaireResponseApi, self).get(id_)

    @app_util.auth_required(PTC)
    def post(self, p_id):
        resp = super(QuestionnaireResponseApi, self).post(participant_id=p_id)
        if resp and 'id' in resp:

            qr_id = int(resp['id'])
            if GAE_PROJECT == 'localhost':
                bq_questionnaire_update_task(p_id, qr_id)
            else:
                params = {'p_id': p_id, 'qr_id': qr_id}
                task = GCPCloudTask('bq_rebuild_questionnaire_task',
                                    queue='bigquery-tasks', payload=params, in_seconds=5)
                task.execute()

        return resp


class ParticipantQuestionnaireAnswers(Resource):
    @app_util.auth_required(PTC_AND_HEALTHPRO)
    def get(self, p_id=None, module=None):
        """
    Return questionnaire answers for a participant
    :param p_id: participant id
    :param module: questionnaire module name
    """
        with CodeDao().session() as session:

            # verify participant exists.
            participant = session.query(Participant.participantId).filter(Participant.participantId == p_id).first()
            if not participant:
                raise BadRequest("invalid participant")

            # verify module exists and is a module.
            code = session.query(Code.codeId, Code.value, Code.codeType).filter(Code.value == module).first()
            if not code or code.codeType != CodeType.MODULE:
                raise BadRequest("invalid questionnaire module")

            # see if the participant has submitted a questionnaire response for the module.
            resp = (
                session.query(QuestionnaireResponse.questionnaireId, QuestionnaireResponse.participantId)
                .join(
                    QuestionnaireConcept, QuestionnaireConcept.questionnaireId == QuestionnaireResponse.questionnaireId
                )
                .filter(QuestionnaireConcept.codeId == code.codeId)
                .first()
            )
            if not resp:
                raise NotFound("participant response for module not found.")

        skip_null = (
            True if "skip_null" in request.args and request.args["skip_null"].strip('"').lower() == "true" else False
        )
        filters = request.args["fields"].strip('"') if "fields" in request.args else None

        # If filtering, always include these fields in the response.
        if filters:
            filters += (
                ",questionnaire_id,questionnaire_response_id,created,code_id"
                + ",version,authored,language,participant_id,module"
            )
        try:
            results = QuestionnaireResponseDao().call_proc(
                "sp_get_questionnaire_answers", args=[module, p_id], filters=filters, skip_null=skip_null
            )
        except ValueError as e:
            raise BadRequest(e.message)
        except Exception:
            raise
            # raise BadRequest('zzz invalid request')

        return results
