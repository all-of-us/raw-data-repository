from datetime import datetime
import json
import os

from rdr_service.api.etm_api import EtmApi
from rdr_service.model.etm import EtmQuestionnaireResponse
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase, logger

tool_cmd = 'etm-response-hash-backfill'
tool_desc = 'Backfill the response digest for EtM responses'


class EtmDigestBackfillTool(ToolBase):
    def run(self):
        super().run()

        if self.args.id_list:
            self.update_id_list()
        else:
            self.update_all()

    def update_id_list(self):

        fname = self.args.id_list
        filename = os.path.expanduser(fname)
        if not os.path.exists(filename):
            logger.error(f"File '{filename}' not found.")
            return

        # read ids from file.
        ids = open(os.path.expanduser(fname)).readlines()
        # convert ids from a list of strings to a list of integers.
        ids = [int(i) for i in ids if i.strip()]
        num_ids = len(ids)
        if num_ids > 2500:
            logger.error(f'Max of 2500 etm_questionnaire_response_ids can be backfilled. File contained {num_ids} ids')
        elif num_ids:
            with self.get_session() as session:
                questionnaire_response_query = session.query(
                    EtmQuestionnaireResponse
                ).filter(
                    EtmQuestionnaireResponse.etm_questionnaire_response_id.in_(ids),
                    EtmQuestionnaireResponse.response_hash.is_(None)
                ).limit(2500)

                for response in questionnaire_response_query:
                    response_hash = EtmApi.calculate_response_hash(json.loads(response.resource))
                    response.response_hash = response_hash

                session.commit()

    def update_all(self):
        latest_id = -10
        with self.get_session() as session:
            found_responses = True
            while found_responses:
                found_responses = False
                questionnaire_response_query = session.query(
                    EtmQuestionnaireResponse
                ).filter(
                    EtmQuestionnaireResponse.etm_questionnaire_response_id > latest_id,
                    EtmQuestionnaireResponse.response_hash.is_(None)
                ).order_by(EtmQuestionnaireResponse.etm_questionnaire_response_id).limit(2500)

                for response in questionnaire_response_query:
                    found_responses = True

                    response_hash = EtmApi.calculate_response_hash(json.loads(response.resource))
                    response.response_hash = response_hash

                    latest_id = response.questionnaireResponseId

                if found_responses:
                    logger.info(f'got to {latest_id}')
                    logger.info(datetime.now())
                    logger.info('committing')
                    session.commit()


def add_additional_arguments(parser):
    parser.add_argument('--id-list', required=False,
                        help="file of specific etm_questionnaire_response_id values to backfill (max length 2500)")


def run():
    return cli_run(tool_cmd, tool_desc, EtmDigestBackfillTool, add_additional_arguments)
