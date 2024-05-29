from sqlalchemy.orm import aliased

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.dao.database_factory import get_database
from rdr_service.model.etm import EtmQuestionnaireResponse


tool_cmd = 'etm-duplication-detector'
tool_desc = 'Identify and mark duplicate etm responses'


class EtmDuplicateDetector(ToolBase):
    def run(self):
        super().run()

        with get_database().session() as session:
            duplicate_qr = aliased(EtmQuestionnaireResponse)
            duplicates = session.query(
                EtmQuestionnaireResponse.etm_questionnaire_response_id,
                duplicate_qr.etm_questionnaire_response_id
            ).join(
                duplicate_qr,
                duplicate_qr.response_hash == EtmQuestionnaireResponse.response_hash
            ).filter(
                EtmQuestionnaireResponse.etm_questionnaire_response_id != duplicate_qr.etm_questionnaire_response_id
            )
            print(duplicates.all())


def run():
    cli_run(tool_cmd, tool_desc, EtmDuplicateDetector)
