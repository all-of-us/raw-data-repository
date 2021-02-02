import logging

from rdr_service.offline.update_ehr_status import make_update_participant_summaries_job,\
    update_participant_summaries_from_job
from rdr_service.services.system_utils import setup_logging
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

tool_cmd = 'update-participant-ehr'
tool_desc = 'Sync deceased reports from Redcap to an environment'


class UpdateEhrStatusTool(ToolBase):
    def run(self):
        super(UpdateEhrStatusTool, self).run()

        # UpdateEHR code uses a logger instance created in its name, have that print to stdout too
        logging_instance = logging.getLogger('rdr_service.offline.update_ehr_status')
        setup_logging(logging_instance, tool_cmd)

        job = make_update_participant_summaries_job(project_id=self.gcp_env.project, bigquery_view=self.args.view_name)
        if job is not None:
            update_participant_summaries_from_job(job, project_id=self.gcp_env.project)
        else:
            logger.error('Unable to update EHR data: curation data not found')


def add_additional_arguments(parser):
    # todo: set up ability to use configurator SA to pull this from the config
    #  (need to use another SA to activate the proxy and have access to the BQ view)
    parser.add_argument('--view-name', required=True, help='Name of the curation BigQuery view')


def run():
    return cli_run(tool_cmd, tool_desc, UpdateEhrStatusTool, add_additional_arguments)
