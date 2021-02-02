import logging
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.services.gcp_utils import gcp_monitoring_list_policy, gcp_monitoring_create_policy, \
    gcp_monitoring_update_policy, gcp_monitoring_delete_policy

_logger = logging.getLogger("rdr_logger")
tool_cmd = 'monitoring-policy'
tool_desc = 'Google Cloud monitoring policy management'

# monitoring-policy tool command examples:
# list:     python -m tools monitoring-policy --project [project name] --action list
#           or
#           python -m tools monitoring-policy --project [project name] --action list [policy name]
# create:   python -m tools monitoring-policy --project [project name] --action create --policy-file [file location]
# update:   python -m tools monitoring-policy --project [project name] --action update --policy-name [policy name]
#           --policy-file [file location]
# delete:   python -m tools monitoring-policy --project all-of-us-rdr-sandbox --action delete [policy name]


class MonitoringPolicyTool(ToolBase):
    def run(self):
        super(MonitoringPolicyTool, self).run()
        if not self.args.action:
            _logger.error(f'Error: no action parameter found, aborting')
            return 1

        project = self.args.project
        action = self.args.action.lower()
        policy_name = self.args.policy_name
        policy_file = self.args.policy_file

        if action not in ('create', 'update', 'delete', 'list'):
            _logger.error(f'Error: only support the following actions: "create, update, delete or list", aborting')
            return 1

        if action == 'list':
            gcp_monitoring_list_policy(project, policy_name)
        elif action == 'create':
            gcp_monitoring_create_policy(project, policy_file)
        elif action == 'update':
            gcp_monitoring_update_policy(project, policy_file, policy_name)
        elif action == 'delete':
            gcp_monitoring_delete_policy(project, policy_name)


def add_additional_arguments(arg_parser):
    arg_parser.add_argument('--action', help='required, specify "create, update, delete or list"', default=None)
    arg_parser.add_argument('--policy-name', help='optional, specify the policy name', default=None)
    arg_parser.add_argument('--policy-file', help='optional, specify the policy yaml file', default=None)


def run():
    return cli_run(tool_cmd, tool_desc, MonitoringPolicyTool, add_additional_arguments)
