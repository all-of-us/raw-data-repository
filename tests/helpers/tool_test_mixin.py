from contextlib import nullcontext
import mock
from typing import Type

from rdr_service.tools.tool_libs.tool_base import ToolBase


class ToolTestMixin:

    @staticmethod
    def _build_args(args):
        args_obj = mock.MagicMock()
        if args is not None:
            for field_name, value in args.items():
                args_obj.__setattr__(field_name, value)

        return args_obj

    @classmethod
    def run_tool(cls, tool_class: Type[ToolBase], tool_args: dict = None, server_config: dict = None,
                 mock_session=False, project='localhost'):
        gcp_env = mock.MagicMock()
        gcp_env.project = project

        tool_args = ToolTestMixin._build_args(tool_args)

        session_patch = mock.patch.object(ToolBase, 'get_session') if mock_session else nullcontext()
        with mock.patch.object(ToolBase, 'initialize_process_context') as mock_init_env,\
             mock.patch('rdr_service.services.config_client.ConfigClient.get_server_config') as mock_server_config,\
             session_patch:

            mock_init_env.return_value.__enter__.return_value = gcp_env
            mock_server_config.return_value = server_config or {}

            tool_instance = tool_class(tool_args, gcp_env)
            return tool_instance.run_process()
