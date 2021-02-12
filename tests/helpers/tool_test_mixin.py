import mock
import json
import os
from typing import Type

import rdr_service
from rdr_service.tools.tool_libs.tool_base import ToolBase

PROJECT_ROOT = os.path.dirname(os.path.dirname(rdr_service.__file__))


class ToolTestMixin:

    @staticmethod
    def _build_env(server_config):
        if server_config is None:
            server_config = {}

        def construct_server_config_response(*_):
            return json.dumps(server_config), 'test_server_config_file_name'

        gcp_env = mock.MagicMock()
        gcp_env.project = 'localhost'
        gcp_env.git_project = PROJECT_ROOT
        gcp_env.get_latest_config_from_bucket = construct_server_config_response

        return gcp_env

    @staticmethod
    def _build_args(args):
        args_obj = mock.MagicMock()
        if args is not None:
            for field_name, value in args.items():
                args_obj.__setattr__(field_name, value)

        return args_obj

    @classmethod
    def run_tool(cls, tool_class: Type[ToolBase], tool_args: dict = None, server_config: dict = None):
        gcp_env = ToolTestMixin._build_env(server_config)
        tool_args = ToolTestMixin._build_args(tool_args)

        with mock.patch.object(ToolBase, 'initialize_process_context') as mock_init_env:
            mock_init_env.return_value.__enter__.return_value = gcp_env

            tool_instance = tool_class(tool_args, gcp_env)
            return tool_instance.run_process()
