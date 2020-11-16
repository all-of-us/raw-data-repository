#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import argparse
import json
import logging
import os
import sys
import traceback

from .gcp_cloud_datastore import GoogleCloudDatastoreConfigProvider
from .gcp_utils import gcp_get_current_account, gcp_get_current_project
from .system_utils import setup_logging, JSONObject

# from google.api_core.exceptions import NotFound

_logger = logging.getLogger("rdr_logger")


class GCPFunctionConfigObject(object):

    project = None
    git_project = None

    def __init__(self, config_dict):
        """
        :param config_dict: A dict of config items.
        """
        if config_dict:
            for key, val in config_dict.items():
                self.__dict__[key] = val

    def cleanup(self):
        pass

    def get_func_config(self, config_key='function_config', project=None):
        """
        Get the current function config.
        :return: dict
        """
        if not project:
            project = self.project

        # See if we should use local configs or cloud configs.
        if not project or project == 'localhost':
            file = os.path.join(self.git_project, f'rdr_service/.configs/{config_key}.json')
            config = json.loads(open(file, 'r').read())
        else:
            provider = GoogleCloudDatastoreConfigProvider()
            config = provider.load(config_key, project=project)


class GCPCloudFunctionContext(object):
    """
    A processing context manager for GCP operations
    """
    _tool_cmd = None
    _command = None
    _project = 'localhost'  # default to localhost.
    _account = None
    _service_account = None
    _env = None

    _env_config_obj = None

    def __init__(self, command, args):
        """
        Initialize GCP Context Manager
        :param command: command name
        :param args: parsed argparser commandline arguments object.
        """
        if not command:
            _logger.error("command not set, aborting.")
            exit(1)

        self._command = command
        if args:
            self._project = args.project
            self._account = args.account
            self._service_account = args.service_account
        else:
            # Note: This may break if Python38 is used. Google notes that not all vars are available yet.
            self._project = gcp_get_current_project()
            self._account = gcp_get_current_account()
            self._service_account = None

        self._env = {
            "project": self._project,
            "account": self._account,
            "service_account": self._service_account
        }

    def __enter__(self):
        """ Return object with properties set to config values """
        self._env_config_obj = GCPFunctionConfigObject(self._env)
        return self._env_config_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Clean up or close everything we need to """
        self._env_config_obj.cleanup()

        if exc_type is not None:
            print((traceback.format_exc()))
            _logger.error("program encountered an unexpected error, quitting.")
            exit(1)

    @staticmethod
    def setup_logging(tool_cmd):
        setup_logging(
            _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None)

    @staticmethod
    def get_argparser(tool_cmd, tool_desc):
        """
        :param tool_cmd: Tool command line id.
        :param tool_desc: Tool description.
        """
        # Setup program arguments.
        parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
        parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
        parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
        parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
        parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
        parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
        return parser


class FunctionBaseHandler:
    """
    Base class for all GCloud function event handling.
    https://cloud.google.com/functions/docs/concepts/events-triggers#functions_parameters-python
    """
    gcp_env = None
    debug = False

    def __init__(self, gcp_env):
        self.gcp_env = gcp_env

        if os.getenv('FUNC_DEBUG'):
            self.debug = True
        # TODO: Setup Python logging to Stackdriver here.

    def run(self):
        raise NotImplemented('Run method not implemented.')


class FunctionStoragePubSubHandler(FunctionBaseHandler):
    """
    Handler for GCloud Storage Pub/Sub events.
    https://cloud.google.com/functions/docs/calling/storage
    """
    event = None
    context = None

    def __init__(self, gcp_env, event, context):
        super().__init__(gcp_env)
        if not isinstance(event, JSONObject):
            self.event = JSONObject(event)
        self.context = context

        if self.debug:
            _logger.info(f'Current project: {gcp_env.project}')
            _logger.info(f'Current account: {gcp_env.account}')
            _logger.info('Event ID: {}'.format(self.context.event_id))
            _logger.info('Event type: {}'.format(self.context.event_type))

            _logger.info('Bucket: {}'.format(self.event.bucket))
            _logger.info('File: {}'.format(self.event.name))
            _logger.info('Metageneration: {}'.format(self.event.metageneration))
            _logger.info('Created: {}'.format(self.event.timeCreated))
            _logger.info('Updated: {}'.format(self.event.updated))
            _logger.info(self.event.to_json())

    def created(self):
        """ Called when a new object is created in a bucket. """
        raise NotImplemented('Method not implemented.')

    def deleted(self):
        """ Called when an object is deleted from a bucket. """
        raise NotImplemented('Method not implemented.')

    def archived(self):
        """ Called when an object is archived in a bucket. """
        raise NotImplemented('Method not implemented.')

    def meta_updated(self):
        """ Called when an object's meta data is updated in a bucket. """
        raise NotImplemented('Method not implemented.')

    def run(self):

        if self.context.event_type == 'google.storage.object.finalize':
            self.created()
        elif self.context.event_type == 'google.storage.object.delete':
            self.deleted()
        elif self.context.event_type == 'google.storage.object.archive':
            self.archived()
        elif self.context.event_type == 'google.storage.object.metadataUpdate':
            self.meta_updated()
        else:
            # Should never reach here.
            raise NameError('Context ID name not matched.')


class FunctionEvent(object):
    """ Simple Function Event class for debugging/testing """
    event_id = None
    event_type = None

    def __init__(self, event_id, event_type):
        self.event_id = event_id
        self.event_type = event_type
