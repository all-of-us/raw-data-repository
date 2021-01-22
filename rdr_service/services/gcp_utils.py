#
# Google Cloud Platform helpers
#
# !!! This file is python 3.x compliant !!!
#
# superfluous-parens
# pylint: disable=W0612
from dateutil import parser
import glob
import json
import logging
import os
import shlex
import subprocess
from collections import OrderedDict
from random import choice
from typing import List

from .gcp_config import GCP_INSTANCES, GCP_PROJECTS, GCP_REPLICA_INSTANCES, GCP_SERVICE_KEY_STORE
from .system_utils import run_external_program, which

_logger = logging.getLogger("rdr_logger")


def gcp_test_environment():
    """
  Make sure the local environment is good
  :return: True if yes, False if not.
  """
    progs = ["gcloud", "gsutil", "cloud_sql_proxy", "bq", "grep"]

    for prog in progs:

        if not which(prog):
            _logger.error("[{0}] executable is not found.".format(prog))
            return False

    # TODO: Future: put additional checks here as needed, IE: required environment vars.

    _logger.debug("Local Environment : Good.")
    return True


def gcp_validate_project(project):
    """
  Make sure project given is a valid GCP project. Allow short or long project names.
  :param project: project name
  :return: long project id or None if invalid
  """
    if not project:
        _logger.error("project name not set, unable to validate.")
        return None
    if project in ["localhost", "127.0.0.1"]:
        return project
    # check for full length project name
    if "pmi-drc-api" in project or "all-of-us-rdr" in project or "aou-pdr-data" in project:
        if project not in GCP_PROJECTS:
            _logger.error("invalid project name [{0}].".format(project))
            return None
        return project

    # check short project name
    if "test" in project:
        project = "pmi-drc-api-{0}".format(project)
    else:
        project = "all-of-us-rdr-{0}".format(project)

    if project not in GCP_PROJECTS:
        _logger.error("invalid project name [{0}].".format(project))
        return None

    return project


def gcp_get_current_project():
    """
  Return the currently set project name
  :return: project name
  """
    # gcloud config list --format 'value(core.project)'
    pcode, so, se = gcp_gcloud_command("config", 'list --format "value(core.project)"')
    if pcode != 0:
        _logger.error("failed to get current project name. ({0}: {1}).".format(pcode, se))
        return None
    return so.strip()


def gcp_get_project_short_name(project=None):
    """
  Return the short name for the given project
  :param project: project name (optional)
  :return: project short name
  """
    if not project:
        project = gcp_get_current_project()
        if not project:
            return None

    if project in ["localhost", "127.0.0.1"]:
        return project

    project = gcp_validate_project(project)

    if not project:
        return None

    return project.split("-")[-1]


def gcp_initialize(project, account=None, service_account=None):
    """
  Apply settings to local GCP environment. This must be called first to set the
  account and project.
  :param project: gcp project name
  :param account: pmi-ops account
  :param service_account: gcp iam service account
  :return: environment dict
  """
    if not gcp_test_environment():
        return None
    if project:
        if project not in ["localhost", "127.0.0.1"] and not gcp_validate_project(project):
            return None
        if "APPLICATION_ID" not in os.environ:
            os.environ["APPLICATION_ID"] = project
    else:
        # If we don't have a project id, try using the currently set project id.
        project = gcp_get_current_project()
        if not project:
            project = "localhost"

    # Use the account and service_account parameters if set, otherwise try the environment var.
    account = account if account else (os.environ["RDR_ACCOUNT"] if "RDR_ACCOUNT" in os.environ else None)
    service_account = (
        service_account
        if service_account
        else (os.environ["RDR_SERVICE_ACCOUNT"] if "RDR_SERVICE_ACCOUNT" in os.environ else None)
    )

    env = OrderedDict()
    env["project"] = project
    env["account"] = account
    env["service_account"] = service_account
    env["service_key_id"] = None

    if account and not gcp_activate_account(account):
        return None
    # if this is a local project, just return now.
    if project in ["localhost", "127.0.0.1"]:
        return env
    # Set current project.
    if not gcp_set_config("project", project):
        return False
    # set service account and generate a service key.
    if service_account:
        env["service_key_id"] = gcp_create_iam_service_key(service_account, account)
        if not env["service_key_id"]:
            return None
        if not gcp_activate_iam_service_key(env["service_key_id"]):
            return None

    for key, value in list(env.items()):
        _logger.debug("{0} : [{1}].".format(key, value))

    return env


def gcp_cleanup(account):
    """
  Clean up items to do at the program's completion.
  """
    # activate the pmi-ops account so we can delete.
    # Use the account and service_account parameters if set, otherwise try the environment var.
    account = account if account else (os.environ["RDR_ACCOUNT"] if "RDR_ACCOUNT" in os.environ else None)
    if account:
        gcp_activate_account(account)

    # Scan for keys in GCP_SERVICE_KEY_STORE and delete them.
    service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, "*.json")
    files = glob.glob(service_key_path)

    for filename in files:
        service_key_id = os.path.basename(filename).split(".")[0]
        gcp_delete_iam_service_key(service_key_id, account)


def gcp_gcloud_command(group, args, flags=None):
    """
  Run a gcloud command
  :param group: group name
  :param args: command arguments
  :param flags: additional flags to pass to gcloud executable
  :return: (exit code, stdout, stderr)
  """
    if not group or not args or not isinstance(args, str):
        _logger.error("invalid parameters passed to gcp_gcloud_command.")
        return False

    prog = which("gcloud")
    p_args = shlex.split("{0} {1} {2} {3}".format(prog, group, args, flags if flags else ""))
    return run_external_program(p_args)


def gcp_gsutil_command(cmd, args, flags=None):
    """
  Run a gsutil command
  :param cmd: gsutil command name
  :param args: command arguments
  :param flags: additional flags to pass to gsutil executable
  :return: (exit code, stdout, stderr)
  """
    if not cmd or not args or not isinstance(args, str):
        _logger.error("invalid parameters passed to gcp_gsutil_command.")
        return -1, "", "invalid parameters passed to gcp_gsutil_command."

    prog = which("gsutil")
    p_args = shlex.split("{0} {1} {2} {3}".format(prog, flags if flags else "", cmd, args))

    return run_external_program(p_args)


def gcp_set_config(prop, value, flags=None):
    """
  Generic function to set the local GCP SDK config properties.
  https://cloud.google.com/sdk/gcloud/reference/config/set
  :param prop: property name to set value to
  :param value: property value string
  :param flags: additional flags to pass to gcloud executable
  :return: True if successful otherwise False
  """
    if not prop or not value or not isinstance(value, str):
        _logger.error("invalid parameters passed to gcp_set_config.")
        return False

    if prop.lower() == "project":
        value = gcp_validate_project(value)
        if not value:
            _logger.error('"{0}" is an invalid project.'.format(value))
            return False

    _logger.debug('setting gcp config property "{0}" to "{1}".'.format(prop, value))

    # Ex: 'gcloud config set prop value'
    args = "set {0} {1}".format(prop, value)
    pcode, so, se = gcp_gcloud_command("config", args, flags)

    if pcode != 0:
        _logger.error("failed to set gcp config property. ({0}: {1}).".format(pcode, se))
        return False

    _logger.debug("successfully set gcp config property.")

    if prop.lower() == "project":
        _logger.debug("current Project : {0}".format(value))
    else:
        _logger.debug("config : {0} is now {1}".format(prop, value))

    return True


def gcp_unset_config(prop, value, flags=None):
    """
  Generic function to unset the local GCP SDK config properties.
  https://cloud.google.com/sdk/gcloud/reference/config/set
  :param prop: property name to unset value
  :param value: property value string
  :param flags: additional flags to pass to gcloud executable
  :return: True if successful otherwise False
  """
    if not prop or not value or not isinstance(value, str):
        _logger.error("invalid parameters passed to gcp_unset_config.")
        return False

    _logger.debug('setting gcp config property "{0}" to "{1}".'.format(prop, value))

    # Ex: 'gcloud config unset prop value'
    args = "unset {0} {1}".format(prop, value)
    pcode, so, se = gcp_gcloud_command("config", args, flags)

    if pcode != 0:
        _logger.error("failed to unset gcp config property. ({0}: {1}).".format(pcode, se))
        return False

    _logger.debug("successfully unset gcp config property.")

    return True


def gcp_get_config(prop, flags=None):
    """
  Generic function to get a value from the local GCP SDK config properties.
  https://cloud.google.com/sdk/gcloud/reference/config/set
  :param prop: property name to get value
  :param flags: additional flags to pass to gcloud executable
  :return: config property value
  """
    if not prop:
        _logger.error("invalid parameters passed to gcp_unset_config.")
        return None

    _logger.debug('getting gcp config property "{0}".'.format(prop))

    # Ex: 'gcloud config get-value prop'
    args = "get-value {0}".format(prop)
    pcode, so, se = gcp_gcloud_command("config", args, flags)

    if pcode != 0:
        _logger.error("failed to get gcp config property. ({0}: {1}).".format(pcode, se))
        return None

    _logger.debug("successfully unset gcp config property.")

    return so.strip()


def gcp_activate_account(account, flags=None):
    """
  Call gcloud to set current account
  :param account: pmi-ops account
  :param flags: additional flags to pass to gcloud command
  :return: True if successful otherwise False
  """
    _logger.debug("setting active gcp account to {0}.".format(account))

    if not account:
        _logger.error("no GCP account given, aborting.")
        return False

    # Ex: 'gcloud auth login xxx.xxx@pmi-ops.org'
    args = "login {0}".format(account)
    pcode, so, se = gcp_gcloud_command("auth", args, flags)

    if pcode != 0:
        _logger.error("failed to set gcp auth login account. ({0}: {1}).".format(pcode, se))
        return False

    _logger.debug("successfully set account to active.")

    lines = se.split("\n")
    for line in lines:
        if "You are now logged in as" in line:
            _logger.debug(line)

    return True


def gcp_application_default_creds_exist():
    """
    Return true if the application default credentials file exists.
    :return: True if we can find app default creds file otherwise False.
    """
    cred_file = os.path.expanduser('~/.config/gcloud/application_default_credentials.json')
    return os.path.exists(cred_file)


def gcp_get_app_host_name(project=None):
    """
  Return the App Engine hostname for the given project
  :param project: gcp project name
  :return: hostname
  """
    # Get the currently configured project
    if not project:
        project = gcp_get_config("project")

    if project in ["localhost", "127.0.0.1"]:
        return project

    project = gcp_validate_project(project)
    if not project:
        _logger.error('"{0}" is an invalid project.'.format(project))
        return None

    host = "{0}.appspot.com".format(project)
    return host


def gcp_get_app_access_token():
    """
  Get the OAuth2 access token for active gcp account or service account.
  :return: access token string
  """
    args = "print-access-token"
    pcode, so, se = gcp_gcloud_command("auth", args)

    if pcode != 0:
        _logger.error("failed to retrieve auth access token. ({0}: {1}).".format(pcode, se))
        return None

    _logger.debug("retrieved auth access token.")

    return so.strip()


def gcp_make_auth_header():
    """
  Make an oauth authentication header
  :return: dict
  """
    headers = dict()
    headers["Authorization"] = "Bearer {0}".format(gcp_get_app_access_token())
    return headers


def gcp_get_private_key_id(service_key_path):
    """
  Return the private key id for the given key file.
  :param service_key_path: path to service key json file.
  :return: private key id, service account
  """
    private_key = None
    service_account = None

    if not os.path.exists(service_key_path):
        _logger.error("service key file not found ({0}).".format(service_key_path))
        return private_key

    lines = open(service_key_path).readlines()
    for line in lines:
        if "private_key_id" in line:
            private_key = shlex.split(line)[1].replace(",", "")
        if "client_email" in line:
            service_account = shlex.split(line)[1].replace(",", "")

    return private_key, service_account


def gcp_create_iam_service_key(service_account, account=None):
    """
  # Note: Untested
  :param service_account: service account
  :param account: authenticated account if needed
  :return: service key id
  """
    _logger.debug("creating iam service key for service account [{0}].".format(service_account))

    # make sure key store directory exists
    if not os.path.exists(GCP_SERVICE_KEY_STORE):
        os.makedirs(GCP_SERVICE_KEY_STORE)

    # make sure we never duplicate an existing key
    while True:
        service_key_id = "".join(choice("0123456789ABCDEF") for _ in range(12))
        service_key_file = "{0}.json".format(service_key_id)
        service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

        if not os.path.exists(os.path.join(GCP_SERVICE_KEY_STORE, service_key_path)):
            break

    # Ex: 'gcloud iam service-accounts keys create "path/key.json" ...'
    args = 'service-accounts keys create "{0}"'.format(service_key_path)
    flags = "--iam-account={0}".format(service_account)
    if account:
        flags += " --account={0}".format(account)
    pcode, so, se = gcp_gcloud_command("iam", args, flags)

    if pcode != 0:
        _logger.error("failed to create iam service account key. ({0}: {1}).".format(pcode, se))
        return None

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_key_path

    pkid, sa = gcp_get_private_key_id(service_key_path)

    _logger.info("created key file [{0}] with id [{1}].".format(service_key_id, pkid))

    return service_key_id

def gcp_get_iam_service_key_info(service_key_id):
    """
    Get information about the given service key file ID
    :param service_key_id:
    :return: dict with key info
    """
    service_key_file = "{0}.json".format(service_key_id)
    service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

    data = {
        'key_id': service_key_id,
        'key_file': service_key_file,
        'key_path': service_key_path,
        'exists': os.path.exists(service_key_path)
    }

    return data

def gcp_delete_iam_service_key(service_key_id, account=None):
    """
  # Note: Untested
  :param service_key_id: local service key file ID
  :param account: pmi-ops account if needed
  :return: True if successful else False
  """
    _logger.debug("deleting iam service key [{0}].".format(service_key_id))

    service_key_file = "{0}.json".format(service_key_id)
    service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

    if not os.path.exists(service_key_path):
        _logger.error("service key file does not exist ({0}).".format(service_key_id))
        return False

    # Get the private key value so we can delete the key
    pkid, service_account = gcp_get_private_key_id(service_key_path)
    if not pkid:
        return False

    # Ex: 'gcloud iam service-accounts keys delete "private key id" ...'
    args = 'service-accounts keys delete "{0}"'.format(pkid)
    flags = "--quiet --iam-account={0}".format(service_account)
    if account:
        flags += " --account={0}".format(account)

    pcode, so, se = gcp_gcloud_command("iam", args, flags)

    if pcode != 0:
        _logger.warning("failed to delete iam service account key. ({0}: {1}).".format(pcode, se))
        if "NOT_FOUND" in se:
            os.remove(service_key_path)
        return False

    os.remove(service_key_path)

    _logger.info("deleted service account key [{0}] with id [{1}].".format(service_key_id, pkid))

    return True


def gcp_activate_iam_service_key(service_key_id, flags=None):
    """
  Activate the service account key
  :param service_key_id: local service key file ID
  :param flags: additional gcloud command flags
  :return: True if successful else False
  """
    _logger.debug("activating iam service key [{0}].".format(service_key_id))

    service_key_file = "{0}.json".format(service_key_id)
    service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

    if not os.path.exists(service_key_path):
        _logger.error("service key file does not exist ({0}).".format(service_key_id))
        return False

    # Get the private key value so we can delete the key
    pkid, service_account = gcp_get_private_key_id(service_key_path)
    if not pkid:
        return False

    args = "activate-service-account --key-file={0}".format(service_key_path)
    pcode, so, se = gcp_gcloud_command("auth", args, flags)

    if pcode != 0:
        _logger.error("failed to activate iam service account key. ({0}: {1}).".format(pcode, se))
        return False

    _logger.info("activated iam service key [{0}] with id [{1}].".format(service_key_id, pkid))

    return True


def gcp_format_sql_instance(project, port=3320, replica=False):
    """
  Use the project and port to craft a cloud_sql_proxy instance string
  :param project: project name
  :param port: local tcp port
  :param replica: use replica instance
  :return: instance string
  """
    # We don't check for a localhost project here, because establishing a proxy to localhost
    # does not make sense.
    project = gcp_validate_project(project)
    if not project:
        _logger.error('"{0}" is an invalid gcp project.'.format(project))
        return None

    name = GCP_INSTANCES[project] if not replica else GCP_REPLICA_INSTANCES[project]
    instance = "{0}=tcp:{1}".format(name, port)

    return instance


def gcp_activate_sql_proxy(instances):
    """
  Call cloud_sql_proxy to make a connection to the given instance.
  NOTE: If you are using a GCPProcessContext object, call self.gcp_env.activate_sql_proxy() instead
        of calling this function directly.
  :param instances: full instance information, format "name:location:database=tcp:PORT, ...".
  :return: popen object
  """
    prog = which("cloud_sql_proxy")
    p = subprocess.Popen(shlex.split("{0} -instances={1} ".format(prog, instances)))
    if not p:
        raise IOError("failed to execute cloud_sql_proxy")
    return p


def gcp_get_mysql_instance_service_account(instance: str) -> (str, None):
    """
    Get the service account for the given instance.
    :param instance: MySQL Instance name
    :return: Service account email or None
    """
    if ':' in instance:
        instance = instance.split(':')[-1:][0]

    args = f'instances describe {instance}'

    pcode, so, se = gcp_gcloud_command("sql", args)
    if pcode != 0:
        _logger.error("failed to get mysql instance service account. ({0}: {1}).".format(pcode, se))
        return None

    lines = so.split('\n')
    for line in lines:
        if line.startswith('serviceAccountEmailAddress'):
            return line.split(':')[1].strip()

    return None


def gcp_bq_command(cmd, args, global_flags=None, command_flags=None, headless=True):
    """
  Run a bq command
  :param cmd: bq command name
  :param args: command arguments
  :param global_flags: global flags to pass to bq executable
  :param command_flags: command flags to pass to bq executable
  :param headless: run the 'bq' command in headless mode
  :return: (exit code, stdout, stderr)
  """
    if not cmd or not args or not isinstance(args, str):
        _logger.error("invalid parameters passed to gcp_bq_command.")
        return False

    p_args = shlex.split(
        "{0} {1} {2} {3} {4} {5}".format(
            which("bq"),
            "--headless" if headless else "",
            global_flags if global_flags else "",
            cmd,
            command_flags if command_flags else "",
            args,
        )
    )

    return run_external_program(p_args)


def gcp_cp(src, dest, args=None, flags=None):
    """
  GCP utility to copy files.
  :param src: source path and file name
  :param dest: destination path and file name
  :param args: additional args for the `cp` command
  :param flags: additional flags to pass to gsutil
  :return: True if completed successfully otherwise False
  """
    if not src or not isinstance(src, str):
        raise ValueError("invalid src value")
    if "//" in src[:6] and not src.startswith("gs://"):
        raise ValueError("src does not start with gs://")

    if not dest or not isinstance(dest, str):
        raise ValueError("invalid dest value")
    if "//" in src[:6] and not src.startswith("gs://"):
        raise ValueError("dest does not start with gs://")

    cmd = "cp {0}".format(args if args else "").strip()
    pcode, so, se = gcp_gsutil_command(cmd, "{0} {1}".format(src, dest), flags=flags)
    if pcode != 0:
        _logger.error("failed to copy file. ({0}: {1}).".format(pcode, se))
        return False

    return True


def gcp_mv(src, dest, args=None, flags=None):
    """
  GCP utility to move files.
  :param src: source path and file name
  :param dest: destination path and file name
  :param args: additional flags for the `mv` command
  :param flags: additional flags to pass to gsutil
  :return: True if completed successfully otherwise False
  """
    if not src or not isinstance(src, str):
        raise ValueError("invalid src value")
    if "//" in src[:6] and not src.startswith("gs://"):
        raise ValueError("src does not start with gs://")

    if not dest or not isinstance(dest, str):
        raise ValueError("invalid dest value")
    if "//" in src[:6] and not src.startswith("gs://"):
        raise ValueError("dest does not start with gs://")

    cmd = "mv {0}".format(args if args else "").strip()
    pcode, so, se = gcp_gsutil_command(cmd, "{0} {1}".format(src, dest), flags=flags)
    if pcode != 0:
        _logger.error("failed to copy file. ({0}: {1}).".format(pcode, se))
        return False

    return True


def gcp_get_app_versions(running_only: bool = False, sort_by: List[str] = None):
    """
    Get the list of current App Engine services and versions.
    :param running_only: Only showing running versions if True.
    :param sort_by: List of strings specifying what the list of versions should be sorted by
    :return: dict(service_name: dict(version, split, deployed, status))
    """

    args = "versions list"
    if sort_by:
        args += f" --sort-by={','.join(sort_by)}"
    pcode, so, se = gcp_gcloud_command("app", args)

    if pcode != 0 or not so:
        _logger.error("failed to retrieve app services and versions. ({0}: {1}).".format(pcode, se))
        return None

    lines = so.split('\n')
    if not lines or not lines[0].startswith('SERVICE'):
        _logger.error("invalid response when trying retrieve app information. ({0}: {1}).".format(pcode, se))
        return None

    lines.pop(0)

    services = OrderedDict()

    for line in lines:
        if not line:
            continue
        while '  ' in line:
            line = line.replace('  ', ' ')
        parts = line.split(' ')

        name = parts[0]
        if not services.get(name, None):
            services[name] = list()

        if not running_only or (parts[4] == 'SERVING' and float(parts[2]) > 0.0):
            services[name].append({
                'version': parts[1],
                'split': float(parts[2]),
                'deployed': parser.parse(parts[3]),
                'status': parts[4]
            })

    return services


def gcp_delete_versions(service_name: str, version_names: List[str]):
    """
    Delete the versions from the specified service

    :param service_name: Specifies the service to delete the versions from
    :param version_names: Names of the versions to delete
    """

    version_list_str = ' '.join(version_names)
    args = f'versions delete --service={service_name} {version_list_str}'

    exit_code, _, error = gcp_gcloud_command('app', args)
    if exit_code != 0:
        _logger.error(error)
        _logger.error('Failed to delete the versions.')


def gcp_app_services_split_traffic(service: str, versions: list, split_by: str = 'random'):
    """
    Split App Engine traffic between two or more services.  The sum of the split ratios must equal 1.0.
    :param service: Service name to apply traffic splits to.
    :param versions: A list of tuples containing (service name, split ratio).
    :param split_by: Must be one of "ip", "cookie" or "random".
    :return: True if successful, otherwise False
    """

    if not versions or not isinstance(versions, list):
        _logger.error('list of services invalid.')
        return False

    total = 0.0
    splits = ""
    for item in versions:
        if not isinstance(item, tuple) or len(item) != 2:
            _logger.error('service description must be a tuple containing service name and split ratio.')
            return False
        total += float(item[1])
        splits += "{0}={1},".format(item[0], item[1])

    if total != 1.0:
        _logger.error('service splits do not equal 1.0, unable to continue.')
        return False

    args = "--quiet services set-traffic {0}".format(service)
    flags = "--splits {0} --split-by={1}".format(splits[:-1], split_by)

    pcode, so, se = gcp_gcloud_command("app", args, flags)

    if pcode != 0:
        _logger.error("failed to set traffic split. ({0}: {1}).".format(pcode, se))
        return False

    _logger.debug(so if so else se)

    return True


def gcp_deploy_app(project, config_files: list, version: str = None, promote: bool = False):
    """
    Deploy an app to App Engine.
    :param project: project name
    :param config_files: Path to app configuration yaml file.
    :param version: Deploy as different version if needed.
    :param promote: Promote version to serving traffic.
    :return: True if successful, otherwise False.
    """
    if not config_files or not isinstance(config_files, list):
        raise ValueError('Invalid configuration file list argument.')

    configs = ' '.join(config_files)

    args = "--quiet deploy {0}".format(configs)
    if project:
        args += " --project {0}".format(project)
    flags = ''
    if version:
        flags += ' --version "{0}"'.format(version)
    if not promote:
        flags += ' --no-promote'

    pcode, so, se = gcp_gcloud_command("app", args, flags.strip())

    if pcode != 0:
        _logger.error("failed to deploy app. ({0}: {1}).".format(pcode, se))
        return False

    _logger.debug(so if so else se)

    return True

def gcp_restart_instances(project, service=None):
    """
    Restart running instances of an App Engine environment.
    :return: True if successful, else False.
    """

    if service is None:
        service_msg = 'all services'
    else:
        service_msg = f'service "{service}"'
    _logger.debug(f'Restarting instances for project "{project}" and {service_msg}')

    # First get instance ID's
    args = "instances list --format json --project {}".format(project)
    pcode, so, se = gcp_gcloud_command("app", args)

    if pcode != 0 or not so:
        _logger.error("Failed to list running instances. (%s: %s)", pcode, se)

    instance_list = json.loads(so)

    # iterate and delete each instance (you can not pass multiple to old style gcloud)
    # if we ever move to compute engine we can pass a list of instances
    se_list = []
    for instance_json in instance_list:
        instance_id = instance_json['id']
        instance_version = instance_json['version']
        instance_service = instance_json['service']

        if service is None or service == instance_service:
            args = f"instances delete {instance_id} --version={instance_version} " \
                   f"--project={project} --service={instance_service} -q"
            pcode, so, se = gcp_gcloud_command("app", args)
            # this method always sends to se
            se_list.append(se)

    for i in se_list:
        if not i.startswith('Deleting the instance'):
            _logger.warning(i)
            return 1
        else:
            _logger.info(i)
    return 0


def gcp_sql_export_csv(project, sql, destination, replica=True, database=None):
    """
    Create a CSV export from the database and stores the result in a bucket.
    Uses the CLI utility documented at https://cloud.google.com/sdk/gcloud/reference/sql/export/csv

    :param project: Specifies the project (or environment) to use to find the database
    :param sql: SQL statement (as a string) used for retrieving CSV data
    :param destination: Location to save the export file (in the form of gs://bucketName/fileName)
    :param replica: Uses the first replica instance when set to True (defaults to True)
    """

    db_instance = GCP_REPLICA_INSTANCES[project] if replica else GCP_INSTANCES[project]
    db_instance = db_instance.split(':')[-1]

    args = f'{db_instance} {destination} --query="{sql}"'

    if database:
        args += f' --database {database}'

    exit_code, _, error = gcp_gcloud_command('sql export csv', args)
    if exit_code != 0:
        _logger.error(error)
        _logger.error(f'Failed to run sql export to {destination}. Gcloud gave exit code {exit_code}.')


def gcp_monitoring_create_policy(project, policy_file):
    if not policy_file:
        _logger.error("please specify the policy file for creating")
        return False

    args = 'create --policy-from-file {0}'.format(policy_file)

    if project:
        args += ' --project {0}'.format(project)

    pcode, so, se = gcp_gcloud_command('alpha monitoring policies', args, '')

    if pcode != 0:
        _logger.error("failed to create monitoring policies. ({0}: {1}).".format(pcode, se))
        return False

    _logger.info(so)
    _logger.info(se)

    return True


def gcp_monitoring_update_policy(project, policy_file, policy_name):
    if not policy_file or not policy_name:
        _logger.error("please specify the policy name and policy file for updating")
        return False

    args = 'update {0} --policy-from-file {1}'.format(policy_name, policy_file)

    if project:
        args += ' --project {0}'.format(project)

    pcode, so, se = gcp_gcloud_command('alpha monitoring policies', args, '')

    if pcode != 0:
        _logger.error("failed to update monitoring policies. ({0}: {1}).".format(pcode, se))
        return False

    _logger.info(so)
    _logger.info('policy [{0}] has been updated.'.format(policy_name))

    return True


def gcp_monitoring_delete_policy(project, policy_name):
    if not policy_name:
        _logger.error("please specify the policy name for deleting")
        return False

    confirm = input('Are you about to delete policy [{0}] (y/n)? : '.format(policy_name))
    if confirm and confirm.lower().strip() != 'y':
        return False

    args = 'delete {0} {1}'.format(policy_name, '--quiet')
    if project:
        args += ' --project {0}'.format(project)

    pcode, so, se = gcp_gcloud_command('alpha monitoring policies', args, '')

    if pcode != 0:
        _logger.error("failed to delete monitoring policies. ({0}: {1}).".format(pcode, se))
        return False

    _logger.info(so)
    _logger.info(se)

    return True


def gcp_monitoring_list_policy(project, policy_name=None):
    args = 'list'
    if policy_name:
        args = 'describe {0}'.format(policy_name)
    if project:
        args += ' --project {0}'.format(project)
    pcode, so, se = gcp_gcloud_command('alpha monitoring policies', args, '')

    if pcode != 0:
        _logger.error("failed to list monitoring policies. ({0}: {1}).".format(pcode, se))
        return False
    _logger.info(so)

    return True
