# -*- coding: utf-8 -*-
#
# Small helper functions for system services
#
# !!! This file is python 3.x compliant !!!
#

from dataclasses import dataclass
from typing import List, Optional

import gettext
import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from datetime import datetime, date
from json import JSONDecodeError

try:
    import requests
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass

_logger = logging.getLogger("rdr_logger")


class JSONObject:
    """ Simple object to recursively convert a dict or json string to object properties """
    __data_dict__ = None

    def __init__(self, data):

        if isinstance(data, str):
            data = json.loads(data)

        if data:
            for k, v in data.items():
                if isinstance(v, dict):
                    self.__dict__[k] = JSONObject(v)
                elif isinstance(v, list):
                    _tmp = list()
                    for i in v:
                        if isinstance(i, dict):
                            _tmp.append(JSONObject(i))
                        elif isinstance(i, str):
                            try:
                                _tmp_data = json.loads(i)
                                if _tmp_data:
                                    _tmp.append(JSONObject(_tmp_data))
                                else:
                                    _tmp.append(i)  # For when the value is a string = 'null'.
                            except JSONDecodeError:
                                _tmp.append(i)
                        else:
                            _tmp.append(i)
                    self.__dict__[k] = _tmp
                else:
                    self.__dict__[k] = v

        self.__data_dict__ = data
        self.__data_str__ = json.dumps(data, default=self.json_serial)

    def json_serial(self, obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return obj.__repr__()

    def to_json(self, indent=None):
        """ Export config as json """
        if indent:
            return json.dumps(self.__data_dict__, default=self.json_serial, indent=indent)
        return self.__data_str__

    def to_dict(self):
        """ Export config as json """
        return self.__data_dict__

    def __repr__(self):
        return self.__data_str__


class TerminalColors(object):
    """
    Simple class for setting terminal colors.
    https://en.wikipedia.org/wiki/ANSI_escape_code
    """

    reset = '\033[0m'
    bold = '\033[1m'
    underline = '\033[4m'

    fg_black = '\033[38;5;0m'
    fg_red = '\033[38;5;1m'
    fg_green = '\033[38;5;2m'
    fg_yellow = '\033[38;5;3m'
    fg_blue = '\033[38;5;4m'
    fg_magenta = '\033[38;5;5m'
    fg_cyan = '\033[38;5;6m'
    fg_white = '\033[38;5;7m'

    fg_bright_black = '\033[38;5;8m'
    fg_bright_red = '\033[38;5;9m'
    fg_bright_green = '\033[38;5;10m'
    fg_bright_yellow = '\033[38;5;11m'
    fg_bright_blue = '\033[38;5;12m'
    fg_bright_magenta = '\033[38;5;13m'
    fg_bright_cyan = '\033[38;5;14m'
    fg_bright_white = '\033[38;5;15m'

    bg_black = '\033[48;5;0m'
    bg_red = '\033[48;5;1m'
    bg_green = '\033[48;5;2m'
    bg_yellow = '\033[48;5;3m'
    bg_blue = '\033[48;5;4m'
    bg_magenta = '\033[48;5;5m'
    bg_cyan = '\033[48;5;6m'
    bg_white = '\033[48;5;7m'

    bg_bright_black = '\033[48;5;8m'
    bg_bright_red = '\033[48;5;9m'
    bg_bright_green = '\033[48;5;10m'
    bg_bright_yellow = '\033[48;5;11m'
    bg_bright_blue = '\033[48;5;12m'
    bg_bright_magenta = '\033[48;5;13m'
    bg_bright_cyan = '\033[48;5;14m'
    bg_bright_white = '\033[48;5;15m'

    _default_format = ''
    _default_background = ''
    _default_foreground = ''

    def custom_fg_color(self, index: int) -> str:
        """
        Get a custom color seq
        :param index: intger 0 - 255
        :return: string
        """
        return f'\033[38;5;{index}m'

    def custom_bg_color(self, index: int) -> str:
        """
        Get a custom color seq
        :param index: intger 0 - 255
        :return: string
        """
        return f'\033[48;5;{index}m'

    def set_default_formatting(self, *args):
        """
        Set the default colors for formatting.
        :param args: list of colors.
        """
        self._default_format = ''
        for arg in args:
            self._default_format += arg

    def set_default_background(self, *args):
        """
        Set default background colors
        :param args: list of colors
        """
        self._default_background = ''
        for arg in args:
            self._default_background += arg

    def set_default_foreground(self, *args):
        """
        Set default foreground colors
        :param args: list of colors
        """
        self._default_foreground = ''
        for arg in args:
            self._default_foreground += arg


    def fmt(self, line: str, *args) -> str:
        """
        Color a line of text
        :param line: string
        :param args: list of colors.
        :return: string
        """
        if not args:
            l = self._default_format
        else:
            l = ''
            for arg in args:
                l += arg

        l += str(line)
        l += self.reset

        l += self._default_background
        l += self._default_foreground

        return l


class _ToolLoggingFormatter(logging.Formatter):
    """
    Add colorization to logging messages.
    """

    _color = TerminalColors()

    def format(self, record):
        msg = super(_ToolLoggingFormatter, self).format(record)
        if record.levelno == logging.DEBUG:
            msg = self._color.fmt(msg, self._color.fg_cyan)
        elif record.levelno == logging.ERROR:
            msg = self._color.fmt(msg, self._color.fg_bright_red)
        elif record.levelno == logging.WARNING:
            msg = self._color.fmt(msg, self._color.fg_bright_yellow)

        return msg


def setup_logging(logger, progname, debug=False, logfile=None):
    """
  Setup Python logging
  :param logger: Handle to logger object
  :param progname: Name of application or service
  :param debug: True if Debugging enabled
  :param logfile: Path and filename to log file to output to
  :return: Nothing
  """
    if not logger:
        return False

    # Set our logging options and formatter now that we have the program arguments.
    if debug:
        logging.basicConfig(filename=os.devnull, datefmt="%Y-%m-%d %H:%M:%S", level=logging.DEBUG)
        formatter = _ToolLoggingFormatter("%(asctime)s {0}: %(levelname)s: %(message)s".format(progname))
    else:
        logging.basicConfig(filename=os.devnull, datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)
        # formatter = logging.Formatter('%(levelname)s: {0}: %(message)s'.format(progname))
        formatter = _ToolLoggingFormatter("%(message)s")

    # Setup stream logging handler
    handler = logging.StreamHandler(sys.stdout)
    handler.flush = sys.stdout.flush
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Setup file logging handler
    if logfile:

        # make sure the path exists
        logpath = os.path.dirname(os.path.abspath(os.path.expanduser(logfile)))

        if not os.path.exists(logpath):
            os.makedirs(logpath)

        handler = logging.FileHandler(logfile)
        handler.setFormatter(logging.Formatter("%(asctime)s {0}: %(levelname)s: %(message)s".format(progname)))

        logger.addHandler(handler)


def setup_i18n():
    """
  Enable i18n internationalization support for python programs
  """
    # Setup i18n - Good for 2.x and 3.x python.
    kwargs = {}
    if sys.version_info[0] < 3:
        kwargs["unicode"] = True
    gettext.install("sys_update", **kwargs)


def which(program):
    """
  Find the path for a given program
  http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
  :param program: name of executable file to find
  """

    try:
        path_spec = os.environ["PATH"]
    except KeyError:
        # for weird times when we don't have a good environment
        path_spec = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin:/root/bin"

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    # pylint: disable=unused-variable
    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in path_spec.split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None


def run_external_program(args, cwd=None, env=None, shell=False, debug=False):
    """
  Run an external program, arguments
  :param args: program name plus arguments in a list
  :param cwd: Current working directory
  :param env: A modified environment to use
  :param shell: Use shell to execute command
  :param debug: Add '--debug' to args if '--debug' is in sys.argv
  :return: exit code, stdoutdata, stderrdata
  """

    if not args:
        _logger.debug("run_external_program: bad arguments")
        return -1, None, None

    if debug is True and "--debug" in sys.argv and "--debug" not in args:
        args.append("--debug")

    _logger.debug("external: {0} {1}".format(os.path.basename(args[0]), ' '.join(args[1:])))

    p = subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, shell=shell)
    stdoutdata, stderrdata = p.communicate()
    p.wait()

    if isinstance(stdoutdata, (bytes, bytearray)):
        stdoutdata = stdoutdata.decode("utf-8")
    if isinstance(stderrdata, (bytes, bytearray)):
        stderrdata = stderrdata.decode("utf-8")

    return p.returncode, stdoutdata, stderrdata


def list_chunks(lst, chunk_size):
    """
    Yield successive chunk_size sublists from lst.  Useful for tools/process that need to adhere to size limits
    when processing large lists of items (e.g., batching groups of records to be processed by a cloud task).  Example:
            for batch_of_ids in list_chunks(big_list_of_ids, chunk_size):
                <process the chunk-sized sublist of ids>
    :param lst:  The list object to be broken into chunks
    :param chunk_size:  int value of the desired sublist/chunk size
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def is_valid_email(email):
    """
  Validate email parameter is a valid formatted email address
  :param email: string containing email address
  :return: True if email is valid otherwise False
  """
    if not email:
        return False

    return bool(re.match("^.+@(\[?)[a-zA-Z0-9-.]+.([a-zA-Z]{2,3}|[0-9]{1,3})(]?)$", email))


def signal_process(name, signal_code=signal.SIGHUP):
    """
  Send a signal to a program
  :param name: name of the executable, not a systemd service name
  :param signal_code: signal code from signal object
  """
    pid = None

    prog = which("pidof")

    if not prog:
        _logger.error('unable to locate "pidof" executable.')
        return False

    # Get the process id
    try:

        args = shlex.split("{0} {1}".format(prog, name))
        pid = int(subprocess.check_output(args).decode("utf-8").strip())
    except subprocess.CalledProcessError:
        _logger.error("failed to get program pid ({0})".format(name))

    # Send signal to process
    if pid:
        os.kill(pid, signal_code)
        _logger.debug("send signal to {0} complete".format(name))
        return True

    _logger.debug("send signal to {0} failed".format(name))

    return False


def pid_is_running(pid: int):
    """
  Check For the existence of a unix pid.
  :param pid: integer ID of this process
  :return: True if process with this ID is running, otherwise False
  """
    # See if there is a currently running mysqld instance
    # pylint: disable=unused-variable
    args = ['ps', '-eo', 'ruid,pid,ppid,args']
    code, so, se = run_external_program(args=args)
    if code == 0:
        lines = so.split('\n')
        for line in lines:
            if str(pid) in line:
                while '  ' in line:
                    line = line.replace('  ', ' ')
                if pid == int(line.strip().split(' ')[1]) and '<defunct>' not in line:
                    return True
    return False


def get_process_pids(matches: list) -> list:
    """
    Get the pids of a currently running process.  May match more than one running process.
    :param matches: parts of process command to match in process list
    :return: List of PIDs
    """
    pids = list()

    if not matches or len(matches) == 0:
        raise ValueError('matches list may not be empty.')

    for match in matches:
        if not isinstance(match, str):
            raise ValueError('invalid match value, must be string.')

    # See if there is a currently running mysqld instance
    # pylint: disable=unused-variable
    args = ['ps', '-ef']
    code, so, se = run_external_program(args=args)
    if code == 0:
        lines = so.split('\n')
        for line in lines:
            no_match = False
            for match in matches:
                if match not in line:
                    no_match = True
                    break

            if no_match is True:
                continue
            while '  ' in line:
                line = line.replace('  ', ' ')
            pids.append(int(line.split(' ')[1]))

    return pids


def write_pidfile_or_die(progname: str, pid_file: str = None):
    """
  Attempt to write our PID to the given PID file or raise an exception.
  :param progname: Name of this program
  :param pid_file: an alternate path and pid file to use
  :return: pid path and filename
  """
    if not pid_file:
        home = os.path.expanduser("~")
        pid_path = os.path.join(home, ".local/run")
        pid_file = os.path.join(pid_path, "{0}.pid".format(progname))
    else:
        pid_path = os.path.dirname(pid_file)

    if not os.path.exists(pid_path):
        os.makedirs(pid_path)

    if os.path.exists(pid_file):
        pid = int(open(pid_file).read())

        if pid_is_running(pid):
            _logger.warning("program is already running, aborting.")
            raise SystemExit

        else:
            os.remove(pid_file)

    open(pid_file, "w").write(str(os.getpid()))

    return pid_file


def remove_pidfile(progname: str, pid_file: str = None):
    """
  Remove the PID file for the given program
  :param progname: Name of this program
  :param pid_file: an alternate pid file to use
  """
    if not pid_file:
        home = os.path.expanduser("~")
        pid_path = os.path.join(home, ".local/run")
        pid_file = os.path.join(pid_path, "{0}.pid".format(progname))

    if os.path.exists(pid_file):
        os.remove(pid_file)


def json_datetime_handler(x):
    """ Format datetime objects for the json engine """
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


def make_api_request(
    host, api_path, headers=None, cookies=None, timeout=60, req_type="GET", json_data=None, ret_type="json"
):
    """
  contact the primary and check for updated records
  :param host: host name or ip address
  :param api_path: url path
  :param headers: list of headers
  :param cookies: list of cookies
  :param timeout: request timeout in seconds
  :param req_type: request type
  :param json_data: json data to pass with a POST, PUT, PATCH request type
  :param ret_type: expected return data type from request, default 'json'.
  :return: response code, response data
  """
    resp_data = None

    if api_path.startswith("/"):
        api_path = api_path[1:]

    # Do not use https for local system requests
    protocol = "http" if "127.0.0.1" in host or "localhost" in host else "https"
    url = "{0}://{1}/{2}".format(protocol, host, api_path)

    try:

        if req_type.lower() == "get":
            rq = requests.get(url, timeout=timeout, headers=headers, cookies=cookies, verify=False)
        elif req_type.lower() == "post":
            rq = requests.post(url, json=json_data, timeout=timeout, headers=headers, cookies=cookies, verify=False)
        elif req_type.lower() == "put":
            rq = requests.put(url, json=json_data, timeout=timeout, headers=headers, cookies=cookies, verify=False)
        elif req_type.lower() == "patch":
            rq = requests.patch(url, json=json_data, timeout=timeout, headers=headers, cookies=cookies, verify=False)
        elif req_type.lower() == "delete":
            rq = requests.delete(url, timeout=timeout, headers=headers, cookies=cookies, verify=False)
        else:
            return -1, None

    except requests.Timeout:
        resp_code = requests.codes.request_timeout
        resp_data = "remote api request timed out."
        _logger.error(resp_data)
    except requests.ConnectionError:
        resp_code = requests.codes.service_unavailable
        resp_data = "remote connection error."
        _logger.error(resp_data)
    except requests.RequestException:
        resp_code = requests.codes.service_unavailable
        resp_data = "remote api request failed."
        _logger.error(resp_data)
    else:

        resp_code = rq.status_code

        if rq.status_code == requests.codes.ok or rq.status_code == requests.codes.created:
            try:
                if ret_type == "json":
                    resp_data = rq.json()
                else:
                    resp_data = rq.text
            except ValueError:
                pass
        else:
            resp_data = "{0} [{1}]".format(rq.reason, rq.text.strip())
            _logger.debug(resp_data)

    return resp_code, resp_data


def print_progress_bar(iteration, total, prefix="", suffix="", decimals=1, bar_length=90, fill="█"):
    """
  Call in a loop to create terminal progress bar.
  https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
  https://gist.github.com/aubricus/f91fb55dc6ba5557fbab06119420dd6a
  :param iteration: Required  : current iteration (Int)
  :param total: Required  : total iterations (Int)
  :param prefix: Optional  : prefix string (Str)
  :param suffix: Optional  : suffix string (Str)
  :param decimals: Optional  : positive number of decimals in percent complete (Int)
  :param bar_length: Optional  : character length of bar (Int)
  :param fill: Optional  : bar fill character (Str)
  """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = fill * filled_length + "-" * (bar_length - filled_length)

    sys.stdout.write("\r{0} [{1}] {2}{3} {4}".format(prefix, bar, percents, "%", suffix))

    if iteration == total:
        sys.stdout.write("\n")
    sys.stdout.flush()


def find_mysqld_executable() -> str:
    """
    Find local mysql server executable
    :return: Executable path or None
    """
    KNOWN_PATHS = [
        '/usr/sbin/mysqld',
        '/usr/libexec/mysqld',
        '/var/run/mysqld/mysql',
        '/usr/local/opt/mysql@5.7/bin/mysqld',
        '/usr/local/opt/mysql@8.0/bin/mysqld'
    ]
    # Try known paths
    for path in KNOWN_PATHS:
        if os.path.exists(path):
            return path

    # Try which()
    path = which('mysqld')
    if path:
        return path

    # See if there is a currently running mysqld instance
    # pylint: disable=unused-variable
    args = ['ps', '-ef']
    code, so, se = run_external_program(args=args)
    if code == 0:
        lines = so.split('\n')
        for line in lines:
            if '/mysqld' in line and '--basedir=' in line:
                path = line[line.find('/'):]
                return path.split(' ')[0]

    return ''


def start_mysqld_instance(basedir: str) -> int:
    """
    Launch a new instance of mysqld, only if it is not already running.
    https://dev.mysql.com/doc/refman/8.0/en/multiple-servers.html
    :param basedir: Path to run the new instance in, must be unique.
    :return: PID
    """
    # See if there is a currently running mysqld instance
    # pylint: disable=unused-argument
    # pylint: disable=unused-variable
    args = ['ps', '-ef']
    code, so, se = run_external_program(args=args)
    if code == 0:
        lines = so.split('\n')
        for line in lines:
            if '/mysqld' in line and '--basedir=' in line:
                path = line[line.find('/'):]
                return path.split(' ')[0]


def git_project_root(path=None):
    """
    Figure out the git project top level directory.
    :param path: optional: path to check.
    :return: Git project root path or None
    """
    cwd = os.curdir
    if path:
        if not os.path.exists(path):
            raise ValueError('Invalid directory path argument')
        os.chdir(path)

    args = ['git', 'rev-parse', '--show-toplevel']
    # pylint: disable=unused-variable
    code, so, se = run_external_program(args=args)

    os.chdir(cwd)

    if code == 0:
        return so.strip()

    return None

def git_current_branch():
    """
    Get the currently checked out branch.
    :return: Git branch name.
    """
    args = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
    # pylint: disable=unused-variable
    code, so, se = run_external_program(args=args)

    if code == 0:
        return so.strip()

    return None

def git_current_tag():
    """
    Get the current git tag
    :return: Git tag
    """
    args = ['git', 'describe', '--tags']
    # pylint: disable=unused-variable
    code, so, se = run_external_program(args=args)

    if code == 0:
        return so.strip()

    return None

def git_checkout_branch(branch):
    """
    Change current branch to the given branch.
    :param branch: git branch name to checkout.
    :return: True if successful otherwise False
    """
    args = ['git', 'checkout', branch]
    # pylint: disable=unused-variable
    code, so, se = run_external_program(args=args)

    if code == 0:
        return True

    _logger.error(se if se else so)

    return False

def is_git_branch_clean():
    """
    Does the current branch have any un-committed changes.
    :return: True if successful otherwise False
    """
    args = ['git', 'status', '--porcelain']
    # pylint: disable=unused-variable
    code, so, se = run_external_program(args=args)

    if code == 0 and not so:
        return True

    return False

def is_valid_release_git_tag(git_tag):
    """
    Does the git tag exist in RDR github repository and does it
    conform to RDR release semantic version (X.Y.Z)
    :return:  True if tag is validated otherwise False
    """
    if not git_tag:
        return False

    # Semantic version validation (X.Y.Z, all positive integers, no leading zeroes)
    if not re.match(r"[1-9]+[0-9]*\.[1-9]+[0-9]*\.[1-9]+[0-9]*$", git_tag):
        _logger.error(f'Nonconforming release tag: {git_tag}')
        return False

    # Check for 'git ls-remote' output confirming tag exists
    args = ['git', 'ls-remote', '--tags', 'origin', git_tag]
    code, so, se = run_external_program(args=args)

    if code == 0 and len(so):
        return True
    else:
        _logger.error(se if se else f'Git tag {git_tag} not found on remote origin')

    return False


def retry_func(func, retries=25, backoff_amount=2.0, **kwargs):
    """
    Retry a function call multiple times, delaying more each retry.
    :param func: function to retry
    :param retries: Integer, number of retries
    :param backoff_amount: Float, number of seconds to add to backoff for next retry.
    :return: function return
    """
    result = None
    count = abs(retries)
    backoff = 0.1
    while count >= 0:
        try:
            result = func(**kwargs)
            break
        except Exception as e:  # pylint: disable=broad-except
            if count == 0:
                raise e
            time.sleep(backoff)
            count -= 1
            backoff += backoff_amount
    return result


def min_or_none(value_list):
    min_found = None
    for val in value_list:
        if(
            min_found is None
            or (
                val is not None
                and min_found > val
            )
        ):
            min_found = val

    return min_found


@dataclass
class DateRange:
    """
    Convenience class for representing a date range.
    """

    start: datetime
    end: datetime = None

    def find_first_overlap(self, other: datetime) -> Optional[datetime]:
        """
        The other date is considered an unending range.
        If this range overlaps, return the first date of that overlap. Otherwise return None.
        """
        if other is None:
            # No overlap since the other date is None
            return None

        if self.end is None:
            # Both date ranges are unending, the overlap starts at the latest date
            return max(self.start, other)

        if self.end < other:
            # If the date range ends before the other, then there isn't an overlap
            return None

        # The date range ends after the other, the overlap starts at the other date
        return other

    def find_first_overlap_list(self, date_list: List[datetime]):
        """
        The provided dates are considered unending ranges.
        Return the start of where they all overlap.
        """
        if any(other_date is None for other_date in date_list):
            # If any of the given dates is None, then there can't be a place where they all overlap
            return None

        # The overlap would start at the latest date given
        return self.find_first_overlap(max(date_list))
