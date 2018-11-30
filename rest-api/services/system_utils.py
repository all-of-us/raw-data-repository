#
# Authors: Robert Abram <rabram991@gmail.com>
#
# Small helper functions for system services
#
# !!! This file is python 3.x compliant !!!
#

import gettext
import logging
import os
import re
import signal
import subprocess
import sys
from datetime import datetime

_logger = logging.getLogger(__name__)


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

  logdir = '/tmp/logging'

  if not os.path.exists(logdir):
    os.mkdir(logdir)

  # Set our logging options now that we have the program arguments.
  if debug:
    logging.basicConfig(filename=os.devnull,
                        datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
    # Setup logging formatter
    formatter = logging.Formatter('%(asctime)s {0}: %(levelname)s: %(message)s'.format(progname))
  else:
    logging.basicConfig(filename=os.devnull,
                        datefmt='%Y-%m-%d %H:%M:%S', level=logging.WARNING)
    # Setup logging formatter
    formatter = logging.Formatter('%(levelname)s: {0}: %(message)s'.format(progname))

  # Setup stream logging handler
  handler = logging.StreamHandler(sys.stdout)
  handler.setFormatter(formatter)

  logger.addHandler(handler)

  if not os.path.exists(logdir):
    os.mkdir(logdir)

  # Setup file logging handler
  if logfile:
    handler = logging.FileHandler(os.path.join(logdir, logfile))
    handler.setFormatter(formatter)

    logger.addHandler(handler)


def setup_unicode():
  """
  Enable unicode support for python programs
  """
  # Setup i18n - Good for 2.x and 3.x python.
  kwargs = {}
  if sys.version_info[0] < 3:
    kwargs['unicode'] = True
  gettext.install('sys_update', **kwargs)


def which(program):
  """
  Find the path for a given program
  http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
  """

  try:
    path_spec = os.environ['PATH']
  except KeyError:
    # for weird times when we don't have a good environment
    path_spec = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin:/root/bin'

  def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

  # pylint: disable=W0612
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
    _logger.debug('run_external_program: bad arguments')
    return -1, None, None

  if debug is True and '--debug' in sys.argv and '--debug' not in args:
    args.append('--debug')

  _logger.debug('external: {0}'.format(os.path.basename(args[0])))

  p = subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE, env=env, shell=shell)
  stdoutdata, stderrdata = p.communicate()
  p.wait()

  return p.returncode, stdoutdata, stderrdata


def is_valid_email(email):
  return bool(re.match("^.+@(\[?)[a-zA-Z0-9-.]+.([a-zA-Z]{2,3}|[0-9]{1,3})(]?)$", email))


def signal_process(name, signal_code=signal.SIGHUP):
  """
  Send a signal to a program
  :param name: name of the executable, not a systemd service name
  :param signal_code: signal code from signal object
  """
  pid = None

  # Get the process id
  try:
    pid = int(subprocess.check_output(["pidof", name]).decode('utf-8').strip())
  except subprocess.CalledProcessError:
    _logger.error('failed to get program pid ({0})'.format(name))

  # Send signal to process
  if pid:
    os.kill(pid, signal_code)
    _logger.debug('send signal to {0} complete'.format(name))
    return True

  _logger.debug('send signal to {0} failed'.format(name))

  return False


def pid_is_running(pid):
  """
  Check For the existence of a unix pid.
  :param pid: integer ID of this process
  :return: True if process with this ID is running, otherwise False
  """
  try:
    os.kill(pid, 0)
  except OSError:
    return False

  return True


def write_pidfile_or_die(progname):
  """
  Attempt to write our PID to the given PID file
  :param progname: Name of this program
  :return:
  """

  pidfile = '/tmp/{0}.pid'.format(progname)

  if os.path.exists(pidfile):
    pid = int(open(pidfile).read())

    if pid_is_running(pid):
      _logger.warning('program is already running, aborting.')
      raise SystemExit

    else:
      os.remove(pidfile)

  open(pidfile, 'w').write(str(os.getpid()))

  return pidfile


def remove_pidfile(progname):
  """
  Remove the PID file for the given program
  :param progname: Name of this program
  :return: None
  """

  pidfile = '/tmp/{0}.pid'.format(progname)
  os.remove(pidfile)


def json_datetime_handler(x):
  """ Format datetime objects for the json engine """
  if isinstance(x, datetime):
    return x.isoformat()
  raise TypeError("Unknown type")
