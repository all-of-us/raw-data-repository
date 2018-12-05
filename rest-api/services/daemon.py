#
# Authors: Robert Abram <robert.abram@entpack.com>
#
# Original author is Sander Marechal <s.marechal@jejik.com>
# http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# !!! This file is python 3.x compliant !!!
#

# Changes:#
#   - Added code to demote daemon process to specified uid and gid
#   - Added debug logging
#   - Re-coded for python 3.x


import atexit
import grp
import logging
import os
import pwd
import sys
import time
from signal import SIGTERM

_logger = logging.getLogger(__name__)


class Daemon(object):
  """
  A generic daemon class.

  Usage: subclass the Daemon class and override the run() method
  """

  def __init__(self, procbase=None, dirmask=None, pidfile=None, uid='nobody', gid='nobody',
               stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    self.procbase = procbase
    self.dirmask = dirmask

    if procbase and pidfile:
      self.pidfile = os.path.join(procbase, pidfile)
    else:
      self.pidfile = pidfile

    self.stdin = stdin
    self.stdout = stdout
    self.stderr = stderr
    self.uid = uid
    self.gid = gid

  def daemonize(self):
    """
    do the UNIX double-fork magic, see Stevens' "Advanced
    Programming in the UNIX Environment" for details (ISBN 0201563177)
    http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
    """

    # Lookup group and user id if we are the root user
    if self.uid != 'nobody' and os.getuid() == 0:
      try:
        groupinfo = grp.getgrnam(self.gid)
        _logger.debug('our group info. n: {0}, i:{1}'.format(groupinfo.gr_name, groupinfo.gr_gid))

      except KeyError:
        _logger.critical('get daemon group id failed')
        sys.exit(1)

      try:
        userinfo = pwd.getpwnam(self.uid)
        _logger.debug('our user info n: {0}, i:{1}'.format(userinfo.pw_name, userinfo.pw_uid))
      except KeyError:
        _logger.critical('get daemon user id failed')
        sys.exit(1)

    try:
      pid = os.fork()
      if pid > 0:
        # exit first parent
        sys.exit(0)
    except os.error as err:
      _logger.critical('fork #1 of double fork failed. ({0}): {1}'.format(err.errno, err.strerror))
      sys.exit(1)

    # decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # do second fork
    try:
      pid = os.fork()
      if pid > 0:
        # exit from second parent
        sys.exit(0)
    except os.error as err:
      _logger.critical('fork #2 of double fork failed. ({0}): {1}'.format(err.errno, err.strerror))
      sys.exit(1)

    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(self.stdin, 'rb')
    so = open(self.stdout, 'a+b')
    se = open(self.stderr, 'a+b')
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # Setup proc base directory and mask
    try:
      os.makedirs(self.procbase, int(self.dirmask, 8))
    except OSError:
      pass

    # write pidfile
    atexit.register(self.delpid)
    pid = str(os.getpid())
    # open(self.pidfile, 'w+').write("%s\n" % pid)
    with open(self.pidfile, 'w+') as handle:
      handle.write("%s\n" % pid)
    os.chmod(self.pidfile, 0o440)

    # If uid != nobody try to demote the process
    if self.uid != 'nobody':
      # Assume that we can only demote the process if we are running as root
      if os.getuid() == 0:
        # Make the procbase directory and pid file are owned by our process user
        os.chown(self.procbase, userinfo.pw_uid, groupinfo.gr_gid)
        os.chown(self.pidfile, userinfo.pw_uid, groupinfo.gr_gid)
        # demote process to 'pki' user and 'secure' group
        os.setgid(groupinfo.gr_gid)
        os.setuid(userinfo.pw_uid)
      else:
        _logger.warning('not running as root, unable to demote process')

  def delpid(self):
    os.remove(self.pidfile)

  def start(self):
    """
    Start the daemon
    """

    if os.path.exists(self.pidfile):
      print('pidfile {0} exists. daemon already running?'.format(self.pidfile))
      return

    # Check for a pidfile to see if the daemon already runs
    pid = None

    if os.path.isfile(self.pidfile):
      try:
        pf = open(self.pidfile, 'r')
        pid = int(pf.read().strip())
        pf.close()
      except os.error:
        pid = None

    if pid:
      _logger.warning('PID file already exists. daemon already running?'.format(self.pidfile))
      self.stop()

    # Start the daemon
    self.daemonize()
    self.run()

  def stop(self):
    """
    Stop the daemon
    """

    if not os.path.exists(self.pidfile):
      print('pidfile {0} does not exist. daemon not running?'.format(self.pidfile))
      return

    # Get the pid from the pidfile
    try:
      pf = open(self.pidfile, 'r')
      pid = int(pf.read().strip())
      pf.close()
    except os.error as err:
      _logger.debug('unknown error when attempting to get pid from pid file. ({0}): {1}'
                    .format(err.errno, err.strerror))
      pid = None

    if not pid:
      _logger.warning('pidfile {0} does not exist. daemon not running?'.format(self.pidfile))
      return  # not an error in a restart

    # Try killing the daemon process
    try:
      while 1:
        os.kill(pid, SIGTERM)
        time.sleep(0.2)
    except os.error as err:
      if err.errno == 3:  # No such process
        if os.path.exists(self.pidfile):
          os.remove(self.pidfile)
      else:
        _logger.error('unknown error when attempting to kill process. ({0}): {1}'
                      .format(err.errno, err.strerror))
        sys.exit(1)

    try:
      os.rmdir(self.procbase)
    except OSError:
      pass

  def restart(self):
    """
    Restart the daemon
    """
    self.stop()
    self.start()

  def run(self):
    """
    You should override this method when you subclass Daemon. It will be called after the
    process has been daemonized by start() or restart().
    """
    pass
