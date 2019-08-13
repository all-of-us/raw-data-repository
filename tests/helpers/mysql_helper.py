#
# MySQL Unit Test Helper
#
# The goal is to setup a MySQL instance once for all tests, reset the database any number of times and
# finally do any clean up when Python is ready to exit.
#
#
#
import atexit
import os
import signal
import subprocess
import shlex
import shutil
import tempfile
from time import sleep


from rdr_service.services.system_utils import find_mysqld_executable, get_process_pids, pid_is_running, which, \
                run_external_program

# TODO: In the future, setup a memory disk and set the base path there for faster testing.
BASE_PATH = '{0}/rdr-mysqld'.format(tempfile.gettempdir())


def start_mysql_instance():
    """
    Start a new mysqld service instance for running unittests.  If there is
    already one running, do nothing.
    """
    # Check for a running instance of mysqld
    # pids = get_process_pids(['/mysqld', '--port=9306', '--basedir={0}'.format(BASE_PATH)])
    # if len(pids) == 0:
    pid_file = os.path.join(BASE_PATH, 'mysqld.pid')
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as handle:
            pid = int(handle.read())
        if pid_is_running(pid):
            return

    if os.path.exists(BASE_PATH):
        shutil.rmtree(BASE_PATH)
    os.mkdir(BASE_PATH)

    mysqld = find_mysqld_executable()
    mariadb_install = which('mysql_install_db')
    if not mysqld:
        raise FileNotFoundError('mysqld executable not found')

    data_dir = os.path.join(BASE_PATH, 'data')
    log_file = os.path.join(BASE_PATH, 'mysqld.log')
    tmp_dir = os.path.join(BASE_PATH, 'tmp')
    sock_file = os.path.join(BASE_PATH, 'mysqld.sock')
    os.mkdir(tmp_dir)
    if mariadb_install:
        cmd = '{0} --datadir={1}'.format(mariadb_install, data_dir)
    else:
        cmd = '{0} --initialize-insecure --datadir={1}'.format(mysqld, data_dir)

    # Initialize data directory
    code, so, se = run_external_program(shlex.split(cmd))
    if code != 0:
        raise SystemError(se)

    # Start mysqld service
    cmd = '{0} --port=9306 --datadir={1} --tmpdir={2} --pid-file={3} --log-error={4} --socket={5}'. \
                        format(mysqld, data_dir, tmp_dir, pid_file, log_file, sock_file)
    proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, start_new_session=True)

    sleep(0.5)
    if not proc or not pid_is_running(proc.pid):
        raise OSError('new instance of mysqld service did not start.')
    # Register the stop method
    atexit.register(stop_mysql_instance)


def reset_mysql_instance():

    start_mysql_instance()

    # TODO: Setup database fixtures


def stop_mysql_instance():
    """
    Stop the rdr unittest mysqld service instance.
    """
    pid_file = os.path.join(BASE_PATH, 'mysqld.pid')
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as handle:
            pid = int(handle.read())
        if pid_is_running(pid):
            os.kill(pid, signal.SIGTERM)
