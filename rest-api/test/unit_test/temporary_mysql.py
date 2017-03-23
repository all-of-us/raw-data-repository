import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time

from dao import database_factory
import singletons


_READY_STR = 'ready for connections'
_FAILED_STR = 'Shutdown complete'


class TemporaryMysql(object):
  def __init__(self):
    self.__mysql_proc = None
    self.__mysql_data_path = None

  def start_and_install(self):
    self.__mysql_data_path = tempfile.mkdtemp()
    mysql_socket_path = os.path.join(self.__mysql_data_path, 'mysql.socket')
    mysql_pid_path = os.path.join(self.__mysql_data_path, 'mysql.pid')
    log_file_path = os.path.join(self.__mysql_data_path, 'stderr.log')
    self.__out_log_file = open(log_file_path, 'w')
    self.__mysql_proc = subprocess.Popen([
        'mysqld',
        '--datadir=' + self.__mysql_data_path,
        '--pid-file=' + mysql_pid_path,
        '--socket=' + mysql_socket_path,
        '--skip-networking',
        '--skip-grant-tables'], stdout=self.__out_log_file, stderr=self.__out_log_file)
    print (
        'Temporary MySQL starting, PID %d, stderr to %r.'
        % (self.__mysql_proc.pid, log_file_path))
    with open(log_file_path) as log_file:
      while True:
        text = log_file.readline()
        if text:
          print 'mysql>', text.rstrip()
        if _READY_STR in text:
          break
        elif _FAILED_STR in text:
          raise RuntimeError('MySQL startup failed.')
    print 'Temporary MySQL ready.'

    try:
      singletons.reset_for_tests()
      database_factory.DB_CONNECTION_STRING = (
          'mysql+mysqldb://root@localhost/?unix_socket=%s&charset=utf8' % (mysql_socket_path))
      self.reset()

      singletons.reset_for_tests()
      database_factory.DB_CONNECTION_STRING = (
          'mysql://root@localhost/%s?unix_socket=%s&charset=utf8'
          % (database_factory.DB_NAME, mysql_socket_path))
      print 'Temporary MySQL db set up at %r.' % database_factory.DB_CONNECTION_STRING
    except:
      self.__mysql_proc.terminate()
      self.__mysql_proc = None
      raise

  @staticmethod
  def reset():
    # Note, in database_test after a while / in some condition, executing the below hangs.
    db = database_factory.get_database()
    db.get_engine().execute('DROP DATABASE IF EXISTS %s' % database_factory.DB_NAME)
    db.get_engine().execute('CREATE DATABASE %s' % database_factory.DB_NAME)
    db.get_engine().execute('USE %s' % database_factory.DB_NAME)

  def stop(self):
    if self.__mysql_proc is not None:
      self.__mysql_proc.terminate()
      self.__mysql_proc.wait()
      self.__mysql_proc = None
    if self.__mysql_data_path is not None:
      shutil.rmtree(self.__mysql_data_path)
      self.__out_log_file.close()
      self.__mysql_data_path = None
    print 'Temporary MySQL shut down.'
