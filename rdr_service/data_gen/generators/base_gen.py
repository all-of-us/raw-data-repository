#
# data generator base.
#
import csv
import json
import logging
import os

from glob import glob

_logger = logging.getLogger('rdr_logger')


class BaseGen(object):
  """
  base data generator object.
  """

  _app_data = dict()

  def __init__(self, load_data=True):
    """
    Initialize the QuestionnaireGen object. Try to re-use to save on loading files.
    :param load_data: load data from app_data directory
    """
    if not load_data:
      return

    # Load data files
    data_paths = ['app_data', '..app_data']
    adp = None

    for data_path in data_paths:
      if os.path.exists(os.path.join(os.curdir, data_path)):
        adp = os.path.join(os.curdir, data_path)

    if not adp:
      _logger.error('app data path not found.')
      return

    # load all files in the rest_api/app_data path.
    files = glob(os.path.join(adp, '*'))
    for filename in files:

      if filename.endswith('.txt'):
        key = os.path.basename(filename).replace('.txt', '')
        # decode and encode are required for the cryillic names in the files.
        self._app_data[key] = \
                [line.strip().decode('utf-8').encode('utf-8') for line in open(filename, 'rb').readlines()]

      if filename.endswith('.csv'):
        key = os.path.basename(filename).replace('.csv', '')
        self._app_data[key] = list(csv.DictReader(open(filename)))

      if filename.endswith('.json'):
        key = os.path.basename(filename).replace('.json', '')
        self._app_data[key] = json.loads(open(filename).read())

  def update(self, resp):
    """
    Update this object with response data
    :param resp: request response dict object
    """
    if resp and isinstance(resp, dict):
      for key, value in resp.items():
        self.__dict__[key] = value
      return resp

    _logger.warning('invalid response data, unable to update object.')
