import csv
import logging
from client import Client

from cloudstorage import cloudstorage_api
client = Client()


def _read_csv_lines(filepath):
  with open(filepath, 'r') as f:
    reader = csv.reader(f)
    request_list = [line[0].strip() for line in reader][:30]
    return request_list


def _get_csv_file():
  file_path = '/ptc-uploads-pmi-drc-api-test/ptc_test_participants_generate_fake_data.csv'
  return cloudstorage_api.open(file_path, mode='r')


def generate_data():
  csv_file = _get_csv_file()
  logging.info('file name is %s' % csv_file)
  reader = _read_csv_lines(csv_file)
  # make a list contained within a list so request_json handles it properly.
  request_body = [reader]
  logging.info('requesting pm&b for participant')
  client.request_json('DataGen', 'PUT', request_body)

