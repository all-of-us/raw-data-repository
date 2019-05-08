import json
import logging
from dateutil.parser import parse

import app_util
from api_util import HEALTHPRO
from app_util import nonprod, get_validated_user_info
from config_api import is_config_admin
from data_gen.fake_biobank_samples_generator import generate_samples
from data_gen.fake_participant_generator import FakeParticipantGenerator
from data_gen.in_process_client import InProcessClient
from flask import request
from flask_restful import Resource
from google.appengine.ext import deferred
from werkzeug.exceptions import Forbidden, BadRequest

# 10% of individual stored samples are missing by default.
_SAMPLES_MISSING_FRACTION = 0.1


def _auth_required_healthpro_or_config_admin(func):
  """A decorator that checks that the caller is a config admin for the app."""
  def wrapped(*args, **kwargs):
    if not is_config_admin(app_util.get_oauth_id()):
      _, user_info = get_validated_user_info()
      if not HEALTHPRO in user_info.get('roles', []):
        logging.info('User has roles {}, but HEALTHPRO or admin is required'.format(
          user_info.get('roles')))
        raise Forbidden()
    return func(*args, **kwargs)
  return wrapped


class DataGenApi(Resource):

  method_decorators = [_auth_required_healthpro_or_config_admin]

  @nonprod
  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    num_participants = int(resource_json.get('num_participants', 0))
    include_physical_measurements = bool(resource_json.get('include_physical_measurements', False))
    include_biobank_orders = bool(resource_json.get('include_biobank_orders', False))
    requested_hpo = resource_json.get('hpo', None)
    if num_participants > 0:
      participant_generator = FakeParticipantGenerator(InProcessClient())
      for _ in range(0, num_participants):
        participant_generator.generate_participant(include_physical_measurements,
                                                   include_biobank_orders,
                                                   requested_hpo)
    if resource_json.get('create_biobank_samples'):
      deferred.defer(
          generate_samples,
          resource_json.get('samples_missing_fraction', _SAMPLES_MISSING_FRACTION))

  @nonprod
  def put(self):
    resource = request.get_data()
    p_id = json.loads(resource)
    participant_generator = FakeParticipantGenerator(InProcessClient(), withdrawn_percent=0,
                                                     suspended_percent=0)

    participant_generator.add_pm_and_biospecimens_to_participants(p_id)


class SpecDataGenApi(Resource):
  """
  API for creating specific fake participant data. Only works with one fake
  participant at a time.
  """
  @nonprod
  def post(self):

    req = json.loads(request.get_data())

    target = req.get('api', None)
    data = req.get('data', None)
    timestamp = req.get('timestamp', None)
    if timestamp:
      timestamp = parse(timestamp)

    if not target:
      raise BadRequest({'status': 'error', 'error': 'target api invalid'})

    result = InProcessClient().request_json(target, 'POST', body=data, pretend_date=timestamp)
    return result

  def put(self):

    req = json.loads(request.get_data())

    target = req.get('api', None)  # this value will usually have the participant_id in it
    data = req.get('data', None)
    timestamp = req.get('timestamp', None)
    if timestamp:
      timestamp = parse(timestamp)

    if not target:
      raise BadRequest({'status': 'error', 'error': 'target api invalid'})

    result = InProcessClient().request_json(target, 'POST', body=data, pretend_date=timestamp)
    return result

  def _process_request(self, method):

    pass
