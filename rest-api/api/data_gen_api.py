import api_util
import executors
import json
import logging

from api_util import nonprod, get_validated_user_info, HEALTHPRO
from config_api import is_config_admin
from data_gen.fake_participant_generator import FakeParticipantGenerator
from data_gen.fake_biobank_samples_generator import generate_samples, FakeBiobankSamplesGenerator
from data_gen.in_process_client import InProcessClient
from flask import request
from flask.ext.restful import Resource
from model.utils import from_client_participant_id
from werkzeug.exceptions import Forbidden

DATE_FORMAT = '%Y-%m-%d'

def auth_required_healthpro_or_config_admin(func):
  """A decorator that checks that the caller is a config admin for the app."""
  def wrapped(*args, **kwargs):
    if not is_config_admin(api_util.get_oauth_id()):
      _, user_info = get_validated_user_info()
      if not HEALTHPRO in user_info.get('roles', []):
        logging.info('User has roles {}, but HEALTHPRO or admin is required'.format(
          user_info.get('roles')))
        raise Forbidden()
    return func(*args, **kwargs)
  return wrapped


class DataGenApi(Resource):

  method_decorators = [auth_required_healthpro_or_config_admin]

  @nonprod
  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    num_participants = int(resource_json.get('num_participants', 0))
    response = {}
    include_physical_measurements = bool(resource_json.get('include_physical_measurements', False))
    include_biobank_orders = bool(resource_json.get('include_biobank_orders', False))
    requested_hpo = resource_json.get('hpo', None)
    if num_participants > 0:
      participant_generator = FakeParticipantGenerator(InProcessClient())
      for _ in range(0, num_participants):
        participant_generator.generate_participant(include_physical_measurements,
                                                   include_biobank_orders,
                                                   requested_hpo)
    biobank_samples_target = resource_json.get('create_biobank_samples', None)
    if biobank_samples_target:
      if biobank_samples_target == 'all':
        executors.defer(generate_samples)
      else:
        participant_id = from_client_participant_id(biobank_samples_target)
        num_samples = FakeBiobankSamplesGenerator().generate_samples_for_participant(participant_id)
        response['num_samples'] = num_samples
    return response
