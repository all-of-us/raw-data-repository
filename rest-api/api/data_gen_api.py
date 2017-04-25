import logging
import json

from api_util import nonprod
from config_api import auth_required_config_admin
from data_gen.fake_participant_generator import FakeParticipantGenerator
from data_gen.fake_biobank_samples_generator import FakeBiobankSamplesGenerator
from data_gen.in_process_client import InProcessClient
from flask import request
from flask.ext.restful import Resource
from model.utils import from_client_participant_id

DATE_FORMAT = '%Y-%m-%d'


class DataGenApi(Resource):

  method_decorators = [auth_required_config_admin]

  @nonprod
  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    num_participants = int(resource_json.get('num_participants', 0))
    response = {}
    include_physical_measurements = bool(resource_json.get('include_physical_measurements', False))
    include_biobank_orders = bool(resource_json.get('include_biobank_orders', False))
    if num_participants > 0:
      participant_generator = FakeParticipantGenerator(InProcessClient())
      for _ in range(0, num_participants):
        participant_generator.generate_participant(include_physical_measurements,
                                                   include_biobank_orders)
    biobank_samples_target = resource_json.get('create_biobank_samples', None)
    if biobank_samples_target:
      if biobank_samples_target == 'all':
        num_samples, path = FakeBiobankSamplesGenerator().generate_samples()
        logging.info("Generated %d samples in %s." % (num_samples, path))
        response['num_samples'] = num_samples
        response['samples_path'] = path
      else:
        participant_id = from_client_participant_id(biobank_samples_target)
        num_samples = FakeBiobankSamplesGenerator().generate_samples_for_participant(participant_id)
        response['num_samples'] = num_samples
    return response
