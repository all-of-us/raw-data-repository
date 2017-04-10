import logging
import json

from api_util import nonprod
from config_api import auth_required_config_admin
from data_gen.fake_participant_generator import FakeParticipantGenerator
from data_gen.fake_biobank_samples_generator import FakeBiobankSamplesGenerator
from data_gen.request_sender import ServerRequestSender
from flask import request
from flask.ext.restful import Resource

DATE_FORMAT = '%Y-%m-%d'

class DataGenApi(Resource):

  method_decorators = [auth_required_config_admin]

  def __init__(self):
    self._participant_generator = FakeParticipantGenerator(ServerRequestSender())

  @nonprod
  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    num_participants = int(resource_json.get('num_participants', 0))
    response = {}
    include_physical_measurements = bool(resource_json.get('include_physical_measurements', False))
    include_biobank_orders = bool(resource_json.get('include_biobank_orders', False))
    for _ in range(0, num_participants):
      self._participant_generator.generate_participant(include_physical_measurements,
                                                       include_biobank_orders)
    if resource_json.get('create_biobank_samples', False):
      num_samples, path = FakeBiobankSamplesGenerator().generate_samples()
      logging.info("Generated %d samples in %s." % (num_samples, path))
      response['num_samples'] = num_samples
      response['samples_path'] = path
    return response
