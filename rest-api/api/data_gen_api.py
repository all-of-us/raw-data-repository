import api_util
import json

from api_util import HEALTHPRO
from config_api import auth_required_config_admin
from dao.metrics_dao import MetricsBucketDao
from data_gen.participant_generator import ParticipantGenerator
from data_gen.request_sender import ServerRequestSender
from flask import request
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest

DATE_FORMAT = '%Y-%m-%d'

class DataGenApi(Resource):

  method_decorators = [auth_required_config_admin]

  def __init__(self):
    self.participant_generator = ParticipantGenerator(ServerRequestSender())

  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    num_participants = int(resource_json['num_participants'])
    for _ in range(0, num_participants):
      self.participant_generator.generate_participant()
    return 'OK'
