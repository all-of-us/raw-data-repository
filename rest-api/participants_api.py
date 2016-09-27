"""The API definition file for the participants API.

This defines the APIs and the handlers for the APIs.
"""

import datetime
import uuid

import api_util
import evaluation
import participant

from protorpc import message_types
from protorpc import messages

from google.appengine.ext import ndb
from flask import Flask, request
from flask.ext.restful import Resource, reqparse, abort
from werkzeug.exceptions import BadRequest, NotFound

class ParticipantGetAPI(Resource):
  def get(self, p_id):
    api_util.check_auth()
    p = ndb.Key(participant.Participant, p_id).get()
    if not p:
      raise NotFound('Participant with id {} not found.', p_id)
    return participant.to_json(p)

class ParticipantListAPI(Resource):
  def get(self):
    api_util.check_auth()
    # In order to do a query, at least the last name and the birthdate must be
    # specified.
    last_name = request.args.get('last_name', None)
    date_of_birth = request.args.get('date_of_birth', None)
    first_name = request.args.get('first_name', None)
    if not last_name or not date_of_birth:
      raise BadRequest(
          'Last name and date of birth must be specified.')
    return participant.list(first_name, last_name, date_of_birth)

class ParticipantInsertAPI(Resource):
  def post(self):
    api_util.check_auth()

    resource = request.get_json(force=True)
    p = participant.from_json(resource, id=str(uuid.uuid4()))
    if not p.sign_up_time:
      p.sign_up_time = datetime.datetime.now()

    p.put()
    return participant.to_json(p)

class ParticipantUpdateAPI(Resource):
  def put(self, p_id):
    api_util.check_auth()
    old_p = ndb.Key(participant.Participant, p_id).get()
    new_p = participant.from_json(request.get_json(force=True))
    api_util.update_model(old_model=old_p, new_model=new_p)
    old_p.put()
    return participant.to_json(old_p)


class EvaluationListAPI(Resource):
  def get(self, p_id):
    api_util.check_auth()
    return evaluation.list(p_id)

class EvaluationInsertAPI(Resource):
  def post(self, p_id):
    api_util.check_auth()
    resource = request.get_json(force=True)
    if 'evaluation_id' in resource:
      id = resource['evaluation_id']
    else:
      id = str(uuid.uuid4())
    e = evaluation.from_json(resource, p_id, id)
    e.put()
    return evaluation.to_json(e)

class EvaluationUpdateAPI(Resource):
  def put(self, p_id, e_id):
    api_util.check_auth()
    old_e = ndb.Key(
        participant.Participant, p_id, evaluation.Evaluation, e_id).get()
    new_e = evaluation.from_json(request.get_json(force=True), p_id, e_id)
    api_util.update_model(old_model=old_e, new_model=new_e)
    old_e.put()
    return evaluation.to_json(old_e)

class EvaluationGetAPI(Resource):
  def get(self, p_id, e_id):
    api_util.check_auth()
    e = ndb.Key(participant.Participant, p_id,
                evaluation.Evaluation, e_id).get()
    if not e:
      raise NotFound(
          'Evaluation with participant id {} and id {} not found.'.format(
              p_id, e_id))
    return evaluation.to_json(e)
