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

class ParticipantAPI(Resource):
  @api_util.auth_required
  def get(self, p_id=None):
    if not p_id:
      return self.list()
    p = ndb.Key(participant.Participant, p_id).get()
    if not p:
      raise NotFound('Participant with id {} not found.', p_id)
    return participant.to_json(p)

  @api_util.auth_required
  def list(self):
    # In order to do a query, at least the last name and the birthdate must be
    # specified.
    last_name = request.args.get('last_name', None)
    date_of_birth = request.args.get('date_of_birth', None)
    first_name = request.args.get('first_name', None)
    zip_code = request.args.get('zip_code', None)
    if not last_name or not date_of_birth:
      raise BadRequest(
          'Last name and date of birth must be specified.')
    return participant.list(first_name, last_name, date_of_birth, zip_code)

  @api_util.auth_required
  def post(self):
    resource = request.get_json(force=True)
    p = participant.from_json(resource, id=str(uuid.uuid4()))
    if not p.sign_up_time:
      p.sign_up_time = datetime.datetime.now()

    p.put()
    return participant.to_json(p)

  @api_util.auth_required
  def patch(self, p_id):
    old_p = ndb.Key(participant.Participant, p_id).get()
    new_p = participant.from_json(request.get_json(force=True))
    api_util.update_model(old_model=old_p, new_model=new_p)
    old_p.put()
    return participant.to_json(old_p)


class EvaluationAPI(Resource):
  @api_util.auth_required
  def get(self, p_id, e_id=None):
    if not e_id:
      return self.list(p_id)
    e = ndb.Key(participant.Participant, p_id,
                evaluation.Evaluation, e_id).get()
    if not e:
      raise NotFound(
          'Evaluation with participant id {} and id {} not found.'.format(
              p_id, e_id))
    return evaluation.to_json(e)

  @api_util.auth_required
  def list(self, p_id):
    return evaluation.list(p_id)

  @api_util.auth_required
  def post(self, p_id):
    resource = request.get_json(force=True)
    if 'evaluation_id' in resource:
      id = resource['evaluation_id']
    else:
      id = str(uuid.uuid4())
    e = evaluation.from_json(resource, p_id, id)
    e.put()
    return evaluation.to_json(e)

  @api_util.auth_required
  def patch(self, p_id, e_id):
    old_e = ndb.Key(
        participant.Participant, p_id, evaluation.Evaluation, e_id).get()
    new_e = evaluation.from_json(request.get_json(force=True), p_id, e_id)
    api_util.update_model(old_model=old_e, new_model=new_e)
    old_e.put()
    return evaluation.to_json(old_e)
