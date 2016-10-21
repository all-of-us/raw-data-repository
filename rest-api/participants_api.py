"""The API definition file for the participants API.

This defines the APIs and the handlers for the APIs.
"""

import datetime
import uuid

import api_util
import base_api
import evaluation
import participant

from protorpc import message_types
from protorpc import messages

from google.appengine.ext import ndb
from flask import Flask, request
from flask.ext.restful import Resource, reqparse, abort
from werkzeug.exceptions import BadRequest, NotFound

class ParticipantAPI(base_api.BaseApi):
  def __init__(self):
    super(ParticipantAPI, self).__init__(participant.DAO)

  @api_util.auth_required
  def list(self, a_id=None):
    # In order to do a query, at least the last name and the birthdate must be
    # specified.
    last_name = request.args.get('last_name', None)
    date_of_birth = request.args.get('date_of_birth', None)
    first_name = request.args.get('first_name', None)
    zip_code = request.args.get('zip_code', None)
    if not last_name or not date_of_birth:
      raise BadRequest(
          'Last name and date of birth must be specified.')
    return participant.DAO.list(first_name, last_name, date_of_birth, zip_code)

  def validate_object(self, p, a_id=None):
    if not p.sign_up_time:
      p.sign_up_time = datetime.datetime.now()

class EvaluationAPI(base_api.BaseApi):
  def __init__(self):
    super(EvaluationAPI, self).__init__(evaluation.DAO)

  @api_util.auth_required
  def list(self, a_id):
    return evaluation.DAO.list(a_id)
