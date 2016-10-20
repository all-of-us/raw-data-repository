"""The API definition file for the Biobank orders API.

This defines the APIs and the handlers for the APIs.
"""

import datetime
import uuid

import api_util
import base_api
import biobank_order

from protorpc import message_types
from protorpc import messages

from google.appengine.ext import ndb
from flask import Flask, request
from flask.ext.restful import Resource, reqparse, abort
from werkzeug.exceptions import BadRequest, NotFound

class BiobankOrderAPI(base_api.BaseApi):
  valid_tests = frozenset(["1ED10", "2ED10", "1ED04", "1SST8", "1PST8", "1HEP4",
                           "1UR10", "1SAL"])
  
  def __init__(self):
    super(BiobankOrderAPI, self).__init__(biobank_order.DAO)

  def validate_object(self, p):
     # Check for missing fields?
     for sample in p.samples:
       if not sample.test in BiobankOrderAPI.valid_tests:
         raise BadRequest('Invalid test value: ' + sample.test)     
   