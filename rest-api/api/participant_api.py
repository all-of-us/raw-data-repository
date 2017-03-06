import api_util
import json
import participant_dao
import participant

from google.appengine.ext import ndb
from api.base_api import UpdatableApi
from api_util import PTC, PTC_AND_HEALTHPRO
from dao.participant_dao import ParticipantDao
from model.utils import from_client_participant_id, to_client_participant_id, to_client_biobank_id

class ParticipantApi(UpdatableApi):
  def __init__(self):
    super(ParticipantApi, self).__init__(ParticipantDao())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None):
    return super(ParticipantApi, self).get(from_client_participant_id(id_))

  # TODO(DA-218): remove this
  def _do_insert(self, m):
    super(ParticipantApi, self)._do_insert(m)
    participant_dao.DAO().do_insert(_make_ndb_participant(m), client_id=m.clientId)

  # TODO(DA-218): remove this
  def _do_update(self, m):
    super(ParticipantApi, self)._do_update(m)
    ndb_participant = _make_ndb_participant(m)
    existing_participant = participant_dao.DAO().load(ndb_participant.key.id())
    expected_version = participant_dao.DAO().make_version_id(existing_participant.last_modified)
    participant_dao.DAO().update(_make_ndb_participant(m), expected_version, client_id=m.clientId)

  @api_util.auth_required(PTC)
  def post(self):
    result = super(ParticipantApi, self).post()
    return result

  @api_util.auth_required(PTC)
  def put(self, id_):
    return super(ParticipantApi, self).put(from_client_participant_id(id_))

  # TODO(DA-216): remove once PTC migrates to PUT
  @api_util.auth_required(PTC)
  def patch(self, id_):
    return super(ParticipantApi, self).put(from_client_participant_id(id_))

def _make_ndb_participant(obj):
  key = ndb.Key(participant.Participant, to_client_participant_id(obj.participantId))
  p = participant.Participant(key=key,
                              biobankId=to_client_biobank_id(obj.biobankId),
                              last_modified=obj.lastModified,
                              signUpTime=obj.signUpTime)
  if obj.providerLink:
    provider_link_json = json.loads(obj.providerLink)
    if provider_link_json:
      p.providerLink = []
      for link_json in provider_link_json:
        new_link = participant.ProviderLink()
        new_link.populate(**link_json)
        p.providerLink.append(new_link)
  return p
