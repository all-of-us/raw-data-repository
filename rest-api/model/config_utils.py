import config
from werkzeug.exceptions import BadRequest


def get_biobank_id_prefix():
  return config.getSetting(config.BIOBANK_ID_PREFIX)

def to_client_biobank_id(biobank_id):
  return '%s%d' % (get_biobank_id_prefix(), biobank_id)

def from_client_biobank_id(biobank_id):
  if not biobank_id.startswith(get_biobank_id_prefix()):
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)
  try:
    return int(biobank_id[1:])
  except ValueError:
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)
