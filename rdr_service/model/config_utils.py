from rdr_service import config
import logging
from werkzeug.exceptions import BadRequest


def get_biobank_id_prefix():
  return config.getSetting(config.BIOBANK_ID_PREFIX)


def to_client_biobank_id(biobank_id):
  return '%s%d' % (get_biobank_id_prefix(), biobank_id)


def from_client_biobank_id(biobank_id, log_exception=False):
  if not biobank_id.startswith(get_biobank_id_prefix()):
    # @TODO: Remove log exception and throw a hard error anytime after May 1, 2019
    if not log_exception:
      raise BadRequest("Invalid biobank ID: %s" % biobank_id)
    else:
      logging.warn('Biobank param without environment prefix is deprecated.')
      return int(biobank_id)
  try:
    return int(biobank_id[1:])
  except ValueError:
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)
