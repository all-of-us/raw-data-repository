import json
import logging
import pprint
import urllib2

from api_util import auth_required, PTC_AND_HEALTHPRO
from dao.code_dao import CodeBookDao


_CODEBOOK_URL_BASE = 'https://raw.githubusercontent.com/all-of-us-terminology/codebook-to-fhir/'
_CODEBOOK_ERRORS_URL = _CODEBOOK_URL_BASE + 'gh-pages/CodeSystem/ppi.issues.json'
_CODEBOOK_URL = _CODEBOOK_URL_BASE + 'gh-pages/CodeSystem/ppi.json'


@auth_required(PTC_AND_HEALTHPRO)
def import_codebook():
  """Imports the latest published codebook, so long as the published version has no errors.

  May be called with rdr_client/import_codebook.py.

  Expects an empty request body. The API is idempotent.

  Example responses:
  {
    'active_version': '0.2.52',  # What version is now active?
    'published_version': '0.2.53',  # What version did we find as the latest published?
    'error_messages': [
      # Human readable explanation if the active version was not updated.
      'Published codebook has issues, not imported.'
    ]
  }
  or:
  {
    'active_version': '0.2.53',  # We were able to import the latest published version.
    'published_version': '0.2.53',
    'status_messages': [
      # Human readable descriptions of the import.
      '999 codes imported.'
    ]
  }
  """
  response = {}
  codebook_json, codebook_issues_json = _fetch_codebook_json()
  new_codebook_version = codebook_json.get('version')
  response['published_version'] = new_codebook_version
  dao = CodeBookDao()
  with dao.session() as session:
    previous_codebook = dao.get_latest_with_session(session, codebook_json['url'])
    response['active_version'] = previous_codebook.version if previous_codebook else None

  if new_codebook_version is None:
    response['error_messages'] = [
        'Published codebook is missing "version", import aborted.']
    return _log_and_return_json(response)
  if codebook_issues_json != []:
    response['error_messages'] = [
        'Published codebook has issues, import aborted.\n' + pprint.pformat(codebook_issues_json)]
    return _log_and_return_json(response)

  if new_codebook_version == response['active_version']:
    response['status_messages'] = [
        'Version %s already active, not importing.' % response['active_version']]
    return _log_and_return_json(response)

  new_codebook, code_count = CodeBookDao().import_codebook(codebook_json)
  response['active_version'] = new_codebook.version
  response['status_messages'] = ['Imported %d codes.' % code_count]
  return _log_and_return_json(response)


def _fetch_codebook_json():
  """Returns (codebook_json, codebook_issues_json) for the latest published codebook on GitHub."""
  codebook_response = urllib2.urlopen(_CODEBOOK_URL)
  codebook = json.loads(codebook_response.read())
  issues_response = urllib2.urlopen(_CODEBOOK_ERRORS_URL)
  issues = json.loads(issues_response.read())
  return codebook, issues


def _log_and_return_json(response):
  for status in response.get('status_messages', []):
    logging.info(status)
  for error in response.get('error_messages', []):
    logging.info('Codebook data error: ' + error)
  return json.dumps(response)
