#
# CodeBook generator
#

import json
import logging

from rdr_service.lib_fhir.fhirclient_3_0_0.models.codesystem import CodeSystem
from rdr_service.services.system_utils import make_api_request

_logger = logging.getLogger("rdr_logger")


class CodeBook(object):
    """
  Codebook concepts. Try to re-use this object to minimize downloads of the codebook.
  """

    _host = "raw.githubusercontent.com"
    _url = "all-of-us-terminology/codebook-to-fhir/gh-pages/CodeSystem/ppi.json"
    _concepts = None

    def __init__(self):
        self._download_concepts()

    def _download_concepts(self):
        """
    Download the concept code book from github and return CodeSystem fhir object.
    Requires fhirclient >= v3.0.0
    :return: True if successful otherwise False
    """
        _logger.debug("retrieving code book from github.")

        self._concepts = None

        code, resp = make_api_request(self._host, self._url, ret_type="text")

        if code != 200:
            _logger.error("failed to get code book from github.")
            return False

        # Fix missing value data so the FHIR parser doesn't throw an exception.
        resp = resp.replace('"valueCode": null,', '"valueCode": "null",')

        concepts = CodeSystem(json.loads(resp))
        if concepts:
            self._concepts = concepts
            return True

        _logger.error("failed to parse the code book data.")
        return False

    def get_concept(self, concept):
        """
    Return the FHRI concept object for the requested concept.
    :param concept:
    :return: fhir concept object
    """
        if not concept:
            _logger.error("concept parameter not set.")
            return None

        if not self._concepts:
            _logger.error("code book is not loaded.")

        _logger.debug("searching code book for concept [{0}].".format(concept))

        for item in self._concepts.concept:
            if item.code.lower() == concept.lower():
                return item

        _logger.warning("concept not found [{0}] in code book.".format(concept))

        return None
