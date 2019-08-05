#
# Participant data generator.
#
import logging

from rdr_service.data_gen.generators.hpo import HPOGen
from rdr_service.data_gen.generators.base_gen import BaseGen

_logger = logging.getLogger('rdr_logger')

class ParticipantGen(BaseGen):
  """
  Fake participant data generator
  ref: fake_participant_generator.py:727
  """
  _site = None
  providerLink = None
  participantId = None

  def __init__(self):
    """ initialize participant generator """
    super(ParticipantGen, self).__init__(load_data=False)

  def new(self, site=None):
    """
    Return a new participant object with the assigned site.
    :param site: HPOSiteGen object
    :return: return cloned ParticipantGen object
    """
    clone = self.__class__()

    if site:
      clone._site = site
    else:
      clone._site = HPOGen().get_random_site()

    clone.providerLink = clone._site.hpo.get_provider_link()

    return clone

  def to_dict(self):
    """
    Return object data as json
    :return: dict
    """
    data = dict()

    for key, val in self.__dict__.items():
      if key.startswith('_'):
        continue
      data[key] = val

    return data
