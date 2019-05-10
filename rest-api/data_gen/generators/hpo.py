#
# Generate HPO information
#
import csv
import logging
import os
import random
from copy import copy

from data_gen.generators.base_gen import BaseGen

_logger = logging.getLogger('rdr_logger')


class HPOGen(BaseGen):
  """
  Represents a HPO site. Use this object to get or set a HPO site.
  """
  _hpo_awardees = list()
  _hpo_sites = list()

  id = None
  name = None

  sites = None

  def __init__(self, hpo_id=None):
    """ initialize hpo generator """
    super(HPOGen, self).__init__(load_data=False)

    self._load_hpo_data()

    if hpo_id:
      self.get_hpo(hpo_id)

  def _load_hpo_data(self):
    """
    Load the awardees.csv and sites.csv files once
    """
    if len(self._hpo_awardees) > 0:
      return

    paths = ['data', '../data']
    for path in paths:
      if os.path.exists(os.path.join(os.curdir, path)):
        awardee_file = os.path.join(os.curdir, path, 'awardees.csv')
        with open(awardee_file) as handle:
          data = list(csv.DictReader(handle))
        self._hpo_awardees = data

        sites_file = os.path.join(os.curdir, path, 'sites.csv')
        with open(sites_file) as handle:
          data = list(csv.DictReader(handle))
        self._hpo_sites = data
        break

  def get_hpo(self, hpo_id):
    """
    Set the current HPO awardee and load related sites for the given HPO ID. Use this if
    you only want the hpo and are not worried about the specific site.
    :param hpo_id: hpo awardee id
    :return: cloned object set to HPO with related sites.
    """
    if not hpo_id or not isinstance(hpo_id, str):
      _logger.error('invalid hpo_id parameter')
      return None

    for awardee in self._hpo_awardees:
      if hpo_id == awardee['Awardee ID']:
        self.id = hpo_id
        self.name = awardee['Name']

        self.sites = list()

        for site in self._hpo_sites:
          if hpo_id in site['Organization ID']:
            self.sites.append(HPOSiteGen(self, site))

        return copy(self)

    _logger.error('hpo awardee not found [{0}].'.format(hpo_id))

    return None

  def get_site(self, site_id):
    """
    Set the current HPO awardee and site to the given site ID. Use this if you want to
    target a specific site.
    :param site_id: site id
    :return: HPOSiteGen object
    """
    if not site_id or not isinstance(site_id, str):
      _logger.error('invalid site_id parameter')
      return None

    for site in self._hpo_sites:
      if site_id == site['Site ID / Google Group'] or 'hpo-site-{0}'.format(site_id) == site['Site ID / Google Group']:
        # find related HPO
        for awardee in self._hpo_awardees:
          if awardee['Awardee ID'] in site['Organization ID']:
            hpo = self.get_hpo(awardee['Awardee ID'])
            obj = HPOSiteGen(hpo, site)
            return obj

    return None

  def get_test_hpo(self):
    """
    Get the testing HPO
    :return: HPOGen object
    """
    return self.get_hpo('TEST')

  def get_random_hpo(self):
    """
    Return a random HPO
    :return: HPOGen object
    """
    awardee = random.choice(self._hpo_awardees)
    return self.get_hpo(awardee['Awardee ID'])

  def get_random_site(self, hpo_id=None):
    """
    Return a random site. If hpo_id set, select random site within HPO.
    :param hpo_id: HPO id
    :return: HPOSiteGen object
    """
    # If no hpo id specified, return any random site.
    if not hpo_id:
      site = random.choice(self._hpo_sites)
      return self.get_site(site['Site ID / Google Group'])

    # only choose a site from within the given hpo.
    hpo = self.get_hpo(hpo_id)
    return random.choice(hpo.sites)

  def get_provider_link(self):
    """
    Returns a dict with the FHIR provider link information
    :return: dict
    """
    data = [{
      'primary': True,
      'organization': {
        'reference': 'Organization/{0}'.format(self.id)
      }
    }]

    return data


class HPOSiteGen(BaseGen):
  """
  Represents a specific HPO site. Don't use this object directly, use HPOGen to get
  or set a site.
  """
  hpo = None

  id = None
  org_id = None
  name = None
  active = False
  city = None
  state = None

  def __init__(self, hpo, site_dict):
    """
    :param hpo: HPOGen object
    :param site_dict: site dict from HPOGen
    """
    super(HPOSiteGen, self).__init__(load_data=False)

    if hpo and site_dict:
      self.set_hpo_site(hpo, site_dict)

  def set_hpo_site(self, hpo, site_dict):
    """
    :param hpo: HPOGen object
    :param site_dict: site dict from HPOGen
    :return: self
    """
    self.hpo = hpo

    self.id = site_dict['Site ID / Google Group']
    self.org_id = site_dict['Organization ID']
    self.city = site_dict['City']
    self.state = site_dict['State']
    self.name = site_dict['Site']

    if site_dict['Enrolling Status STABLE'] == 'Active':
      self.active = True
