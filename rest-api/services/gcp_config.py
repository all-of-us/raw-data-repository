#
# Authors: Robert Abram <robert.m.abram@vumc.org>
#

GCP_PROJECTS = [
  'all-of-us-rdr-prod',
  'all-of-us-rdr-stable',
  'all-of-us-rdr-staging',
  'all-of-us-rdr-sandbox',
  'pmi-drc-api-test',
]


GCP_INSTANCES = {
  'all-of-us-rdr-prod': 'all-of-us-rdr-prod:us-central1:rdrmaindb',
  'all-of-us-rdr-stable': 'all-of-us-rdr-stable:us-central1:rdrmaindb',
  'all-of-us-rdr-sandbox': 'all-of-us-rdr-sandbox:us-central1:rdrmaindb',
  'all-of-us-rdr-staging': 'all-of-us-rdr-staging:us-central1:rdrbackupdb',
  '': 'pmi-drc-api-test:us-central1:rdrmaindb',
}


def validate_project(project):
  """
  Make sure project given is a valid GCP project
  :param project: project id
  :return: project id or None if invalid
  """

  if 'test' in project and 'pmi-drc-api' not in project:
    project = 'pmi-drc-api-'.format(project)
  else:
    if 'all-of-us-rdr' not in project:
      project = 'all-of-us-rdr-{0}'.format(project)

  if project not in GCP_PROJECTS:
    return None

  return project
