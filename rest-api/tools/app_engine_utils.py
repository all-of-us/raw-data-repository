from googleapiclient import discovery
import os

from googleapiclient.errors import HttpError

""" Helper functions for using app_engine api's with Python SDK
This script can be ran from rdr_client"""

def get_key():
  if os.path.isfile(get_key()):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = get_key()
    storage_key = os.path.expanduser('~/.gcp/cloudsecurity-monitoring.json')

  return storage_key




def get_projects():
  """ List all projects"""
  project_list = []

  service = discovery.build('cloudresourcemanager', 'v1')
  request = service.projects().list()
  while request is not None:
    response = request.execute()
    for projects in response['projects']:
      if projects['lifecycleState'] == 'ACTIVE':
        project_list.append(projects['projectId'])

    request = service.projects().list_next(previous_request=request, previous_response=response)

  print project_list

def make_key(project):
  """ Creates user managed keys for every service account in a project
  PARAM: project"""
  service = discovery.build('iam', 'v1')
  project_name = 'projects/' + project
  request = service.projects().serviceAccounts().list(name=project_name)
  response = request.execute()
  accounts = response['accounts']
  for account in accounts:
    email = account['email']
    serviceaccount = project_name + '/serviceAccounts/' + email
    create_request = service.projects().serviceAccounts().keys().create(
                     name=serviceaccount, body={})
    key = create_request.execute()
    print 'key created: {}'.format(key['name'])

def delete_keys(project):
  """ Deletes all user managed keys for service accounts in a project
  PARAM: project"""
  service = discovery.build('iam', 'v1')
  project_name = 'projects/' + project
  request = service.projects().serviceAccounts().list(name=project_name)
  response = request.execute()
  accounts = response['accounts']

  for account in accounts:
    serviceaccount = project_name + '/serviceAccounts/' + account['email']
    request = service.projects().serviceAccounts().keys().list(name=serviceaccount,
                                                               keyTypes='USER_MANAGED')
    response = request.execute()

    if 'keys' in response:
      keys = response['keys']
      for key in keys:
        keyname = key['name']
        print 'keyname is {}'.format(keyname)
        print 'Deleting service Account key: {}'.format(keyname)
        try:
          delete_request = service.projects().serviceAccounts().keys().delete(name=keyname)
          delete_request.execute()
        except HttpError:
          continue
    else:
      print 'No user managed keys for {}'.format(account['name'])


if __name__ == '__main__':
  # delete_keys('pmi-drc-api-test')
  # make_key('pmi-drc-api-test')
  print 'choose a path'
