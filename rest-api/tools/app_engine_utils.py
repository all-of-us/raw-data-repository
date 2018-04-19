from googleapiclient import discovery
import os

""" Helper functions for using app_engine api's with Python SDK """

def get_key():
  if os.path.isfile(get_key()):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = get_key()
    storage_key = os.path.expanduser('~/.gcp/cloudsecurity-monitoring.json')

  return storage_key




def get_projects():
  project_list = []

  service = discovery.build('cloudresourcemanager', 'v1')
  request = service.projects().list()
  while request is not None:
    response = request.execute()
    for projects in response['projects']:
      if projects['lifecycleState'] == 'ACTIVE':
        project_list.append(projects['projectId'])

    request = service.projects().list_next(previous_request=request, previous_response=response)

  return project_list

def make_key(app_id):
  service = discovery.build('iam', 'v1')
  project_name = 'projects/' + app_id
  request = service.projects().serviceAccounts().list(name=project_name)
  response = request.execute()
  account = response['accounts']
  email = account['email']
  serviceaccount = project_name + '/serviceAccounts/' + email
  create_request = service.projects().serviceAccounts().keys().create(name=serviceaccount)
  key = create_request.execute()
  print key
