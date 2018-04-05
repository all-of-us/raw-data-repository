from googleapiclient import discovery
import os


def get_key():
    storage_key = os.path.expanduser('~/.gcp/cloudsecurity-monitoring.json')

    return storage_key


if os.path.isfile(get_key()):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = get_key()


def get_projects():
    project_list = []

    service = discovery.build('cloudresourcemanager', 'v1')
    request = service.projects().list()
    while request is not None:
        response = request.execute()
        for project in response['projects']:
            if project['lifecycleState'] == 'ACTIVE':
                project_list.append(project['projectId'])

        request = service.projects().list_next(previous_request=request, previous_response=response)

    return project_list


if __name__ == "__main__":
    print(get_key())

    for project in get_projects():
        print(project)
