#
#  Methods for interacting with external tools that help automate RDR Documentation updates
#  The main RDR user documentation is currently maintained as a readthedocs.org project (aka RTD here)
#

import os
import json

import requests

from rdr_service import config
from rdr_service.services.system_utils import is_valid_release_git_tag

_RTD_PROJECT_BASE_URL = "https://readthedocs.org/api/v3/projects/all-of-us-raw-data-repository/"
_RTD_REQUESTS = {'version_details': {
                        'req_type': 'get',
                        'url': _RTD_PROJECT_BASE_URL + 'versions/%s/',
                        'success_code': 200
                   },
                   'update_project' : {
                        'req_type': 'patch',
                        'url': _RTD_PROJECT_BASE_URL,
                        'success_code': 204
                    },
                    'trigger_build' : {
                        'req_type': 'post',
                        'url': _RTD_PROJECT_BASE_URL + 'versions/%s/builds/',
                        'success_code': 202
                    },
                    'build_details' : {
                        'req_type': 'get',
                        'url': _RTD_PROJECT_BASE_URL + 'builds/%s/',
                        'success_code': 200
                    },
                    'project_details' : {
                        'req_type': 'get',
                        'url': _RTD_PROJECT_BASE_URL,
                        'success_code': 200
                    }
}

class ReadTheDocsHandler:
    """
    Manage project settings and trigger documentation builds in the RDR readthedocs.org account
    See:  https://docs.readthedocs.io/en/stable/api/v3.html
    """
    def __init__(self, api_token=None):
        self._api_token = api_token
        # Try to resolve API token if not passed in
        if not self._api_token:
            self.set_api_token_from_config_or_env()
        self._api_request_headers = {'Authorization': 'token ' + self._api_token}

    def _get_last_http_status(self):
        """ Extract the HTTP status code from the last API request that was executed """
        if self._api_last_response.status_code:
            return self._api_last_response.status_code

        return None


    def _get_last_response_json(self):
        """ Extract JSON response data from the last API response that was received """
        if self._api_last_response.text:
            return json.loads(self._api_last_response.text)

        return None

    def _make_rtd_request(self, rtd_request, version="", build="", data={}):
        """
         Helper routine to perform implemented readthedocs API requests
         :param rtd_request: (str) A key defined in the _RTD_REQUESTS list
         :param version: (str) A version slug from the RDR readthedocs.org project (e.g., 'latest', 'stable')
         :param build: (int)  A readthedocs.org build ID
         :param data: (dict)  Data to be included as JSON in the API request (for POST and PATCH requests)
         :returns:  True if request succeeded with expected HTTP 2xx status code otherwise False
         :raises: ValueError:  invalid rtd_request parameter
         """
        req = _RTD_REQUESTS[rtd_request]

        if not req:
            raise ValueError(f'Undefined request type {rtd_request}')


        if rtd_request == 'version_details':
            self._api_last_response = requests.get(req['url'] % version, headers=self._api_request_headers)
        elif rtd_request == 'update_project':
            self._api_last_response = requests.patch(req['url'], headers=self._api_request_headers, json=data)
        elif rtd_request == 'trigger_build':
            self._api_last_response = requests.post(req['url'] % version, headers=self._api_request_headers)
        elif rtd_request == 'build_details':
            self._api_last_response = requests.get(req['url'] % build, headers=self._api_request_headers)
        elif rtd_request == 'project_details':
            self._api_last_response = requests.get(req['url'], headers=self._api_request_headers)

        return req['success_code'] == self._get_last_http_status()

    def set_api_token_from_config_or_env(self):
        """
        Checks the app config and the local environment for the ReadTheDocs API token
        """
        rtd_creds = config.getSettingJson(config.READTHEDOCS_CREDS, None)
        if rtd_creds:
            self._api_token = rtd_creds.get("readthedocs_rdr_api_token", None)
        else:
            self._api_token = os.environ.get('RTD_API_TOKEN', None)


    def update_project_to_release(self, git_tag=None):
        """
        See: https://docs.readthedocs.io/en/stable/api/v3.html#project-update
        Update the project advanced setting for the 'latest' default branch (can be a tag) in readthedocs.
        Primary use case is to update the 'latest' version slug in our project when deploying new release to production
        :param git_tag:  A tag applied to the RDR devel branch when cutting a release
        :raises RuntimeError:   Unable to validate the git tag, or the readthedocs API request to update project failed
        """

        if not is_valid_release_git_tag(git_tag):
            raise RuntimeError(f'Failed to validate git tag {git_tag}')

        if not self._make_rtd_request('update_project', data={'default_branch': git_tag}):
            raise RuntimeError(
                f'Failed to update project details on readthedocs.org. HTTP status code {self._get_last_http_status()}')

    def get_build_details(self, build_id):
        """"
        Retrieve details about a specific readthedocs.org build
        See: https://docs.readthedocs.io/en/stable/api/v3.html#build-details
        :param build_id:  Integer build ID value assigned by readthedocs.org when the build was triggered
        :returns:  JSON response data containing the build details
        :raises RuntimeError:  Failed to retrieve the build details
        """
        if not self._make_rtd_request('build_details', build=build_id):
            status = self._get_last_http_status()
            raise RuntimeError(f'Failed to retrieve readthedocs build {build_id}.  HTTP status code {status}')

        return self._get_last_response_json()

    def get_project_details(self):
        """"
        Retrieve details about the RDR readthedocs.org project
        See: https://docs.readthedocs.io/en/stable/api/v3.html#project-details
        :returns:  JSON response data containing the project details
        :raises RuntimeError:  Failed to retrieve the project details
        """
        if not self._make_rtd_request('project_details'):
            status = self._get_last_http_status()
            raise RuntimeError(
                f'Failed to retrieve project details from readthedocs.org.  HTTP status code {status}')

        return self._get_last_response_json()

    def get_version_details(self, version_id):
        """"
        Retrieve details about an RDR readthedocs.org version
        See: https://docs.readthedocs.io/en/stable/api/v3.html#version-detail
        :returns:  JSON response data containing the version details
        :raises RuntimeError:  Failed to retrieve the version details
        """
        if not self._make_rtd_request('version_details', version_id):
            status = self._get_last_http_status()
            raise RuntimeError(
                f'Failed to retrieve version {version_id} from readthedocs.org.  HTTP status code {status}')

        return self._get_last_response_json()

    def build_the_docs(self, version_slug):
        """
        See: https://docs.readthedocs.io/en/stable/api/v3.html#build-triggering
        See: https://docs.readthedocs.io/en/stable/versions.html

        Trigger a new build for the specified version of this project
        Primary use case is to invoke builds automatically when deploying to stable or prod from app_engine_manager.py
        :param version_slug:  (str) The version_slug as defined in readthedocs.org for the RDR project.
        :return:  (int) build id from readthedocs API response data
        :raises RuntimeError:   Failed to retrieve version details from readthedocs.org, version was not active, or
           request to trigger build failed
        """

        # Verify the specified version exists and is active before triggering build
        if not self._make_rtd_request('version_details', version=version_slug):
            status = self._get_last_http_status()
            raise RuntimeError(f'Could not retrieve version {version_slug}.  HTTP status code {status}')

        if not self._get_last_response_json()['active']:
            raise RuntimeError(f'readthedocs {version_slug} is inactive.  Build aborted')

        if not self._make_rtd_request('trigger_build', version=version_slug):
            status = self._get_last_http_status()
            raise RuntimeError(f'Build for readthedocs version {version_slug} failed. HTTP status code {status}')

        return self._get_last_response_json()['build']['id']
