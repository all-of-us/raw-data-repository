from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from rdr_service.api_util import STOREFRONT_AND_REDCAP_RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao
from rdr_service.participant_enums import WorkbenchWorkspaceStatus

DEFAULT_SEQUESTRATION_WINDOWS = 23
MAX_PAGE_SIZE = 2000


class ResearchProjectsDirectoryApi(Resource):
    def __init__(self):
        self.workspace_dao = WorkbenchWorkspaceDao()

    @auth_required(STOREFRONT_AND_REDCAP_RDR)
    def get(self):
        params = {
            'status': request.args.get('status'),
            'sequest_hour': request.args.get('sequestHour'),
            'given_name': request.args.get('givenName'),
            'family_name': request.args.get('familyName'),
            'owner_name': request.args.get('ownerName'),
            'user_source_id': request.args.get('userId'),
            'user_role': request.args.get('userRole'),
            'workspace_name_like': request.args.get('workspaceNameLike'),
            'intend_to_study_like': request.args.get('intendToStudyLike'),
            'workspace_like': request.args.get('workspaceLike'),
            'project_purpose': request.args.get('projectPurpose'),
            'page': request.args.get('page'),
            'page_size': request.args.get('pageSize')
        }

        filters = self.validate_params(params)
        results = self.get_filtered_results(**filters)

        return results

    def get_filtered_results(self, **kwargs):
        """Queries DB, returns results in format consumed by front-end
        :param kwargs: query parameters
        :return: Filtered results
        """

        return self.workspace_dao.get_workspaces_with_user_detail(**kwargs)

    def validate_params(self, params):
        filters = {}
        if params['status']:
            try:
                filters['status'] = WorkbenchWorkspaceStatus(params['status'])
            except TypeError:
                raise BadRequest(f"Invalid parameter status: {params['status']}")
        else:
            filters['status'] = None

        if params['sequest_hour']:
            try:
                filters['sequest_hour'] = int(params['sequest_hour'])
            except TypeError:
                raise BadRequest(f"Invalid parameter sequestHour: {params['sequest_hour']}")
        else:
            filters['sequest_hour'] = DEFAULT_SEQUESTRATION_WINDOWS

        if params['user_source_id']:
            try:
                filters['user_source_id'] = int(params['user_source_id'])
            except TypeError:
                raise BadRequest(f"Invalid parameter userId: {params['user_source_id']}")

        if params['user_role']:
            if params['user_role'] in ['owner', 'member', 'all']:
                filters['user_role'] = params['user_role']
            else:
                raise BadRequest(f"Invalid parameter userRole: {params['user_role']}")

        if params['project_purpose']:
            params['project_purpose'] = params['project_purpose'].strip().split(',')
            if set(params['project_purpose']).issubset(['diseaseFocusedResearch', 'methodsDevelopment', 'ancestry',
                                                        'socialBehavioral', 'populationHealth', 'drugDevelopment',
                                                        'commercialPurpose', 'educational', 'controlSet']):
                filters['project_purpose'] = params['project_purpose']
            else:
                raise BadRequest(f"Invalid parameter projectPurpose: {params['project_purpose']}")

        if params['page']:
            try:
                filters['page'] = int(params['page'])
            except TypeError:
                filters['page'] = 1
        else:
            filters['page'] = 1

        if params['page_size']:
            try:
                filters['page_size'] = int(params['page_size'])
            except TypeError:
                filters['page_size'] = MAX_PAGE_SIZE
        else:
            filters['page_size'] = MAX_PAGE_SIZE

        filters['given_name'] = params['given_name'].lower() if params['given_name'] else None
        filters['family_name'] = params['family_name'].lower() if params['family_name'] else None
        filters['owner_name'] = params['owner_name'].lower() if params['owner_name'] else None

        filters['workspace_name_like'] = '%{}%'.format(params['workspace_name_like'].lower()) \
            if params['workspace_name_like'] else None
        filters['intend_to_study_like'] = '%{}%'.format(params['intend_to_study_like'].lower()) \
            if params['intend_to_study_like'] else None
        filters['workspace_like'] = '%{}%'.format(params['workspace_like'].lower()) \
            if params['workspace_like'] else None

        return filters
