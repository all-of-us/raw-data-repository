import json

from dateutil.parser import parse
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service import clock
from rdr_service.model.workbench_workspace import (
    WorkbenchWorkspace,
    WorkbenchWorkspaceHistory,
    WorkbenchWorkspaceUser,
)
from rdr_service.model.workbench_researcher import (
    WorkbenchResearcher,
    WorkbenchResearcherHistory,
    WorkbenchInstitutionalAffiliations,
)
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole


class WorkbenchWorkspaceDao(UpdatableDao):
    def __init__(self):
        super(WorkbenchWorkspaceDao, self).__init__(WorkbenchWorkspace, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self, resource_json, client_id=None):
        now = clock.CLOCK.now()
        workspaces = []
        for item in resource_json:
            workspace = WorkbenchWorkspace(
                created=now,
                modified=now,
                workspaceSourceId=item['workspaceId'],
                name=item['name'],
                creationTime=parse(item['creationTime']).date(),
                modifiedTime=parse(item['modifiedTime']).date(),
                status=WorkbenchWorkspaceStatus(item['status']),
                excludeFromPublicDirectory=item['excludeFromPublicDirectory'],
                diseaseFocusedResearch=item['diseaseFocusedResearch'],
                diseaseFocusedResearchName=item['diseaseFocusedResearchName'],
                otherPurposeDetails=item['otherPurposeDetails'],
                methodsDevelopment=item['methodsDevelopment'],
                controlSet=item['controlSet'],
                ancestry=item['ancestry'],
                socialBehavioral=item['socialBehavioral'],
                populationHealth=item['populationHealth'],
                drugDevelopment=item['drugDevelopment'],
                commercialPurpose=item['commercialPurpose'],
                educational=item['educational'],
                otherPurpose=item['otherPurpose'],
                resource=json.dumps(item)
            )

            self._add_users(workspace, item['workspaceUsers'])
            workspaces.append(workspace)

        return workspaces

    def _add_users(self, workspace, workspace_users_json):
        now = clock.CLOCK.now()
        workspace_users = []
        for user in workspace_users_json:
            user_obj = WorkbenchWorkspaceUser(
                created=now,
                modified=now,
                userId=user['userId'],
                role=WorkbenchWorkspaceUserRole(user['role']),
                status=WorkbenchWorkspaceStatus(user['status'])
            )
            workspace.workbenchWorkspaceUser.append(user_obj)
        return workspace_users

    def insert_with_session(self, session, workspaces):
        for workspace in workspaces:
            exist = self._get_workspace_by_workspace_id(session, workspace.workspaceSourceId)
            if exist:
                for k, v in workspace.asdict().items():
                    if k != 'id':
                        setattr(exist, k, v)
                self.update_with_session(session, exist)
            else:
                session.add(workspace)
        self._insert_history(session, workspaces)

        return workspaces

    def _validate_update(self, session, obj, existing_obj):
        pass

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchWorkspace):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for workspace in obj:
                result.append(json.loads(workspace.resource))
            return result

    def _insert_history(self, session, workspaces):
        session.flush()
        for workspace in workspaces:
            history = WorkbenchWorkspaceHistory()
            workspace_dict = workspace.asdict()
            workspace_dict.pop('id', None)
            history.fromdict(workspace_dict, allow_pk=True)
            session.add(history)

    def _get_workspace_by_workspace_id(self, session, workspace_id):
        return session.query(WorkbenchWorkspace).filter(WorkbenchWorkspace.workspaceSourceId == workspace_id).first()


class WorkbenchWorkspaceHistoryDao(UpdatableDao):
    def __init__(self):
        super(WorkbenchWorkspaceHistoryDao, self).__init__(WorkbenchWorkspaceHistory, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id


class WorkbenchResearcherDao(UpdatableDao):
    def __init__(self):
        super(WorkbenchResearcherDao, self).__init__(WorkbenchResearcher, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self, resource_json, client_id=None):
        now = clock.CLOCK.now()
        researchers = []
        for item in resource_json:
            researcher = WorkbenchResearcher(
                created=now,
                modified=now,
                userSourceId=item['userId'],
                creationTime=parse(item['creationTime']).date(),
                modifiedTime=parse(item['modifiedTime']).date(),
                givenName=item['givenName'],
                familyName=item['familyName'],
                streetAddress1=item['streetAddress1'],
                streetAddress2=item['streetAddress2'],
                city=item['city'],
                state=item['state'],
                zipCode=item['zipCode'],
                country=item['country'],
                ethnicity=item['ethnicity'],
                gender=item['gender'],
                race=item['race'],
                resource=json.dumps(item)
            )

            self._add_affiliations(researcher, item['affiliations'])
            researchers.append(researcher)

        return researchers

    def _add_affiliations(self, researcher, affiliations_json):
        now = clock.CLOCK.now()
        affiliations = []
        for affiliation in affiliations_json:
            affiliation_obj = WorkbenchInstitutionalAffiliations(
                created=now,
                modified=now,
                institution=affiliation['institution'],
                role=affiliation['role'],
                nonAcademicAffiliation=affiliation['nonAcademicAffiliation']
            )
            researcher.workbenchInstitutionalAffiliations.append(affiliation_obj)
        return affiliations

    def insert_with_session(self, session, researchers):
        for researcher in researchers:
            exist = self._get_researcher_by_user_id(session, researcher.userSourceId)
            if exist:
                for k, v in researcher.asdict().items():
                    if k != 'id':
                        setattr(exist, k, v)
                self.update_with_session(session, exist)
            else:
                session.add(researcher)
        self._insert_history(session, researchers)

        return researchers

    def _validate_update(self, session, obj, existing_obj):
        pass

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchResearcher):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for researcher in obj:
                result.append(json.loads(researcher.resource))
            return result

    def _insert_history(self, session, researchers):
        session.flush()
        for researcher in researchers:
            history = WorkbenchResearcherHistory()
            researcher_dict = researcher.asdict()
            researcher_dict.pop('id', None)
            history.fromdict(researcher_dict, allow_pk=True)
            session.add(history)

    def _get_researcher_by_user_id(self, session, user_id):
        return session.query(WorkbenchResearcher).filter(WorkbenchResearcher.userSourceId == user_id).first()


class WorkbenchResearcherHistoryDao(UpdatableDao):
    def __init__(self):
        super(WorkbenchResearcherHistoryDao, self).__init__(WorkbenchResearcherHistory, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id
