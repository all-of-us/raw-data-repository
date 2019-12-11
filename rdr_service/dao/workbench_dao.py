import json
import sqlalchemy

from werkzeug.exceptions import BadRequest
from dateutil.parser import parse
from sqlalchemy import desc
from sqlalchemy.orm import subqueryload
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service import clock
from rdr_service.model.workbench_workspace import (
    WorkbenchWorkspace,
    WorkbenchWorkspaceHistory,
    WorkbenchWorkspaceUser,
    WorkbenchWorkspaceUserHistory
)
from rdr_service.model.workbench_researcher import (
    WorkbenchResearcher,
    WorkbenchResearcherHistory,
    WorkbenchInstitutionalAffiliations,
    WorkbenchInstitutionalAffiliationsHistory
)
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole


class WorkbenchWorkspaceDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchWorkspace, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchWorkspace).options(
                subqueryload(WorkbenchWorkspace.workbenchWorkspaceUser)
            )
            return query.all()

    def from_client_json(self, resource_json, client_id=None):  # pylint: disable=unused-argument
        now = clock.CLOCK.now()
        workspaces = []
        for item in resource_json:
            workspace = WorkbenchWorkspace(
                created=now,
                modified=now,
                workspaceSourceId=item['workspaceId'],
                name=item['name'],
                creationTime=parse(item['creationTime']),
                modifiedTime=parse(item['modifiedTime']),
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
                workbenchWorkspaceUser=self._get_users(item['workspaceUsers']),
                resource=json.dumps(item)
            )

            workspaces.append(workspace)

        return workspaces

    def _get_users(self, workspace_users_json):
        researcher_dao = WorkbenchResearcherDao()
        now = clock.CLOCK.now()
        workspace_users = []
        for user in workspace_users_json:
            researcher = researcher_dao.get_researcher_by_user_source_id(user['userId'])
            if not researcher:
                raise BadRequest('Researcher not found for user ID: {}'.format(user['userId']))
            user_obj = WorkbenchWorkspaceUser(
                created=now,
                modified=now,
                researcherId=researcher.id,
                userId=user['userId'],
                role=WorkbenchWorkspaceUserRole(user['role']),
                status=WorkbenchWorkspaceStatus(user['status'])
            )
            workspace_users.append(user_obj)
        return workspace_users

    def insert_with_session(self, session, workspaces):
        for workspace in workspaces:
            exist = self._get_workspace_by_workspace_id_with_session(session, workspace.workspaceSourceId)
            if exist:
                session.delete(exist)
                session.commit()
            session.add(workspace)
        self._insert_history(session, workspaces)

        return workspaces

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchWorkspace):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for workspace in obj:
                result.append(json.loads(workspace.resource))
            return result

    def _insert_history(self, session, workspaces):
        history_researcher_dao = WorkbenchResearcherHistoryDao()
        session.flush()
        for workspace in workspaces:
            history = WorkbenchWorkspaceHistory()
            for k, v in workspace:
                if k != 'id':
                    setattr(history, k, v)
            users_history = []
            for user in workspace.workbenchWorkspaceUser:
                history_researcher = history_researcher_dao.get_researcher_history_by_user_source_id(user.userId)
                user_obj = WorkbenchWorkspaceUserHistory(
                    created=user.created,
                    modified=user.modified,
                    researcherId=history_researcher.id,
                    userId=user.userId,
                    role=user.role,
                    status=user.status
                )
                users_history.append(user_obj)
            history.workbenchWorkspaceUser = users_history
            session.add(history)

    def _get_workspace_by_workspace_id_with_session(self, session, workspace_id):
        return session.query(WorkbenchWorkspace).filter(WorkbenchWorkspace.workspaceSourceId == workspace_id).first()

    def get_workspaces_with_user_detail(self, status):

        query = sqlalchemy.select(
            [
                WorkbenchWorkspace.workspaceSourceId.label('workspaceId'),
                WorkbenchWorkspace.name.label('name'),
                WorkbenchWorkspace.status.label('status'),
                WorkbenchWorkspace.creationTime.label('creationTime'),
                WorkbenchWorkspace.modifiedTime.label('modifiedTime'),
                WorkbenchWorkspace.excludeFromPublicDirectory.label('excludeFromPublicDirectory'),
                WorkbenchWorkspace.diseaseFocusedResearch.label('diseaseFocusedResearch'),
                WorkbenchWorkspace.diseaseFocusedResearchName.label('diseaseFocusedResearchName'),
                WorkbenchWorkspace.otherPurposeDetails.label('otherPurposeDetails'),
                WorkbenchWorkspace.methodsDevelopment.label('methodsDevelopment'),
                WorkbenchWorkspace.controlSet.label('controlSet'),
                WorkbenchWorkspace.ancestry.label('ancestry'),
                WorkbenchWorkspace.socialBehavioral.label('socialBehavioral'),
                WorkbenchWorkspace.populationHealth.label('populationHealth'),
                WorkbenchWorkspace.drugDevelopment.label('drugDevelopment'),
                WorkbenchWorkspace.commercialPurpose.label('commercialPurpose'),
                WorkbenchWorkspace.educational.label('educational'),
                WorkbenchWorkspace.otherPurpose.label('otherPurpose'),

                WorkbenchWorkspaceUser.userId.label('userId'),
                WorkbenchWorkspaceUser.role.label('role'),
                WorkbenchResearcher.givenName.label('givenName'),
                WorkbenchResearcher.familyName.label('familyName'),

                WorkbenchInstitutionalAffiliations.institution.label('institution'),
                WorkbenchInstitutionalAffiliations.role.label('institutionRole'),
                WorkbenchInstitutionalAffiliations.nonAcademicAffiliation.label('nonAcademicAffiliation')
            ]
        ).select_from(
            sqlalchemy.join(
                sqlalchemy.join(WorkbenchWorkspace, WorkbenchWorkspaceUser,
                                WorkbenchWorkspace.id == WorkbenchWorkspaceUser.workspaceId),
                sqlalchemy.join(WorkbenchResearcher, WorkbenchInstitutionalAffiliations,
                                WorkbenchResearcher.id == WorkbenchInstitutionalAffiliations.researcherId),
                WorkbenchResearcher.id == WorkbenchWorkspaceUser.researcherId
            )
        ).where(WorkbenchWorkspaceUser.role == WorkbenchWorkspaceUserRole.OWNER)

        if status is not None:
            query = query.where(WorkbenchWorkspace.status == status)

        results = []
        with self.session() as session:
            cursor = session.execute(query)
            for row in cursor:
                record = {
                    'workspaceId': row.workspaceId,
                    'name': row.name,
                    'creationTime': row.creationTime,
                    'modifiedTime': row.modifiedTime,
                    'status': str(WorkbenchWorkspaceStatus(row.status)),
                    'workspaceOwner': [
                        {
                            'userId': row.userId,
                            'userName': row.givenName + ' ' + row.familyName,
                            'affiliations': [
                                {
                                    "institution": row.institution,
                                    "role": row.institutionRole,
                                    "nonAcademicAffiliation": row.nonAcademicAffiliation
                                }
                            ]
                        }
                    ],
                    "excludeFromPublicDirectory": row.excludeFromPublicDirectory,
                    "diseaseFocusedResearch": row.diseaseFocusedResearch,
                    "diseaseFocusedResearchName": row.diseaseFocusedResearchName,
                    "otherPurposeDetails": row.otherPurposeDetails,
                    "methodsDevelopment": row.methodsDevelopment,
                    "controlSet": row.controlSet,
                    "ancestry": row.ancestry,
                    "socialBehavioral": row.socialBehavioral,
                    "populationHealth": row.populationHealth,
                    "drugDevelopment": row.drugDevelopment,
                    "commercialPurpose": row.commercialPurpose,
                    "educational": row.educational,
                    "otherPurpose": row.otherPurpose
                }
                is_exist_workspace = False
                for item in results:
                    if item['workspaceId'] == record['workspaceId']:
                        is_exist_user = False
                        for user in item['workspaceOwner']:
                            if user['userId'] == record['workspaceOwner'][0]['userId']:
                                user['affiliations'] = user['affiliations'] + \
                                                       record['workspaceOwner'][0]['affiliations']
                            is_exist_user = True
                            break
                        if not is_exist_user:
                            item['workspaceOwner'] = item['workspaceOwner'] + record['workspaceOwner']
                        is_exist_workspace = True
                        break
                if not is_exist_workspace:
                    results.append(record)

        return results


class WorkbenchWorkspaceHistoryDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceHistory, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchWorkspaceHistory).options(
                subqueryload(WorkbenchWorkspaceHistory.workbenchWorkspaceUser)
            )
            return query.all()


class WorkbenchResearcherDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchResearcher, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchResearcher).options(
                subqueryload(WorkbenchResearcher.workbenchInstitutionalAffiliations)
            )
            return query.all()

    def get_researcher_by_user_source_id(self, user_source_id):
        with self.session() as session:
            return self._get_researcher_by_user_id_with_session(session, user_source_id)

    def from_client_json(self, resource_json, client_id=None):  # pylint: disable=unused-argument
        now = clock.CLOCK.now()
        researchers = []
        for item in resource_json:
            researcher = WorkbenchResearcher(
                created=now,
                modified=now,
                userSourceId=item['userId'],
                creationTime=parse(item['creationTime']),
                modifiedTime=parse(item['modifiedTime']),
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
                workbenchInstitutionalAffiliations=self._get_affiliations(item['affiliations']),
                resource=json.dumps(item)
            )

            researchers.append(researcher)

        return researchers

    def _get_affiliations(self, affiliations_json):
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
            affiliations.append(affiliation_obj)
        return affiliations

    def insert_with_session(self, session, researchers):
        for researcher in researchers:
            exist = self._get_researcher_by_user_id_with_session(session, researcher.userSourceId)
            if exist:
                session.delete(exist)
                session.commit()
            session.add(researcher)
        self._insert_history(session, researchers)
        return researchers

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
            for k, v in researcher:
                if k != 'id':
                    setattr(history, k, v)
            affiliations_history = []
            for affiliation in researcher.workbenchInstitutionalAffiliations:
                affiliation_obj = WorkbenchInstitutionalAffiliationsHistory(
                    created=affiliation.created,
                    modified=affiliation.modified,
                    institution=affiliation.institution,
                    role=affiliation.role,
                    nonAcademicAffiliation=affiliation.nonAcademicAffiliation
                )
                affiliations_history.append(affiliation_obj)
            history.workbenchInstitutionalAffiliations = affiliations_history
            session.add(history)

    def _get_researcher_by_user_id_with_session(self, session, user_id):
        return session.query(WorkbenchResearcher).filter(WorkbenchResearcher.userSourceId == user_id)\
            .order_by(desc(WorkbenchResearcher.created)).first()


class WorkbenchResearcherHistoryDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchResearcherHistory, order_by_ending=["id"])

    def get_researcher_history_by_user_source_id(self, user_source_id):
        with self.session() as session:
            return session.query(WorkbenchResearcherHistory).filter(WorkbenchResearcherHistory.userSourceId ==
                                                                    user_source_id) \
                .order_by(desc(WorkbenchResearcherHistory.created)).first()

    def get_id(self, obj):
        return obj.id

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchResearcherHistory).options(
                subqueryload(WorkbenchResearcherHistory.workbenchInstitutionalAffiliations)
            )
            return query.all()
