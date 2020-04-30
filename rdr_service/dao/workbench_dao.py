import json

from werkzeug.exceptions import BadRequest
from dateutil.parser import parse
import pytz
from sqlalchemy import desc, or_
from sqlalchemy.orm import subqueryload, joinedload
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service import clock
from datetime import timedelta
from rdr_service.dao.metadata_dao import MetadataDao, WORKBENCH_LAST_SYNC_KEY
from rdr_service.model.workbench_workspace import (
    WorkbenchWorkspaceApproved,
    WorkbenchWorkspaceSnapshot,
    WorkbenchWorkspaceUser,
    WorkbenchWorkspaceUserHistory,
    WorkbenchAudit
)
from rdr_service.model.workbench_researcher import (
    WorkbenchResearcher,
    WorkbenchResearcherHistory,
    WorkbenchInstitutionalAffiliations,
    WorkbenchInstitutionalAffiliationsHistory
)
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole, \
    WorkbenchInstitutionNonAcademic, WorkbenchResearcherEthnicity, WorkbenchResearcherSexAtBirth, \
    WorkbenchResearcherGender, WorkbenchResearcherRace, WorkbenchResearcherEducation, WorkbenchResearcherDisability, \
    WorkbenchResearcherDegree, WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, \
    WorkbenchWorkspaceSexualOrientation, WorkbenchWorkspaceGeography, WorkbenchWorkspaceDisabilityStatus, \
    WorkbenchWorkspaceAccessToCare, WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, \
    WorkbenchWorkspaceRaceEthnicity, WorkbenchWorkspaceAge, WorkbenchAuditWorkspaceAccessDecision, \
    WorkbenchAuditWorkspaceDisplayDecision, WorkbenchAuditReviewType


class WorkbenchWorkspaceDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceApproved, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchWorkspaceApproved).options(
                subqueryload(WorkbenchWorkspaceApproved.workbenchWorkspaceUser)
            )
            return query.all()

    def _validate(self, resource_json):
        for item in resource_json:
            if item.get('workspaceId') is None:
                raise BadRequest('Workspace ID can not be NULL')
            if item.get('name') is None:
                raise BadRequest('Workspace name can not be NULL')
            if item.get('creationTime') is None:
                raise BadRequest('Workspace creationTime can not be NULL')
            if item.get('modifiedTime') is None:
                raise BadRequest('Workspace modifiedTime can not be NULL')
            try:
                WorkbenchWorkspaceStatus(item.get('status'))
            except TypeError:
                raise BadRequest(f"Invalid workspace status: {item.get('status')}")

            if item.get('workspaceUsers'):
                for user in item.get('workspaceUsers'):
                    if user.get('userId') is None:
                        raise BadRequest('Workspace user ID can not be NULL')
                    try:
                        WorkbenchWorkspaceUserRole(user.get('role'))
                    except TypeError:
                        raise BadRequest(f"Invalid user role: {user.get('role')}")
                    try:
                        WorkbenchWorkspaceStatus(user.get('status'))
                    except TypeError:
                        raise BadRequest(f"Invalid user status: {user.get('status')}")

            if item.get("focusOnUnderrepresentedPopulations") and item.get("workspaceDemographic"):
                race_ethnicity_array = []
                if item.get("workspaceDemographic").get('raceEthnicity') is not None:
                    for race_ethnicity in item.get("workspaceDemographic").get('raceEthnicity'):
                        try:
                            race_ethnicity_array.append(int(WorkbenchWorkspaceRaceEthnicity(race_ethnicity)))
                        except TypeError:
                            raise BadRequest(f"Invalid raceEthnicity for workspaceDemographic: {race_ethnicity}")
                item['raceEthnicity'] = race_ethnicity_array

                age_array = []
                if item.get("workspaceDemographic").get('age') is not None:
                    for age in item.get("workspaceDemographic").get('age'):
                        try:
                            age_array.append(int(WorkbenchWorkspaceAge(age)))
                        except TypeError:
                            raise BadRequest(f"Invalid age for workspaceDemographic: {age}")
                item['age'] = age_array

                try:
                    if item.get("workspaceDemographic").get('sexAtBirth') is None:
                        item['sexAtBirth'] = 'UNSET'
                    else:
                        item["sexAtBirth"] = item.get("workspaceDemographic").get("sexAtBirth")
                    WorkbenchWorkspaceSexAtBirth(item['sexAtBirth'])
                except TypeError:
                    raise BadRequest(f"Invalid sexAtBirth for workspaceDemographic: {item.get('sexAtBirth')}")

                try:
                    if item.get("workspaceDemographic").get('genderIdentity') is None:
                        item['genderIdentity'] = 'UNSET'
                    else:
                        item["genderIdentity"] = item.get("workspaceDemographic").get("genderIdentity")
                    WorkbenchWorkspaceGenderIdentity(item['genderIdentity'])
                except TypeError:
                    raise BadRequest(f"Invalid genderIdentity for workspaceDemographic: {item.get('genderIdentity')}")

                try:
                    if item.get("workspaceDemographic").get('sexualOrientation') is None:
                        item['sexualOrientation'] = 'UNSET'
                    else:
                        item["sexualOrientation"] = item.get("workspaceDemographic").get("sexualOrientation")
                    WorkbenchWorkspaceSexualOrientation(item['sexualOrientation'])
                except TypeError:
                    raise BadRequest(f"Invalid sexualOrientation for workspaceDemographic: "
                                     f"{item.get('sexualOrientation')}")

                try:
                    if item.get("workspaceDemographic").get('geography') is None:
                        item['geography'] = 'UNSET'
                    else:
                        item["geography"] = item.get("workspaceDemographic").get("geography")
                    WorkbenchWorkspaceGeography(item['geography'])
                except TypeError:
                    raise BadRequest(f"Invalid geography for workspaceDemographic: "
                                     f"{item.get('geography')}")

                try:
                    if item.get("workspaceDemographic").get('disabilityStatus') is None:
                        item['disabilityStatus'] = 'UNSET'
                    else:
                        item["disabilityStatus"] = item.get("workspaceDemographic").get("disabilityStatus")
                    WorkbenchWorkspaceDisabilityStatus(item['disabilityStatus'])
                except TypeError:
                    raise BadRequest(f"Invalid disabilityStatus for workspaceDemographic: "
                                     f"{item.get('disabilityStatus')}")

                try:
                    if item.get("workspaceDemographic").get('accessToCare') is None:
                        item['accessToCare'] = 'UNSET'
                    else:
                        item["accessToCare"] = item.get("workspaceDemographic").get("accessToCare")
                    WorkbenchWorkspaceAccessToCare(item['accessToCare'])
                except TypeError:
                    raise BadRequest(f"Invalid accessToCare for workspaceDemographic: "
                                     f"{item.get('accessToCare')}")

                try:
                    if item.get("workspaceDemographic").get('educationLevel') is None:
                        item['educationLevel'] = 'UNSET'
                    else:
                        item["educationLevel"] = item.get("workspaceDemographic").get("educationLevel")
                    WorkbenchWorkspaceEducationLevel(item['educationLevel'])
                except TypeError:
                    raise BadRequest(f"Invalid educationLevel for workspaceDemographic: "
                                     f"{item.get('educationLevel')}")

                try:
                    if item.get("workspaceDemographic").get('incomeLevel') is None:
                        item['incomeLevel'] = 'UNSET'
                    else:
                        item["incomeLevel"] = item.get("workspaceDemographic").get("incomeLevel")
                    WorkbenchWorkspaceIncomeLevel(item['incomeLevel'])
                except TypeError:
                    raise BadRequest(f"Invalid incomeLevel for workspaceDemographic: "
                                     f"{item.get('incomeLevel')}")

                if item.get("workspaceDemographic").get('others') is not None:
                    item["others"] = item.get("workspaceDemographic").get("others")

    def from_client_json(self, resource_json, client_id=None):  # pylint: disable=unused-argument
        self._validate(resource_json)
        now = clock.CLOCK.now()
        workspaces = []
        for item in resource_json:
            workspace = WorkbenchWorkspaceSnapshot(
                created=now,
                modified=now,
                workspaceSourceId=item.get('workspaceId'),
                name=item.get('name'),
                creationTime=parse(item.get('creationTime')),
                modifiedTime=parse(item.get('modifiedTime')),
                status=WorkbenchWorkspaceStatus(item.get('status', 'UNSET')),
                excludeFromPublicDirectory=item.get('excludeFromPublicDirectory'),
                reviewRequested=item.get('reviewRequested'),
                diseaseFocusedResearch=item.get('diseaseFocusedResearch'),
                diseaseFocusedResearchName=item.get('diseaseFocusedResearchName'),
                otherPurposeDetails=item.get('otherPurposeDetails'),
                methodsDevelopment=item.get('methodsDevelopment'),
                controlSet=item.get('controlSet'),
                ancestry=item.get('ancestry'),
                socialBehavioral=item.get('socialBehavioral'),
                populationHealth=item.get('populationHealth'),
                drugDevelopment=item.get('drugDevelopment'),
                commercialPurpose=item.get('commercialPurpose'),
                educational=item.get('educational'),
                otherPurpose=item.get('otherPurpose'),
                scientificApproaches=item.get('scientificApproaches'),
                intendToStudy=item.get('intendToStudy'),
                findingsFromStudy=item.get('findingsFromStudy'),
                focusOnUnderrepresentedPopulations=item.get('focusOnUnderrepresentedPopulations'),
                sexAtBirth=WorkbenchWorkspaceSexAtBirth(item.get('sexAtBirth', 'UNSET')),
                genderIdentity=WorkbenchWorkspaceGenderIdentity(item.get('genderIdentity', 'UNSET')),
                sexualOrientation=WorkbenchWorkspaceSexualOrientation(item.get('sexualOrientation', 'UNSET')),
                geography=WorkbenchWorkspaceGeography(item.get('geography', 'UNSET')),
                disabilityStatus=WorkbenchWorkspaceDisabilityStatus(item.get('disabilityStatus', 'UNSET')),
                accessToCare=WorkbenchWorkspaceAccessToCare(item.get('accessToCare', 'UNSET')),
                educationLevel=WorkbenchWorkspaceEducationLevel(item.get('educationLevel', 'UNSET')),
                incomeLevel=WorkbenchWorkspaceIncomeLevel(item.get('incomeLevel', 'UNSET')),
                raceEthnicity=item.get("raceEthnicity", []),
                age=item.get("age", []),
                others=item.get('others'),
                workbenchWorkspaceUser=self._get_users(item.get('workspaceUsers')),
                resource=json.dumps(item)
            )

            workspaces.append(workspace)

        return workspaces

    def _get_users(self, workspace_users_json):
        if workspace_users_json is None:
            return []
        researcher_history_dao = WorkbenchResearcherHistoryDao()
        now = clock.CLOCK.now()
        workspace_users = []
        for user in workspace_users_json:
            researcher = researcher_history_dao.get_researcher_history_by_user_source_id(user.get('userId'))
            if not researcher:
                raise BadRequest('Researcher not found for user ID: {}'.format(user.get('userId')))
            user_obj = WorkbenchWorkspaceUserHistory(
                created=now,
                modified=now,
                researcherId=researcher.id,
                userId=user.get('userId'),
                role=WorkbenchWorkspaceUserRole(user.get('role', 'UNSET')),
                status=WorkbenchWorkspaceStatus(user.get('status', 'UNSET'))
            )
            workspace_users.append(user_obj)
        return workspace_users

    def insert_with_session(self, session, workspaces):
        for workspace in workspaces:
            session.add(workspace)
            self.add_approved_workspace_with_session(session, workspace)
        return workspaces

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchWorkspaceSnapshot):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for workspace in obj:
                result.append(json.loads(workspace.resource))
            return result

    def get_workspace_by_workspace_id_with_session(self, session, workspace_id):
        return session.query(WorkbenchWorkspaceApproved)\
            .filter(WorkbenchWorkspaceApproved.workspaceSourceId == workspace_id).first()

    def remove_workspace_by_workspace_id_with_session(self, session, workspace_id):
        workspace = session.query(WorkbenchWorkspaceApproved) \
            .options(joinedload(WorkbenchWorkspaceApproved.workbenchWorkspaceUser)) \
            .filter(WorkbenchWorkspaceApproved.workspaceSourceId == workspace_id).first()
        if workspace:
            session.delete(workspace)

    def get_redcap_audit_workspaces(self, last_snapshot_id):
        results = []
        with self.session() as session:
            query = (
                session.query(WorkbenchWorkspaceSnapshot, WorkbenchResearcherHistory)
                    .options(joinedload(WorkbenchWorkspaceSnapshot.workbenchWorkspaceUser),
                             joinedload(WorkbenchResearcherHistory.workbenchInstitutionalAffiliations))
                    .filter(WorkbenchWorkspaceUserHistory.researcherId == WorkbenchResearcherHistory.id,
                            WorkbenchWorkspaceSnapshot.id == WorkbenchWorkspaceUserHistory.workspaceId)
                    .order_by(WorkbenchWorkspaceSnapshot.id)
            )

            if last_snapshot_id:
                query = query.filter(WorkbenchWorkspaceSnapshot.id > last_snapshot_id)

            items = query.all()
            for workspace, researcher in items:
                verified_institutional_affiliation = {}
                affiliations = []
                if researcher.workbenchInstitutionalAffiliations:
                    for affiliation in researcher.workbenchInstitutionalAffiliations:
                        affiliations.append(
                            {
                                "institution": affiliation.institution,
                                "role": affiliation.role,
                                "isVerified": affiliation.isVerified,
                                "nonAcademicAffiliation":
                                    str(WorkbenchInstitutionNonAcademic(affiliation.nonAcademicAffiliation))
                                    if affiliation.nonAcademicAffiliation else 'UNSET'
                            }
                        )
                        if affiliation.isVerified:
                            verified_institutional_affiliation = {
                                "institution": affiliation.institution,
                                "role": affiliation.role,
                                "nonAcademicAffiliation":
                                    str(WorkbenchInstitutionNonAcademic(affiliation.nonAcademicAffiliation))
                                    if affiliation.nonAcademicAffiliation else 'UNSET'
                            }
                workspace_researcher = {
                            "userId": researcher.userSourceId,
                            "creationTime": researcher.creationTime,
                            "modifiedTime": researcher.modifiedTime,
                            "givenName": researcher.givenName,
                            "familyName": researcher.familyName,
                            "email": researcher.email,
                            "verifiedInstitutionalAffiliation": verified_institutional_affiliation,
                            "affiliations": affiliations
                        }

                exist = False
                for result in results:
                    if result['snapshotId'] == workspace.id:
                        result['workspaceResearchers'].append(workspace_researcher)
                        exist = True
                        break
                if exist:
                    continue
                record = {
                    'snapshotId': workspace.id,
                    'workspaceId': workspace.workspaceSourceId,
                    'name': workspace.name,
                    'creationTime': workspace.creationTime,
                    'modifiedTime': workspace.modifiedTime,
                    'status': str(WorkbenchWorkspaceStatus(workspace.status)),
                    'workspaceUsers': [
                        {
                            "userId": user.userId,
                            "role": str(WorkbenchWorkspaceUserRole(user.role)) if user.role else 'UNSET',
                            "status": str(WorkbenchWorkspaceStatus(user.status)) if user.status else 'UNSET',
                        } for user in workspace.workbenchWorkspaceUser
                    ] if workspace.workbenchWorkspaceUser else [],
                    'workspaceResearchers': [workspace_researcher],
                    "excludeFromPublicDirectory": workspace.excludeFromPublicDirectory,
                    "reviewRequested": workspace.reviewRequested if workspace.reviewRequested else False,
                    "diseaseFocusedResearch": workspace.diseaseFocusedResearch,
                    "diseaseFocusedResearchName": workspace.diseaseFocusedResearchName,
                    "otherPurposeDetails": workspace.otherPurposeDetails,
                    "methodsDevelopment": workspace.methodsDevelopment,
                    "controlSet": workspace.controlSet,
                    "ancestry": workspace.ancestry,
                    "socialBehavioral": workspace.socialBehavioral,
                    "populationHealth": workspace.populationHealth,
                    "drugDevelopment": workspace.drugDevelopment,
                    "commercialPurpose": workspace.commercialPurpose,
                    "educational": workspace.educational,
                    "otherPurpose": workspace.otherPurpose,
                    "scientificApproaches": workspace.scientificApproaches,
                    "intendToStudy": workspace.intendToStudy,
                    "findingsFromStudy": workspace.findingsFromStudy,
                    "focusOnUnderrepresentedPopulations": workspace.focusOnUnderrepresentedPopulations,
                    "workspaceDemographic": {
                        "raceEthnicity": [str(WorkbenchWorkspaceRaceEthnicity(value))
                                          for value in workspace.raceEthnicity] if workspace.raceEthnicity else None,
                        "age": [str(WorkbenchWorkspaceAge(value)) for value in workspace.age]
                        if workspace.age else None,
                        "sexAtBirth": str(WorkbenchWorkspaceSexAtBirth(workspace.sexAtBirth))
                        if workspace.sexAtBirth else None,
                        "genderIdentity": str(WorkbenchWorkspaceGenderIdentity(workspace.genderIdentity))
                        if workspace.genderIdentity else None,
                        "sexualOrientation": str(WorkbenchWorkspaceSexualOrientation(workspace.sexualOrientation))
                        if workspace.sexualOrientation else None,
                        "geography": str(WorkbenchWorkspaceGeography(workspace.geography))
                        if workspace.geography else None,
                        "disabilityStatus": str(WorkbenchWorkspaceDisabilityStatus(workspace.disabilityStatus))
                        if workspace.disabilityStatus else None,
                        "accessToCare": str(WorkbenchWorkspaceAccessToCare(workspace.accessToCare))
                        if workspace.accessToCare else None,
                        "educationLevel": str(WorkbenchWorkspaceEducationLevel(workspace.educationLevel))
                        if workspace.educationLevel else None,
                        "incomeLevel": str(WorkbenchWorkspaceIncomeLevel(workspace.incomeLevel))
                        if workspace.incomeLevel else None,
                        "others": workspace.others
                    }
                }
                results.append(record)

        return results

    def get_workspaces_with_user_detail(self, status, sequest_hour):
        results = []
        now = clock.CLOCK.now()
        sequest_hours_ago = now - timedelta(hours=sequest_hour)
        with self.session() as session:
            query = (
                session.query(WorkbenchWorkspaceApproved, WorkbenchResearcher)
                    .options(joinedload(WorkbenchWorkspaceApproved.workbenchWorkspaceUser),
                             joinedload(WorkbenchResearcher.workbenchInstitutionalAffiliations))
                    .filter(WorkbenchWorkspaceUser.researcherId == WorkbenchResearcher.id,
                            WorkbenchWorkspaceApproved.id == WorkbenchWorkspaceUser.workspaceId,
                            WorkbenchWorkspaceApproved.excludeFromPublicDirectory == 0,
                            or_(WorkbenchWorkspaceApproved.modified < sequest_hours_ago,
                                WorkbenchWorkspaceApproved.isReviewed == 1))
            )

            if status is not None:
                query = query.filter(WorkbenchWorkspaceApproved.status == status)

            items = query.all()
            for workspace, researcher in items:
                affiliations = []
                if researcher.workbenchInstitutionalAffiliations:
                    for affiliation in researcher.workbenchInstitutionalAffiliations:
                        affiliations.append(
                            {
                                "institution": affiliation.institution,
                                "role": affiliation.role,
                                "isVerified": affiliation.isVerified,
                                "nonAcademicAffiliation":
                                    str(WorkbenchInstitutionNonAcademic(affiliation.nonAcademicAffiliation))
                                    if affiliation.nonAcademicAffiliation else 'UNSET'
                            }
                        )
                owner_user_id = None
                for workspace_user in workspace.workbenchWorkspaceUser:
                    if workspace_user.role == WorkbenchWorkspaceUserRole.OWNER:
                        owner_user_id = workspace_user.userId
                user = {
                    'userId': researcher.userSourceId,
                    'userName': researcher.givenName + ' ' + researcher.familyName,
                    'affiliations': affiliations
                }

                exist = False
                for result in results:
                    if result['workspaceId'] == workspace.workspaceSourceId:
                        result['workspaceUsers'].append(user)
                        if user.get('userId') == owner_user_id:
                            result['workspaceOwner'].append(user)
                        exist = True
                        break
                if exist:
                    continue

                record = {
                    'workspaceId': workspace.workspaceSourceId,
                    'name': workspace.name,
                    'creationTime': workspace.creationTime,
                    'modifiedTime': workspace.modifiedTime,
                    'status': str(WorkbenchWorkspaceStatus(workspace.status)),
                    'workspaceUsers': [user] if user else [],
                    'workspaceOwner': [user] if user.get('userId') == owner_user_id else [],
                    "excludeFromPublicDirectory": workspace.excludeFromPublicDirectory,
                    "reviewRequested": workspace.reviewRequested if workspace.reviewRequested else False,
                    "diseaseFocusedResearch": workspace.diseaseFocusedResearch,
                    "diseaseFocusedResearchName": workspace.diseaseFocusedResearchName,
                    "otherPurposeDetails": workspace.otherPurposeDetails,
                    "methodsDevelopment": workspace.methodsDevelopment,
                    "controlSet": workspace.controlSet,
                    "ancestry": workspace.ancestry,
                    "socialBehavioral": workspace.socialBehavioral,
                    "populationHealth": workspace.populationHealth,
                    "drugDevelopment": workspace.drugDevelopment,
                    "commercialPurpose": workspace.commercialPurpose,
                    "educational": workspace.educational,
                    "otherPurpose": workspace.otherPurpose,
                    "scientificApproaches": workspace.scientificApproaches,
                    "intendToStudy": workspace.intendToStudy,
                    "findingsFromStudy": workspace.findingsFromStudy,
                    "focusOnUnderrepresentedPopulations": workspace.focusOnUnderrepresentedPopulations,
                    "workspaceDemographic": {
                        "raceEthnicity": [str(WorkbenchWorkspaceRaceEthnicity(value))
                                          for value in workspace.raceEthnicity] if workspace.raceEthnicity else None,
                        "age": [str(WorkbenchWorkspaceAge(value)) for value in workspace.age]
                        if workspace.age else None,
                        "sexAtBirth": str(WorkbenchWorkspaceSexAtBirth(workspace.sexAtBirth))
                        if workspace.sexAtBirth else None,
                        "genderIdentity": str(WorkbenchWorkspaceGenderIdentity(workspace.genderIdentity))
                        if workspace.genderIdentity else None,
                        "sexualOrientation": str(WorkbenchWorkspaceSexualOrientation(workspace.sexualOrientation))
                        if workspace.sexualOrientation else None,
                        "geography": str(WorkbenchWorkspaceGeography(workspace.geography))
                        if workspace.geography else None,
                        "disabilityStatus": str(WorkbenchWorkspaceDisabilityStatus(workspace.disabilityStatus))
                        if workspace.disabilityStatus else None,
                        "accessToCare": str(WorkbenchWorkspaceAccessToCare(workspace.accessToCare))
                        if workspace.accessToCare else None,
                        "educationLevel": str(WorkbenchWorkspaceEducationLevel(workspace.educationLevel))
                        if workspace.educationLevel else None,
                        "incomeLevel": str(WorkbenchWorkspaceIncomeLevel(workspace.incomeLevel))
                        if workspace.incomeLevel else None,
                        "others": workspace.others
                    }
                }
                results.append(record)

        metadata_dao = MetadataDao()
        metadata = metadata_dao.get_by_key(WORKBENCH_LAST_SYNC_KEY)
        if metadata:
            last_sync_date = metadata.dateValue
        else:
            last_sync_date = clock.CLOCK.now()

        return {"last_sync_date": last_sync_date, "data": results}

    def add_approved_workspace_with_session(self, session, workspace_snapshot, is_reviewed=False):
        exist = self.get_workspace_by_workspace_id_with_session(session, workspace_snapshot.workspaceSourceId)

        workspace_approved = WorkbenchWorkspaceApproved()
        for k, v in workspace_snapshot:
            if k != 'id':
                setattr(workspace_approved, k, v)
        workspace_approved.excludeFromPublicDirectory = False
        workspace_approved.isReviewed = is_reviewed
        users = []
        researcher_dao = WorkbenchResearcherDao()
        for user in workspace_snapshot.workbenchWorkspaceUser:
            researcher = researcher_dao.get_researcher_by_user_id_with_session(session, user.userId)
            user_obj = WorkbenchWorkspaceUser(
                created=user.created,
                modified=user.modified,
                researcherId=researcher.id,
                userId=user.userId,
                role=user.role,
                status=user.status
            )
            users.append(user_obj)
        workspace_approved.workbenchWorkspaceUser = users

        if exist:
            if is_reviewed is True:
                exist.excludeFromPublicDirectory = False
                exist.isReviewed = is_reviewed
            if exist.modifiedTime.replace(tzinfo=pytz.utc) < workspace_snapshot.modifiedTime.replace(tzinfo=pytz.utc):
                for attr_name in workspace_approved.__dict__.keys():
                    if not attr_name.startswith('_') and attr_name != 'created':
                        setattr(exist, attr_name, getattr(workspace_approved, attr_name))
        else:
            session.add(workspace_approved)


class WorkbenchWorkspaceHistoryDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceSnapshot, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def get_snapshot_by_id_with_session(self, session, snapshot_id):
        return session.query(WorkbenchWorkspaceSnapshot)\
            .options(subqueryload(WorkbenchWorkspaceSnapshot.workbenchWorkspaceUser))\
            .filter(WorkbenchWorkspaceSnapshot.id == snapshot_id).first()

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchWorkspaceSnapshot).options(
                subqueryload(WorkbenchWorkspaceSnapshot.workbenchWorkspaceUser)
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
            return self.get_researcher_by_user_id_with_session(session, user_source_id)

    def _validate(self, resource_json):
        for item in resource_json:
            if item.get('userId') is None:
                raise BadRequest('User ID can not be NULL')
            if item.get('modifiedTime') is None:
                raise BadRequest('User modifiedTime can not be NULL')
            if item.get('givenName') is None:
                raise BadRequest('User givenName can not be NULL')
            if item.get('familyName') is None:
                raise BadRequest('User familyName can not be NULL')

            try:
                if item.get('ethnicity') is None:
                    item['ethnicity'] = 'UNSET'
                WorkbenchResearcherEthnicity(item.get('ethnicity'))
            except TypeError:
                raise BadRequest(f"Invalid ethnicity: {item.get('ethnicity')}")

            set_at_birth_array = []
            if item.get('sexAtBirth') is not None:
                for set_at_birth in item.get('sexAtBirth'):
                    try:
                        set_at_birth_array.append(int(WorkbenchResearcherSexAtBirth(set_at_birth)))
                    except TypeError:
                        raise BadRequest(f"Invalid sexAtBirth: {set_at_birth}")
            item['sexAtBirth'] = set_at_birth_array

            try:
                if item.get('education') is None:
                    item['education'] = 'UNSET'
                # Checking for validation of item passed in only.
                WorkbenchResearcherEducation(item.get('education'))
            except TypeError:
                raise BadRequest(f"Invalid education: {item.get('education')}")

            degree_array = []
            if item.get('degree') is not None:
                for degree in item.get('degree'):
                    try:
                        degree_array.append(int(WorkbenchResearcherDegree(degree)))
                    except TypeError:
                        raise BadRequest(f"Invalid degree: {degree}")
            item['degree'] = degree_array

            try:
                if item.get('disability') is None:
                    item['disability'] = 'UNSET'
                WorkbenchResearcherDisability(item.get('disability'))
            except TypeError:
                raise BadRequest(f"Invalid disability: {item.get('disability')}")

            gender_array = []
            if item.get('gender') is not None:
                for gender in item.get('gender'):
                    try:
                        gender_array.append(int(WorkbenchResearcherGender(gender)))
                    except TypeError:
                        raise BadRequest(f"Invalid gender: {gender}")
            item['gender'] = gender_array

            race_array = []
            if item.get('race') is not None:
                for race in item.get('race'):
                    try:
                        race_array.append(int(WorkbenchResearcherRace(race)))
                    except TypeError:
                        raise BadRequest(f"Invalid race: {race}")
            item['race'] = race_array

            if item.get('affiliations') is not None:
                for institution in item.get('affiliations'):
                    if institution.get('nonAcademicAffiliation') is None:
                        institution['nonAcademicAffiliation'] = 'UNSET'
                    try:
                        WorkbenchInstitutionNonAcademic(institution.get('nonAcademicAffiliation'))
                    except TypeError:
                        raise BadRequest(
                            f"Invalid nonAcademicAffiliation: {institution.get('nonAcademicAffiliation')}")

    def from_client_json(self, resource_json, client_id=None):  # pylint: disable=unused-argument
        self._validate(resource_json)
        now = clock.CLOCK.now()
        researchers = []
        for item in resource_json:
            researcher = WorkbenchResearcher(
                created=now,
                modified=now,
                userSourceId=item.get('userId'),
                creationTime=parse(item.get('creationTime')) if item.get('creationTime') is not None else None,
                modifiedTime=parse(item.get('modifiedTime')),
                givenName=item.get('givenName'),
                familyName=item.get('familyName'),
                email=item.get('email'),
                streetAddress1=item.get('streetAddress1'),
                streetAddress2=item.get('streetAddress2'),
                city=item.get('city'),
                state=item.get('state'),
                zipCode=item.get('zipCode'),
                country=item.get('country'),
                ethnicity=WorkbenchResearcherEthnicity(item.get('ethnicity', 'UNSET')),
                sexAtBirth=item.get('sexAtBirth'),
                identifiesAsLgbtq=item.get('identifiesAsLgbtq'),
                lgbtqIdentity=item.get('lgbtqIdentity') if item.get('identifiesAsLgbtq') else None,
                education=WorkbenchResearcherEducation(item.get('education', 'UNSET')),
                degree=item.get('degree'),
                disability=WorkbenchResearcherDisability(item.get('disability', 'UNSET')),
                gender=item.get('gender'),
                race=item.get('race'),
                workbenchInstitutionalAffiliations=self._get_affiliations(item.get('affiliations'),
                                                                          item.get('verifiedInstitutionalAffiliation')),
                resource=json.dumps(item)
            )

            researchers.append(researcher)

        return researchers

    def _get_affiliations(self, affiliations_json, verified_affiliation_json):
        now = clock.CLOCK.now()
        affiliations = []
        if affiliations_json is not None:
            for affiliation in affiliations_json:
                affiliation_obj = WorkbenchInstitutionalAffiliations(
                    created=now,
                    modified=now,
                    institution=affiliation.get('institution'),
                    role=affiliation.get('role'),
                    nonAcademicAffiliation=WorkbenchInstitutionNonAcademic(affiliation.get('nonAcademicAffiliation',
                                                                                           'UNSET'))
                )
                affiliations.append(affiliation_obj)
        if verified_affiliation_json is not None:
            verified_affiliation_obj = WorkbenchInstitutionalAffiliations(
                created=now,
                modified=now,
                institution=verified_affiliation_json.get('institutionShortName'),
                role=verified_affiliation_json.get('institutionalRole'),
                isVerified=True
            )
            affiliations.append(verified_affiliation_obj)
        return affiliations

    def insert_with_session(self, session, researchers):
        for researcher in researchers:
            exist = self.get_researcher_by_user_id_with_session(session, researcher.userSourceId)
            if exist:
                for attr_name in researcher.__dict__.keys():
                    if not attr_name.startswith('_') and attr_name != 'created':
                        setattr(exist, attr_name, getattr(researcher, attr_name))
            else:
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
                    isVerified=affiliation.isVerified,
                    nonAcademicAffiliation=affiliation.nonAcademicAffiliation
                )
                affiliations_history.append(affiliation_obj)
            history.workbenchInstitutionalAffiliations = affiliations_history
            session.add(history)

    def get_researcher_by_user_id_with_session(self, session, user_id):
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

    def get_researcher_history_by_id_with_session(self, researcher_history_id):
        with self.session() as session:
            return session.query(WorkbenchResearcherHistory)\
                .filter(WorkbenchResearcherHistory.id == researcher_history_id).first()

    def get_id(self, obj):
        return obj.id

    def get_all_with_children(self):
        with self.session() as session:
            query = session.query(WorkbenchResearcherHistory).options(
                subqueryload(WorkbenchResearcherHistory.workbenchInstitutionalAffiliations)
            )
            return query.all()


class WorkbenchWorkspaceAuditDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceAuditDao, order_by_ending=["id"])
        self.workspace_dao = WorkbenchWorkspaceDao()
        self.workspace_snapshot_dao = WorkbenchWorkspaceHistoryDao()

    def _validate(self, resource_json):
        for item in resource_json:
            if item.get('snapshotId') is None:
                raise BadRequest('snapshotId can not be NULL')
            if item.get('reviewType') is None:
                raise BadRequest('reviewType can not be NULL')
            if item.get('displayDecision') is None:
                raise BadRequest('displayDecision can not be NULL')

            try:
                if item.get('accessDecision') is None:
                    item['accessDecision'] = 'UNSET'
                WorkbenchAuditWorkspaceAccessDecision(item.get('accessDecision'))
            except TypeError:
                raise BadRequest(f"Invalid accessDecision: {item.get('accessDecision')}")

            try:
                WorkbenchAuditWorkspaceDisplayDecision(item.get('displayDecision'))
            except TypeError:
                raise BadRequest(f"Invalid displayDecision: {item.get('displayDecision')}")

            try:
                WorkbenchAuditReviewType(item.get('reviewType'))
            except TypeError:
                raise BadRequest(f"Invalid reviewType: {item.get('reviewType')}")

    def from_client_json(self, resource_json, client_id=None):  # pylint: disable=unused-argument
        self._validate(resource_json)
        workbench_audit_records = []
        for item in resource_json:
            record = WorkbenchAudit(
                workspaceSnapshotId=item.get('snapshotId'),
                auditorPmiEmail=item.get('auditorEmail'),
                auditReviewType=WorkbenchAuditReviewType(item.get('reviewType', 'UNSET')),
                auditWorkspaceDisplayDecision=WorkbenchAuditWorkspaceDisplayDecision(
                    item.get('displayDecision', 'UNSET')),
                auditWorkspaceAccessDecision=WorkbenchAuditWorkspaceAccessDecision(
                    item.get('accessDecision', 'UNSET')),
                auditNotes=item.get('auditorNotes'),
                resource=json.dumps(item)
            )

            workbench_audit_records.append(record)

        return workbench_audit_records

    def insert_with_session(self, session, workbench_audit_records):
        for record in workbench_audit_records:
            session.add(record)
            if record.auditWorkspaceDisplayDecision == \
                WorkbenchAuditWorkspaceDisplayDecision.PUBLISH_TO_RESEARCHER_DIRECTORY and \
                record.auditWorkspaceAccessDecision == WorkbenchAuditWorkspaceAccessDecision.UNSET:
                self.add_approved_workspace_with_session(session, record.workspaceSnapshotId)
            else:
                self.remove_approved_workspace_with_session(session, record.workspaceSnapshotId)
        return workbench_audit_records

    def _get_audit_record_by_snapshot_id_with_session(self, session, workspace_snapshot_id):
        return session.query(WorkbenchAudit).filter(WorkbenchAudit.workspaceSnapshotId == workspace_snapshot_id).first()

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchAudit):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for record in obj:
                result.append(json.loads(record.resource))
            return result

    def remove_approved_workspace_with_session(self, session, workspace_snapshot_id):
        workspace_snapshot = self.workspace_snapshot_dao.get_snapshot_by_id_with_session(session, workspace_snapshot_id)
        workspace_snapshot.excludeFromPublicDirectory = True
        workspace_snapshot.isReviewed = True
        self.workspace_dao.remove_workspace_by_workspace_id_with_session(session, workspace_snapshot.workspaceSourceId)


    def add_approved_workspace_with_session(self, session, workspace_snapshot_id):
        workspace_snapshot = self.workspace_snapshot_dao.get_snapshot_by_id_with_session(session, workspace_snapshot_id)
        workspace_snapshot.excludeFromPublicDirectory = False
        workspace_snapshot.isReviewed = True
        self.workspace_dao.add_approved_workspace_with_session(session, workspace_snapshot, is_reviewed=True)
