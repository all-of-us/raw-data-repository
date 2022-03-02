import json

from werkzeug.exceptions import BadRequest
from dateutil.parser import parse
import pytz
from sqlalchemy import desc, or_, and_, func, distinct, case
from sqlalchemy.orm import subqueryload, aliased
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
    WorkbenchAuditWorkspaceDisplayDecision, WorkbenchAuditReviewType, WorkbenchWorkspaceAccessTier


class WorkbenchWorkspaceDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceApproved, order_by_ending=["id"])
        self.is_backfill = False
        self.workspace_snapshot_dao = WorkbenchWorkspaceHistoryDao()

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
                raise BadRequest('WorkspaceID:{} name can not be NULL'.format(item.get('workspaceId')))
            if item.get('creationTime') is None:
                raise BadRequest('WorkspaceID:{} creationTime can not be NULL'.format(item.get('workspaceId')))
            if item.get('modifiedTime') is None:
                raise BadRequest('WorkspaceID:{} modifiedTime can not be NULL'.format(item.get('workspaceId')))
            try:
                WorkbenchWorkspaceStatus(item.get('status'))
            except TypeError:
                raise BadRequest("WorkspaceID:{} Invalid workspace status: {}".format(item.get('workspaceId'),
                                                                                      item.get('status')))

            if item.get('workspaceUsers'):
                for user in item.get('workspaceUsers'):
                    if user.get('userId') is None:
                        raise BadRequest('WorkspaceID:{} user ID can not be NULL'.format(item.get('workspaceId')))
                    try:
                        WorkbenchWorkspaceUserRole(user.get('role'))
                    except TypeError:
                        raise BadRequest("WorkspaceID:{} Invalid user role: {}".format(item.get('workspaceId'),
                                                                                       user.get('role')))
                    try:
                        WorkbenchWorkspaceStatus(user.get('status'))
                    except TypeError:
                        raise BadRequest("WorkspaceID:{} Invalid user status: {}".format(item.get('workspaceId'),
                                                                                         user.get('status')))

            if item.get("focusOnUnderrepresentedPopulations") and item.get("workspaceDemographic"):
                race_ethnicity_array = []
                if item.get("workspaceDemographic").get('raceEthnicity') is not None:
                    for race_ethnicity in item.get("workspaceDemographic").get('raceEthnicity'):
                        try:
                            race_ethnicity_array.append(int(WorkbenchWorkspaceRaceEthnicity(race_ethnicity)))
                        except TypeError:
                            raise BadRequest("WorkspaceID:{} Invalid raceEthnicity for workspaceDemographic: {}"
                                             .format(item.get('workspaceId'), race_ethnicity))
                item['raceEthnicity'] = race_ethnicity_array

                age_array = []
                if item.get("workspaceDemographic").get('age') is not None:
                    for age in item.get("workspaceDemographic").get('age'):
                        try:
                            age_array.append(int(WorkbenchWorkspaceAge(age)))
                        except TypeError:
                            raise BadRequest("WorkspaceID:{} Invalid age for workspaceDemographic: {}"
                                             .format(item.get('workspaceId'), age))
                item['age'] = age_array

                try:
                    if item.get("workspaceDemographic").get('sexAtBirth') is None:
                        item['sexAtBirth'] = 'UNSET'
                    else:
                        item["sexAtBirth"] = item.get("workspaceDemographic").get("sexAtBirth")
                    WorkbenchWorkspaceSexAtBirth(item['sexAtBirth'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid sexAtBirth for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('sexAtBirth')))

                try:
                    if item.get("workspaceDemographic").get('genderIdentity') is None:
                        item['genderIdentity'] = 'UNSET'
                    else:
                        item["genderIdentity"] = item.get("workspaceDemographic").get("genderIdentity")
                    WorkbenchWorkspaceGenderIdentity(item['genderIdentity'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid genderIdentity for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('genderIdentity')))

                try:
                    if item.get("workspaceDemographic").get('sexualOrientation') is None:
                        item['sexualOrientation'] = 'UNSET'
                    else:
                        item["sexualOrientation"] = item.get("workspaceDemographic").get("sexualOrientation")
                    WorkbenchWorkspaceSexualOrientation(item['sexualOrientation'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid sexualOrientation for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('sexualOrientation')))

                try:
                    if item.get("workspaceDemographic").get('geography') is None:
                        item['geography'] = 'UNSET'
                    else:
                        item["geography"] = item.get("workspaceDemographic").get("geography")
                    WorkbenchWorkspaceGeography(item['geography'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid geography for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('geography')))

                try:
                    if item.get("workspaceDemographic").get('disabilityStatus') is None:
                        item['disabilityStatus'] = 'UNSET'
                    else:
                        item["disabilityStatus"] = item.get("workspaceDemographic").get("disabilityStatus")
                    WorkbenchWorkspaceDisabilityStatus(item['disabilityStatus'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid disabilityStatus for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('disabilityStatus')))

                try:
                    if item.get("workspaceDemographic").get('accessToCare') is None:
                        item['accessToCare'] = 'UNSET'
                    else:
                        item["accessToCare"] = item.get("workspaceDemographic").get("accessToCare")
                    WorkbenchWorkspaceAccessToCare(item['accessToCare'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid accessToCare for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('accessToCare')))

                try:
                    if item.get("workspaceDemographic").get('educationLevel') is None:
                        item['educationLevel'] = 'UNSET'
                    else:
                        item["educationLevel"] = item.get("workspaceDemographic").get("educationLevel")
                    WorkbenchWorkspaceEducationLevel(item['educationLevel'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid educationLevel for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('educationLevel')))

                try:
                    if item.get("workspaceDemographic").get('incomeLevel') is None:
                        item['incomeLevel'] = 'UNSET'
                    else:
                        item["incomeLevel"] = item.get("workspaceDemographic").get("incomeLevel")
                    WorkbenchWorkspaceIncomeLevel(item['incomeLevel'])
                except TypeError:
                    raise BadRequest("WorkspaceID:{} Invalid incomeLevel for workspaceDemographic: {}"
                                     .format(item.get('workspaceId'), item.get('incomeLevel')))

                if item.get("workspaceDemographic").get('others') is not None:
                    item["others"] = item.get("workspaceDemographic").get("others")

                try:
                    if item.get("accessTier") is None:
                        item['accessTier'] = 'UNSET'
                    else:
                        item["accessTier"] = item.get("accessTier")
                    WorkbenchWorkspaceAccessTier(item['accessTier'])
                except TypeError:
                    raise BadRequest(f'WorkspaceID:{item.get("workspaceId")} Invalid '
                                     f'accessTier: {item.get("accessTier")}')

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
                ethicalLegalSocialImplications=item.get('ethicalLegalSocialImplications'),
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
                workbenchWorkspaceUser=self._get_users(item.get('workspaceUsers'), item.get('creator')),
                cdrVersion=item.get('cdrVersionName'),
                accessTier=WorkbenchWorkspaceAccessTier(item.get('accessTier', 'UNSET')),
                resource=json.dumps(item)
            )

            workspaces.append(workspace)

        return workspaces

    def _get_users(self, workspace_users_json, creator_json):
        creator_user_id = creator_json.get('userId') if creator_json else None
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
                status=WorkbenchWorkspaceStatus(user.get('status', 'UNSET')),
                isCreator=(user.get('userId') == creator_user_id)
            )
            workspace_users.append(user_obj)
        return workspace_users

    def insert_with_session(self, session, workspaces):
        new_workspaces = []
        for workspace in workspaces:
            if self.is_backfill:
                backfilled_snapshot = self.backfill_workspace_with_session(session, workspace)
                if backfilled_snapshot:
                    new_workspaces.append(backfilled_snapshot)
            else:
                is_exist = self.workspace_snapshot_dao.get_exist_snapshot_with_session(session,
                                                                                       workspace.workspaceSourceId,
                                                                                       workspace.modifiedTime)
                if is_exist:
                    continue
                session.add(workspace)
                new_workspaces.append(workspace)
                if workspace.excludeFromPublicDirectory is not True:
                    self.add_approved_workspace_with_session(session, workspace)

        return new_workspaces

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchWorkspaceSnapshot):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for workspace in obj:
                result.append(json.loads(workspace.resource))
            return result

    @staticmethod
    def get_workspace_by_workspace_id_with_session(session, workspace_id):
        return session.query(WorkbenchWorkspaceApproved) \
            .filter(WorkbenchWorkspaceApproved.workspaceSourceId == workspace_id).first()

    @staticmethod
    def remove_workspace_by_workspace_id_with_session(session, workspace_id):
        workspace = session.query(WorkbenchWorkspaceApproved) \
            .filter(WorkbenchWorkspaceApproved.workspaceSourceId == workspace_id).first()
        if workspace:
            session.delete(workspace)

    def get_redcap_audit_workspaces(
        self,
        last_snapshot_id=None,
        snapshot_id=None,
        workspace_id=None
        ):

        results = []
        with self.session() as session:
            workbench_workspace_snapshot_alias = aliased(WorkbenchWorkspaceSnapshot)
            active_id = session.query(func.max(WorkbenchWorkspaceSnapshot.id))\
                .filter(WorkbenchWorkspaceSnapshot.status == WorkbenchWorkspaceStatus.ACTIVE)\
                .filter(WorkbenchWorkspaceSnapshot.workspaceSourceId ==
                        workbench_workspace_snapshot_alias.workspaceSourceId)\
                .as_scalar()
            case_stmt = case(
                [
                    (and_(workbench_workspace_snapshot_alias.status == WorkbenchWorkspaceStatus.INACTIVE,
                          active_id != None),
                     active_id)
                ],
                else_=workbench_workspace_snapshot_alias.id
            )
            subquery = session.query(case_stmt.label('active_id'), workbench_workspace_snapshot_alias).subquery()
            query = (
                session.query(WorkbenchWorkspaceSnapshot,
                              WorkbenchResearcherHistory,
                              subquery.c.id.label('current_id'),
                              subquery.c.status.label('current_status'))
                    .distinct()
                    .filter(WorkbenchWorkspaceUserHistory.researcherId == WorkbenchResearcherHistory.id,
                            WorkbenchWorkspaceSnapshot.id == subquery.c.active_id,
                            subquery.c.active_id == WorkbenchWorkspaceUserHistory.workspaceId)
            )

            if workspace_id:
                query = query.filter(WorkbenchWorkspaceSnapshot.workspaceSourceId == workspace_id)\
                    .order_by(desc(subquery.c.id)).limit(1)
            elif snapshot_id:
                query = query.filter(subquery.c.id == snapshot_id)
            elif last_snapshot_id:
                query = query.filter(subquery.c.id > last_snapshot_id)\
                    .order_by(subquery.c.id)
            items = query.all()

            for workspace, researcher, current_id, current_status in items:
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
                    if result['snapshotId'] == current_id:
                        result['workspaceResearchers'].append(workspace_researcher)
                        exist = True
                        break
                if exist:
                    continue
                record = {
                    'snapshotId': current_id,
                    'workspaceId': workspace.workspaceSourceId,
                    'name': workspace.name,
                    'creationTime': workspace.creationTime,
                    'modifiedTime': workspace.modifiedTime,
                    'status': str(WorkbenchWorkspaceStatus(current_status)),
                    'workspaceUsers': [
                        {
                            "userId": user.userId,
                            "role": str(WorkbenchWorkspaceUserRole(user.role)) if user.role else 'UNSET',
                            "status": str(WorkbenchWorkspaceStatus(user.status)) if user.status else 'UNSET',
                            "isCreator": user.isCreator
                        } for user in workspace.workbenchWorkspaceUser
                    ] if workspace.workbenchWorkspaceUser else [],
                    'workspaceResearchers': [workspace_researcher],
                    "excludeFromPublicDirectory": workspace.excludeFromPublicDirectory,
                    "ethicalLegalSocialImplications": workspace.ethicalLegalSocialImplications,
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
                    "accessTier": str(WorkbenchWorkspaceAccessTier(workspace.accessTier
                                                                   if workspace.accessTier else 0)),
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
                    },
                    "cdrVersion": workspace.cdrVersion
                }
                results.append(record)

        return results

    def get_workspaces_with_user_detail(self, **kwargs):
        status = kwargs.get('status')
        sequest_hour = kwargs.get('sequest_hour')
        given_name = kwargs.get('given_name')
        family_name = kwargs.get('family_name')
        owner_name = kwargs.get('owner_name')
        user_source_id = kwargs.get('user_source_id')
        user_role = kwargs.get('user_role')
        workspace_name_like = kwargs.get('workspace_name_like')
        intend_to_study_like = kwargs.get('intend_to_study_like')
        workspace_like = kwargs.get('workspace_like')
        project_purpose = kwargs.get('project_purpose')
        page = kwargs.get('page')
        page_size = kwargs.get('page_size')
        if page and page_size:
            offset = (page - 1) * page_size
        if offset < 0:
            raise BadRequest("invalid parameter: page")

        results = []
        now = clock.CLOCK.now()
        sequest_hours_ago = now - timedelta(hours=sequest_hour)
        with self.session() as session:
            subquery = (
                session.query(distinct(WorkbenchWorkspaceUser.workspaceId))
                    .filter(WorkbenchWorkspaceUser.isCreator == 1)
                    .subquery()
            )
            count_query = (session.query(distinct(WorkbenchWorkspaceApproved.workspaceSourceId))
                           .join(WorkbenchWorkspaceUser, WorkbenchResearcher, WorkbenchInstitutionalAffiliations)
                           .filter(WorkbenchWorkspaceApproved.excludeFromPublicDirectory == 0,
                                   WorkbenchWorkspaceApproved.status == int(WorkbenchWorkspaceStatus.ACTIVE),
                                   WorkbenchInstitutionalAffiliations.isVerified == 1,
                                   or_(WorkbenchWorkspaceUser.isCreator == 1,
                                       and_(
                                           WorkbenchWorkspaceUser.role == int(WorkbenchWorkspaceUserRole.OWNER),
                                           WorkbenchWorkspaceApproved.id.notin_(subquery)
                                       )),
                                   or_(WorkbenchWorkspaceApproved.modified < sequest_hours_ago,
                                       WorkbenchWorkspaceApproved.isReviewed == 1))
                           )
            total = count_query.count()

            snapshot_subquery = (
                session.query(func.max(WorkbenchWorkspaceSnapshot.id).label('snapshot_id'),
                              WorkbenchWorkspaceSnapshot.workspaceSourceId)
                    .filter(WorkbenchWorkspaceSnapshot.excludeFromPublicDirectory == 0)
                    .group_by(WorkbenchWorkspaceSnapshot.workspaceSourceId).subquery()
            )
            query = (
                session.query(WorkbenchWorkspaceApproved, WorkbenchResearcher, WorkbenchWorkspaceUser.role,
                              snapshot_subquery.c.snapshot_id)
                    .filter(WorkbenchWorkspaceUser.researcherId == WorkbenchResearcher.id,
                            WorkbenchWorkspaceApproved.id == WorkbenchWorkspaceUser.workspaceId,
                            WorkbenchWorkspaceApproved.excludeFromPublicDirectory == 0,
                            WorkbenchWorkspaceApproved.workspaceSourceId == snapshot_subquery.c.workspace_source_id,
                            or_(WorkbenchWorkspaceApproved.modified < sequest_hours_ago,
                                WorkbenchWorkspaceApproved.isReviewed == 1))
                    .order_by(desc(WorkbenchWorkspaceApproved.modifiedTime))
            )

            if status is not None:
                query = query.filter(WorkbenchWorkspaceApproved.status == status)

            if workspace_like:
                query = query.filter(or_(func.lower(WorkbenchWorkspaceApproved.name).like(workspace_like),
                                         func.lower(WorkbenchWorkspaceApproved.intendToStudy).like(workspace_like)))
            else:
                if workspace_name_like:
                    query = query.filter(func.lower(WorkbenchWorkspaceApproved.name).like(workspace_name_like))

                if intend_to_study_like:
                    query = query.filter(func.lower(WorkbenchWorkspaceApproved.intendToStudy)
                                         .like(intend_to_study_like))

            if project_purpose:
                for purpose in project_purpose:
                    query = query.filter(getattr(WorkbenchWorkspaceApproved, purpose) == 1)

            items = query.all()

            for workspace, researcher, role, snapshot_id in items:
                affiliations = []
                researcher_has_verified_institution = False
                workspace_has_verified_institution = False
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
                        if affiliation.isVerified is True:
                            researcher_has_verified_institution = True
                owner_user_ids = []
                creator_user_id = None
                for workspace_user in workspace.workbenchWorkspaceUser:
                    if workspace_user.role == WorkbenchWorkspaceUserRole.OWNER:
                        owner_user_ids.append(workspace_user.userId)
                    if workspace_user.isCreator is True:
                        creator_user_id = workspace_user.userId
                # if has creator, calculate by creator; if no creator, calculate by owners
                if researcher_has_verified_institution and creator_user_id == researcher.userSourceId:
                    workspace_has_verified_institution = True
                elif creator_user_id is None and researcher.userSourceId in owner_user_ids and \
                    researcher_has_verified_institution:
                    workspace_has_verified_institution = True

                user = {
                    'userId': researcher.userSourceId,
                    'userName': researcher.givenName + ' ' + researcher.familyName,
                    'degree': [str(WorkbenchResearcherDegree(value)) for value in researcher.degree],
                    'affiliations': affiliations
                }
                hit_search = False
                if owner_name and role == WorkbenchWorkspaceUserRole('OWNER'):
                    if owner_name in user.get('userName').lower():
                        hit_search = True
                elif role == WorkbenchWorkspaceUserRole('OWNER'):
                    if given_name and given_name in researcher.givenName.lower():
                        hit_search = True
                    if family_name and family_name in researcher.familyName.lower():
                        hit_search = True
                if user_source_id and user_role:
                    if user_source_id == researcher.userSourceId and role == WorkbenchWorkspaceUserRole('OWNER') \
                        and user_role == 'owner':
                        hit_search = True
                    elif user_source_id == researcher.userSourceId and role != WorkbenchWorkspaceUserRole('OWNER') \
                        and user_role == 'member':
                        hit_search = True
                    elif user_source_id == researcher.userSourceId and user_role == 'all':
                        hit_search = True

                exist = False
                for result in results:
                    if result['workspaceId'] == workspace.workspaceSourceId:
                        result['workspaceUsers'].append(user)
                        if hit_search:
                            result['hitSearch'] = True
                        if user.get('userId') in owner_user_ids:
                            result['workspaceOwner'].append(user)
                        if workspace_has_verified_institution:
                            result['hasVerifiedInstitution'] = True
                        exist = True
                        break
                if exist:
                    continue

                record = {
                    'workspaceId': workspace.workspaceSourceId,
                    'snapshotId': snapshot_id,
                    'hitSearch': hit_search,
                    'name': workspace.name,
                    'creationTime': workspace.creationTime,
                    'modifiedTime': workspace.modifiedTime,
                    'status': str(WorkbenchWorkspaceStatus(workspace.status)),
                    'workspaceUsers': [user] if user else [],
                    'workspaceOwner': [user] if user.get('userId') in owner_user_ids else [],
                    'hasVerifiedInstitution': workspace_has_verified_institution,
                    "excludeFromPublicDirectory": workspace.excludeFromPublicDirectory,
                    "ethicalLegalSocialImplications": workspace.ethicalLegalSocialImplications,
                    "reviewRequested": workspace.reviewRequested if workspace.reviewRequested else False,
                    "diseaseFocusedResearch": workspace.diseaseFocusedResearch,
                    "diseaseFocusedResearchName": workspace.diseaseFocusedResearchName,
                    "otherPurposeDetails": workspace.otherPurposeDetails,
                    "methodsDevelopment": workspace.methodsDevelopment,
                    "controlSet": workspace.controlSet,
                    "ancestry": workspace.ancestry,
                    "accessTier": str(WorkbenchWorkspaceAccessTier(workspace.accessTier
                                                                   if workspace.accessTier else 0)),
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
                    },
                    "cdrVersion": workspace.cdrVersion
                }
                results.append(record)

        if owner_name or given_name or family_name or (user_source_id and user_role):
            expected_result = [ws for ws in results if ws.get('hitSearch') is True
                               and ws.get('hasVerifiedInstitution') is True]
        else:
            expected_result = [ws for ws in results if ws.get('hasVerifiedInstitution') is True]

        for er in expected_result:
            er.pop('hitSearch', None)

        match_number = len(expected_result)
        if offset >= match_number:
            expected_result = []
        else:
            page_end = offset + page_size if offset + page_size < match_number else match_number
            expected_result = expected_result[offset:page_end]
        metadata_dao = MetadataDao()
        metadata = metadata_dao.get_by_key(WORKBENCH_LAST_SYNC_KEY)
        if metadata:
            last_sync_date = metadata.dateValue
        else:
            last_sync_date = clock.CLOCK.now()

        return {
            "totalActiveProjects": total,
            "totalMatchedRecords": match_number,
            "page": page,
            "pageSize": page_size,
            "last_sync_date": last_sync_date,
            "data": expected_result
        }

    def backfill_workspace_with_session(self, session, backfilled_workspace):
        exist_approved = self.get_workspace_by_workspace_id_with_session(session,
                                                                         backfilled_workspace.workspaceSourceId)
        if exist_approved:
            for k, v in backfilled_workspace:
                if k not in ('id', 'workbenchWorkspaceUser', 'excludeFromPublicDirectory', 'isReviewed'):
                    setattr(exist_approved, k, v)

        exist_snapshot = self.workspace_snapshot_dao.get_exist_snapshot_with_session(
            session, backfilled_workspace.workspaceSourceId, backfilled_workspace.modifiedTime)
        if exist_snapshot:
            for k, v in backfilled_workspace:
                if k not in ('id', 'workbenchWorkspaceUser', 'excludeFromPublicDirectory', 'isReviewed'):
                    setattr(exist_snapshot, k, v)

        return exist_snapshot

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
                status=user.status,
                isCreator=user.isCreator
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
        return session.query(WorkbenchWorkspaceSnapshot) \
            .options(subqueryload(WorkbenchWorkspaceSnapshot.workbenchWorkspaceUser)) \
            .filter(WorkbenchWorkspaceSnapshot.id == snapshot_id).first()

    def get_exist_snapshot_with_session(self, session, workspace_id, modified_time):
        record = session.query(WorkbenchWorkspaceSnapshot) \
            .options(subqueryload(WorkbenchWorkspaceSnapshot.workbenchWorkspaceUser))\
            .filter(WorkbenchWorkspaceSnapshot.workspaceSourceId == workspace_id,
                    WorkbenchWorkspaceSnapshot.modifiedTime == modified_time) \
            .first()
        return record

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

    def get_redcap_audit_researchers(
        self,
        last_snapshot_id=None,
        snapshot_id=None,
        user_source_id=None
        ):
        results = []
        with self.session() as session:
            researchers = (
                session.query(WorkbenchResearcher)
            )
            if user_source_id:
                researchers = researchers.filter(WorkbenchResearcher.userSourceId == user_source_id) \
                    .order_by(desc(WorkbenchResearcher.id)).limit(1)
            elif snapshot_id:
                researchers = researchers.filter(WorkbenchResearcher.id == snapshot_id)
            elif last_snapshot_id:
                researchers = researchers.filter(WorkbenchResearcher.id > last_snapshot_id) \
                    .order_by(WorkbenchResearcher.id)

            for researcher in researchers.all():
                affiliations = []
                if researcher.workbenchInstitutionalAffiliations:
                    for affiliation in researcher.workbenchInstitutionalAffiliations:
                        affiliations.append(
                            {
                                "institution": affiliation.institution,
                                "role": affiliation.role,
                                "isVerified": affiliation.isVerified,
                                "nonAcademicAffiliation":
                                    str(WorkbenchInstitutionNonAcademic(affiliation.nonAcademicAffiliation or 0))
                            }
                        )
                results.append({
                    'givenName': researcher.givenName,
                    'familyName': researcher.familyName,
                    'email': researcher.email,
                    'affiliations': affiliations
                })

            return results

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

    @staticmethod
    def _get_affiliations(affiliations_json, verified_affiliation_json):
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
                institution=verified_affiliation_json.get('institutionDisplayName')
                if verified_affiliation_json.get('institutionDisplayName')
                else verified_affiliation_json.get('institutionShortName'),
                role=verified_affiliation_json.get('institutionalRole'),
                isVerified=True
            )
            affiliations.append(verified_affiliation_obj)
        return affiliations

    def insert_with_session(self, session, researchers):
        new_researcher_source_ids = []
        new_researchers = []
        new_researchers_map = {}
        researcher_snapshot_dao = WorkbenchResearcherHistoryDao()
        for researcher in researchers:
            is_snapshot_exist = researcher_snapshot_dao.is_snapshot_exist_with_session(session, researcher.userSourceId,
                                                                                       researcher.modifiedTime)
            if is_snapshot_exist:
                continue
            new_researchers_map[researcher.userSourceId] = researcher
            new_researcher_source_ids.append(researcher.userSourceId)

        exists = self.get_researchers_by_user_id_list_with_session(session, new_researcher_source_ids)
        exist_researcher_source_ids = []
        for exist in exists:
            new_researcher = new_researchers_map[exist.userSourceId]
            for attr_name in new_researcher.__dict__.keys():
                if not attr_name.startswith('_') and attr_name != 'created':
                    setattr(exist, attr_name, getattr(new_researcher, attr_name))
            new_researcher.id = exist.id
            exist_researcher_source_ids.append(exist.userSourceId)
            new_researchers.append(new_researcher)

        not_exist_list = list(set(new_researcher_source_ids) - set(exist_researcher_source_ids))
        for source_id in not_exist_list:
            session.add(new_researchers_map[source_id])
            new_researchers.append(new_researchers_map[source_id])

        self._insert_history(session, new_researchers)
        return new_researchers

    def to_client_json(self, obj):
        if isinstance(obj, WorkbenchResearcher):
            return json.loads(obj.resource)
        elif isinstance(obj, list):
            result = []
            for researcher in obj:
                result.append(json.loads(researcher.resource))
            return result

    @staticmethod
    def _insert_history(session, researchers):
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

    @staticmethod
    def get_researcher_by_user_id_with_session(session, user_id):
        return session.query(WorkbenchResearcher).filter(WorkbenchResearcher.userSourceId == user_id) \
            .order_by(desc(WorkbenchResearcher.created)).first()

    @staticmethod
    def get_researchers_by_user_id_list_with_session(session, user_id_list):
        return session.query(WorkbenchResearcher).filter(WorkbenchResearcher.userSourceId.in_(user_id_list)).all()


class WorkbenchResearcherHistoryDao(UpdatableDao):
    def __init__(self):
        super().__init__(WorkbenchResearcherHistory, order_by_ending=["id"])

    def get_researcher_history_by_user_source_id(self, user_source_id):
        with self.session() as session:
            return session.query(WorkbenchResearcherHistory).filter(WorkbenchResearcherHistory.userSourceId ==
                                                                    user_source_id) \
                .order_by(desc(WorkbenchResearcherHistory.created)).first()

    def is_snapshot_exist_with_session(self, session, user_source_id, modified_time):
        record = session.query(WorkbenchResearcherHistory) \
            .filter(WorkbenchResearcherHistory.userSourceId == user_source_id,
                    WorkbenchResearcherHistory.modifiedTime == modified_time) \
            .first()
        return True if record else False

    def get_researcher_history_by_id_with_session(self, researcher_history_id):
        with self.session() as session:
            return session.query(WorkbenchResearcherHistory) \
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
