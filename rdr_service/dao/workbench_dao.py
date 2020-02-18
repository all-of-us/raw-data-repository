import json
import sqlalchemy

from werkzeug.exceptions import BadRequest
from dateutil.parser import parse
from sqlalchemy import desc, and_
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
from rdr_service.participant_enums import WorkbenchWorkspaceStatus, WorkbenchWorkspaceUserRole, \
    WorkbenchInstitutionNonAcademic, WorkbenchResearcherEthnicity, WorkbenchResearcherSexAtBirth, \
    WorkbenchResearcherGender, WorkbenchResearcherRace, WorkbenchResearcherEducation, WorkbenchResearcherDisability, \
    WorkbenchResearcherDegree, WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, \
    WorkbenchWorkspaceSexualOrientation, WorkbenchWorkspaceGeography, WorkbenchWorkspaceDisabilityStatus, \
    WorkbenchWorkspaceAccessToCare, WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, \
    WorkbenchWorkspaceRaceEthnicity, WorkbenchWorkspaceAge


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
            workspace = WorkbenchWorkspace(
                created=now,
                modified=now,
                workspaceSourceId=item.get('workspaceId'),
                name=item.get('name'),
                creationTime=parse(item.get('creationTime')),
                modifiedTime=parse(item.get('modifiedTime')),
                status=WorkbenchWorkspaceStatus(item.get('status', 'UNSET')),
                excludeFromPublicDirectory=item.get('excludeFromPublicDirectory'),
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
                raceEthnicity=item.get("raceEthnicity"),
                age=item.get("age"),
                others=item.get('others'),
                workbenchWorkspaceUser=self._get_users(item.get('workspaceUsers')),
                resource=json.dumps(item)
            )

            workspaces.append(workspace)

        return workspaces

    def _get_users(self, workspace_users_json):
        researcher_dao = WorkbenchResearcherDao()
        now = clock.CLOCK.now()
        workspace_users = []
        for user in workspace_users_json:
            researcher = researcher_dao.get_researcher_by_user_source_id(user.get('userId'))
            if not researcher:
                raise BadRequest('Researcher not found for user ID: {}'.format(user.get('userId')))
            user_obj = WorkbenchWorkspaceUser(
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
            exist = self._get_workspace_by_workspace_id_with_session(session, workspace.workspaceSourceId)
            if exist:
                for attr_name in workspace.__dict__.keys():
                    if not attr_name.startswith('_') and attr_name != 'created':
                        setattr(exist, attr_name, getattr(workspace, attr_name))
            else:
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
                WorkbenchWorkspace.scientificApproaches.label('scientificApproaches'),
                WorkbenchWorkspace.intendToStudy.label('intendToStudy'),
                WorkbenchWorkspace.findingsFromStudy.label('findingsFromStudy'),
                WorkbenchWorkspace.focusOnUnderrepresentedPopulations.label('focusOnUnderrepresentedPopulations'),
                WorkbenchWorkspace.raceEthnicity.label('raceEthnicity'),
                WorkbenchWorkspace.age.label('age'),
                WorkbenchWorkspace.sexAtBirth.label('sexAtBirth'),
                WorkbenchWorkspace.genderIdentity.label('genderIdentity'),
                WorkbenchWorkspace.sexualOrientation.label('sexualOrientation'),
                WorkbenchWorkspace.geography.label('geography'),
                WorkbenchWorkspace.disabilityStatus.label('disabilityStatus'),
                WorkbenchWorkspace.accessToCare.label('accessToCare'),
                WorkbenchWorkspace.educationLevel.label('educationLevel'),
                WorkbenchWorkspace.incomeLevel.label('incomeLevel'),
                WorkbenchWorkspace.others.label('others'),

                WorkbenchWorkspaceUser.userId.label('userId'),
                WorkbenchWorkspaceUser.role.label('role'),
                WorkbenchResearcher.givenName.label('givenName'),
                WorkbenchResearcher.familyName.label('familyName'),

                WorkbenchInstitutionalAffiliations.institution.label('institution'),
                WorkbenchInstitutionalAffiliations.role.label('institutionRole'),
                WorkbenchInstitutionalAffiliations.nonAcademicAffiliation.label('nonAcademicAffiliation')
            ]
        ).select_from(
            sqlalchemy.outerjoin(
                sqlalchemy.outerjoin(WorkbenchWorkspace, WorkbenchWorkspaceUser,
                                     WorkbenchWorkspace.id == WorkbenchWorkspaceUser.workspaceId),
                sqlalchemy.outerjoin(WorkbenchResearcher, WorkbenchInstitutionalAffiliations,
                                     WorkbenchResearcher.id == WorkbenchInstitutionalAffiliations.researcherId),
                WorkbenchResearcher.id == WorkbenchWorkspaceUser.researcherId
            )
        ).where(and_(WorkbenchWorkspaceUser.role == WorkbenchWorkspaceUserRole.OWNER,
                     WorkbenchWorkspace.excludeFromPublicDirectory == 0))

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
                                    "nonAcademicAffiliation": str(WorkbenchInstitutionNonAcademic(
                                        row.nonAcademicAffiliation if row.nonAcademicAffiliation is not None
                                        else 'UNSET'))
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
                    "otherPurpose": row.otherPurpose,
                    "scientificApproaches": row.scientificApproaches,
                    "intendToStudy": row.intendToStudy,
                    "findingsFromStudy": row.findingsFromStudy,
                    "focusOnUnderrepresentedPopulations": row.focusOnUnderrepresentedPopulations,
                    "workspaceDemographic": {
                        "raceEthnicity": [str(WorkbenchWorkspaceRaceEthnicity(value))
                                          for value in row.raceEthnicity] if row.raceEthnicity else None,
                        "age": [str(WorkbenchWorkspaceAge(value)) for value in row.age] if row.age else None,
                        "sexAtBirth": str(WorkbenchWorkspaceSexAtBirth(row.sexAtBirth)) if row.sexAtBirth else None,
                        "genderIdentity": str(WorkbenchWorkspaceGenderIdentity(row.genderIdentity))
                        if row.genderIdentity else None,
                        "sexualOrientation": str(WorkbenchWorkspaceSexualOrientation(row.sexualOrientation))
                        if row.sexualOrientation else None,
                        "geography": str(WorkbenchWorkspaceGeography(row.geography)) if row.geography else None,
                        "disabilityStatus": str(WorkbenchWorkspaceDisabilityStatus(row.disabilityStatus))
                        if row.disabilityStatus else None,
                        "accessToCare": str(WorkbenchWorkspaceAccessToCare(row.accessToCare))
                        if row.accessToCare else None,
                        "educationLevel": str(WorkbenchWorkspaceEducationLevel(row.educationLevel))
                        if row.educationLevel else None,
                        "incomeLevel": str(WorkbenchWorkspaceIncomeLevel(row.incomeLevel))
                        if row.incomeLevel else None,
                        "others": row.others
                    }
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
            exist = self._get_researcher_by_user_id_with_session(session, researcher.userSourceId)
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
