import json

from sqlalchemy.sql import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_workbench_researcher import BQRWBResearcherSchema, BQRWBInstitutionalAffiliationsSchema, \
    BQRWBResearcher, BQRWBInstitutionalAffiliations
from rdr_service.model.bq_workbench_workspace import BQRWBWorkspaceSchema, BQRWBWorkspaceUsersSchema, \
    BQRWBWorkspace, BQRWBWorkspaceUsers
from rdr_service.participant_enums import WorkbenchResearcherDegree, WorkbenchResearcherSexAtBirth, \
    WorkbenchResearcherEthnicity, WorkbenchResearcherDisability, WorkbenchResearcherEducation, \
    WorkbenchResearcherRace, WorkbenchResearcherGender, WorkbenchWorkspaceUserRole, WorkbenchWorkspaceStatus, \
    WorkbenchInstitutionNonAcademic, WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, \
    WorkbenchWorkspaceSexualOrientation, WorkbenchWorkspaceGeography, WorkbenchWorkspaceAccessToCare, \
    WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, WorkbenchWorkspaceAge, \
    WorkbenchWorkspaceRaceEthnicity, WorkbenchWorkspaceAccessTier


class BQRWBWorkspaceGenerator(BigQueryGenerator):
    """
    Generate a Research Workbench Workspace BQRecord object
    """

    def make_bqrecord(self, src_pk_id, convert_to_enum=False):
        """
        Build a BQRecord object from the given primary key id.
        :param src_pk_id: Primary key value.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        ro_dao = BigQuerySyncDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_workspace_snapshot where id = :id'),
                    {'id': src_pk_id}).first()
            data = ro_dao.to_dict(row)

            data['status'] = str(WorkbenchWorkspaceStatus(row.status))
            data['status_id'] = int(WorkbenchWorkspaceStatus(row.status))

            race_ethnicity = json.loads(row.race_ethnicity if row.race_ethnicity and
                                                              row.race_ethnicity != 'null' else '[]')
            data['race_ethnicities'] = [{'race_ethnicity': str(WorkbenchWorkspaceRaceEthnicity(v)),
                                       'race_ethnicity_id': int(WorkbenchWorkspaceRaceEthnicity(v))} for v in
                                      race_ethnicity]

            age = json.loads(row.age if row.age and row.age != 'null' else '[]')
            data['ages'] = [{'age': str(WorkbenchWorkspaceAge(v)),
                            'age_id': int(WorkbenchWorkspaceAge(v))} for v in age]

            data['sex_at_birth'] = str(WorkbenchWorkspaceSexAtBirth(row.sex_at_birth))
            data['sex_at_birth_id'] = int(WorkbenchWorkspaceSexAtBirth(row.sex_at_birth))

            data['gender_identity'] = str(WorkbenchWorkspaceGenderIdentity(row.gender_identity))
            data['gender_identity_id'] = int(WorkbenchWorkspaceGenderIdentity(row.gender_identity))

            data['sexual_orientation'] = str(WorkbenchWorkspaceSexualOrientation(row.sexual_orientation))
            data['sexual_orientation_id'] = int(WorkbenchWorkspaceSexualOrientation(row.sexual_orientation))

            data['geography'] = str(WorkbenchWorkspaceGeography(row.geography))
            data['geography_id'] = int(WorkbenchWorkspaceGeography(row.geography))

            data['disability_status'] = str(WorkbenchResearcherDisability(row.disability_status))
            data['disability_status_id'] = int(WorkbenchResearcherDisability(row.disability_status))

            data['access_to_care'] = str(WorkbenchWorkspaceAccessToCare(row.access_to_care))
            data['access_to_care_id'] = int(WorkbenchWorkspaceAccessToCare(row.access_to_care))

            data['education_level'] = str(WorkbenchWorkspaceEducationLevel(row.education_level))
            data['education_level_id'] = int(WorkbenchWorkspaceEducationLevel(row.education_level))

            data['income_level'] = str(WorkbenchWorkspaceIncomeLevel(row.income_level))
            data['income_level_id'] = int(WorkbenchWorkspaceIncomeLevel(row.income_level))

            data['access_tier'] = str(WorkbenchWorkspaceAccessTier(row.access_tier))
            data['access_tier_id'] = int(WorkbenchWorkspaceAccessTier(row.access_tier))

            return BQRecord(schema=BQRWBWorkspaceSchema, data=data, convert_to_enum=convert_to_enum)


def bq_workspace_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate Workspace record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQRWBWorkspaceGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQRWBWorkspaceGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(bqr.workspace_source_id, bqr, bqtable=BQRWBWorkspace, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_workspace_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQRWBWorkspaceGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_workspace_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQRWBWorkspaceUsersGenerator(BigQueryGenerator):
    """
    Generate a Research Workbench Workspace BQRecord object
    """

    def make_bqrecord(self, pk_id, convert_to_enum=False):
        """
        Build a BQRecord object from the given primary key id.
        :param pk_id: Primary key value.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        ro_dao = BigQuerySyncDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_workspace_user_history where id = :id order by modified desc'),
                        {'id': pk_id}).first()
            # Fall back to workbench_workspace_user, it looks like there are missing records in the history table.
            if not row:
                row = ro_session.execute(
                    text('select * from rdr.workbench_workspace_user where id = :id order by modified desc'),
                        {'id': pk_id}).first()
            data = ro_dao.to_dict(row)

            if row.role:
                data['role'] = str(WorkbenchWorkspaceUserRole(row.role))
                data['role_id'] = int(WorkbenchWorkspaceUserRole(row.role))

            data['status'] = str(WorkbenchWorkspaceStatus(row.status))
            data['status_id'] = int(WorkbenchWorkspaceStatus(row.status))

            return BQRecord(schema=BQRWBWorkspaceUsersSchema, data=data, convert_to_enum=convert_to_enum)


def bq_workspace_user_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate Workspace Users record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQRWBWorkspaceUsersGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQRWBWorkspaceUsersGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQRWBWorkspaceUsers, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_workspace_user_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQRWBWorkspaceUsersGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_workspace_user_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQRWBResearcherGenerator(BigQueryGenerator):
    """
    Generate a Research Workbench Workspace BQRecord object
    """

    def make_bqrecord(self, src_pk_id, convert_to_enum=False):
        """
        Build a BQRecord object from the given primary key id.
        :param src_pk_id: Primary key value.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        ro_dao = BigQuerySyncDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_researcher where id = :id'), {'id': src_pk_id}).first()
            if not row:
                return None
            data = ro_dao.to_dict(row)

            if row.zip_code and len(row.zip_code) > 3:
                data['zip_code'] = row.zip_code[:3]

            data['ethnicity'] = str(WorkbenchResearcherEthnicity(row.ethnicity))
            data['ethnicity_id'] = int(WorkbenchResearcherEthnicity(row.ethnicity))

            genders = json.loads(row.gender if row.gender and row.gender != 'null' else '[]')
            data['genders'] = [{'gender': str(WorkbenchResearcherGender(v)),
                               'gender_id': int(WorkbenchResearcherGender(v))} for v in genders]

            races = json.loads(row.race if row.race and row.race != 'null' else '[]')
            data['races'] = [{'race': str(WorkbenchResearcherRace(v)),
                             'race_id': int(WorkbenchResearcherRace(v))} for v in races]

            sex_at_birth = json.loads(row.sex_at_birth if row.sex_at_birth and row.sex_at_birth != 'null' else '[]')
            data['sex_at_birth'] = [{'sex_at_birth': str(WorkbenchResearcherSexAtBirth(v)),
                                     'sex_at_birth_id': int(WorkbenchResearcherSexAtBirth(v))} for v in
                                    sex_at_birth]

            data['education'] = str(WorkbenchResearcherEducation(row.education))
            data['education_id'] = int(WorkbenchResearcherEducation(row.education))

            degrees = json.loads(row.degree if row.degree and row.degree != 'null' else '[]')
            data['degrees'] = [{'degree': str(WorkbenchResearcherDegree(v)),
                               'degree_id': int(WorkbenchResearcherDegree(v))} for v in degrees]

            data['disability'] = str(WorkbenchResearcherDisability(row.disability))
            data['disability_id'] = int(WorkbenchResearcherDisability(row.disability))

            return BQRecord(schema=BQRWBResearcherSchema, data=data, convert_to_enum=convert_to_enum)


def bq_researcher_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate Researcher record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQRWBResearcherGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQRWBResearcherGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(bqr.user_source_id, bqr, bqtable=BQRWBResearcher, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_researcher_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQRWBResearcherGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_researcher_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQRWBInstitutionalAffiliationsGenerator(BigQueryGenerator):
    """
    Generate a Research Workbench Workspace BQRecord object
    """

    def make_bqrecord(self, pk_id, convert_to_enum=False):
        """
        Build a BQRecord object from the given primary key id.
        :param pk_id: Primary key value.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :return: BQRecord object
        """
        ro_dao = BigQuerySyncDao(backup=True)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_institutional_affiliations where id = :id'), {'id': pk_id}).first()
            data = ro_dao.to_dict(row)

            data['non_academic_affiliation'] = str(WorkbenchInstitutionNonAcademic(row.non_academic_affiliation))
            data['non_academic_affiliation_id'] = int(WorkbenchInstitutionNonAcademic(row.non_academic_affiliation))

            return BQRecord(schema=BQRWBInstitutionalAffiliationsSchema, data=data, convert_to_enum=convert_to_enum)


def bq_institutional_affiliations_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate Institutional Affiliations record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQRWBInstitutionalAffiliationsGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQRWBInstitutionalAffiliationsGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQRWBInstitutionalAffiliations, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_institutional_affiliations_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQRWBInstitutionalAffiliationsGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_institutional_affiliations_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


def rebuild_bq_workpaces(workspaces):
    """
    Rebuild BQ workbench workspaces.
    :param workspaces: Array of workbench workspace models.
    """
    if not workspaces:
        return

    workspace_gen = BQRWBWorkspaceGenerator()
    users_gen = BQRWBWorkspaceUsersGenerator()
    w_dao = BigQuerySyncDao()
    with w_dao.session() as w_session:

        for workspace in workspaces:
            bqws_rec = workspace_gen.make_bqrecord(workspace.id)
            workspace_gen.save_bqrecord(workspace.workspaceSourceId, bqws_rec, BQRWBWorkspace, w_dao, w_session)

            if workspace.workbenchWorkspaceUser:
                for user in workspace.workbenchWorkspaceUser:
                    bquser_rec = users_gen.make_bqrecord(user.id)
                    users_gen.save_bqrecord(user.id, bquser_rec, BQRWBWorkspaceUsers, w_dao, w_session)


def rebuild_bq_wb_researchers(researchers):
    """
    Rebuild BQ workbench workspaces.
    :param researchers: Array of workbench researcher models.
    """
    if not researchers:
        return

    researcher_gen = BQRWBResearcherGenerator()
    affiliation_gen = BQRWBInstitutionalAffiliationsGenerator()
    w_dao = BigQuerySyncDao()
    with w_dao.session() as w_session:

        for obj in researchers:
            wb_bqr = researcher_gen.make_bqrecord(obj.id)
            if not wb_bqr:
                continue
            researcher_gen.save_bqrecord(obj.userSourceId, wb_bqr, BQRWBResearcher, w_dao, w_session)

            if obj.workbenchInstitutionalAffiliations:
                for aff in obj.workbenchInstitutionalAffiliations:
                    wbu_bqr = affiliation_gen.make_bqrecord(aff.id)
                    if not wbu_bqr:
                        continue
                    affiliation_gen.save_bqrecord(aff.id, wbu_bqr, BQRWBInstitutionalAffiliations, w_dao,
                                                  w_session)
