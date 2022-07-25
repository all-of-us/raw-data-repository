#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import json

from sqlalchemy.sql import text
from werkzeug.exceptions import NotFound

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.resource import generators, schemas
from rdr_service.participant_enums import WorkbenchResearcherDegree, WorkbenchResearcherSexAtBirth, \
    WorkbenchResearcherEthnicity, WorkbenchResearcherDisability, WorkbenchResearcherEducation, \
    WorkbenchResearcherRace, WorkbenchResearcherGender, WorkbenchWorkspaceUserRole, WorkbenchWorkspaceStatus, \
    WorkbenchInstitutionNonAcademic, WorkbenchWorkspaceSexAtBirth, WorkbenchWorkspaceGenderIdentity, \
    WorkbenchWorkspaceSexualOrientation, WorkbenchWorkspaceGeography, WorkbenchWorkspaceAccessToCare, \
    WorkbenchWorkspaceEducationLevel, WorkbenchWorkspaceIncomeLevel, WorkbenchWorkspaceAge, \
    WorkbenchWorkspaceRaceEthnicity, WorkbenchWorkspaceAccessTier, WorkbenchResearcherAccessTierShortName, \
    WorkbenchResearcherSexAtBirthV2, WorkbenchResearcherYesNoPreferNot, WorkbenchResearcherEducationV2, \
    WorkbenchResearcherEthnicCategory, WorkbenchResearcherGenderIdentity, WorkbenchResearcherSexualOrientationV2


class WBWorkspaceGenerator(generators.BaseGenerator):
    """
    Generate a workbench workspace resource object
    """
    ro_dao = None

    def make_resource(self, src_pk_id, backup=False):
        """
        Build a resource object from the given primary key id.
        :param src_pk_id: workbench workspace snapshot ID.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_workspace_snapshot where id = :id'),
                {'id': src_pk_id}).first()
            data = self.ro_dao.to_dict(row)

            if not data:
                msg = f'Workspace id {src_pk_id} not found in workbench_workspace_snapshot table.'
                logging.error(msg)
                raise NotFound(msg)

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

            return generators.ResourceRecordSet(schemas.WorkbenchWorkspaceSchema, data)


def res_workspace_batch_update(_ids):
    """
    Update a batch of ids.
    :param _ids: list of ids
    """
    gen = generators.WBWorkspaceGenerator()
    for _id in _ids:
        res = gen.make_resource(_id)
        res.save()


class WBWorkspaceUsersGenerator(generators.BaseGenerator):
    """
    Generate a workbench workspace user resource object
    """
    ro_dao = None

    def make_resource(self, pk_id, backup=False):
        """
        Build a resource object from the given primary key id.
        :param pk_id: Primary key value.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_workspace_user_history where id = :id order by modified desc'),
                {'id': pk_id}).first()
            # Fall back to workbench_workspace_user, it looks like there are missing records in the history table.
            if not row:
                row = ro_session.execute(
                    text('select * from rdr.workbench_workspace_user where id = :id order by modified desc'),
                    {'id': pk_id}).first()
            data = self.ro_dao.to_dict(row)

            if not data:
                msg = f'Workspace user id {pk_id} not found in workbench_workspace_user table.'
                logging.error(msg)
                raise NotFound(msg)

            if row.role:
                data['role'] = str(WorkbenchWorkspaceUserRole(row.role))
                data['role_id'] = int(WorkbenchWorkspaceUserRole(row.role))

            data['status'] = str(WorkbenchWorkspaceStatus(row.status))
            data['status_id'] = int(WorkbenchWorkspaceStatus(row.status))

            return generators.ResourceRecordSet(schemas.WorkbenchWorkspaceUsersSchema, data)


def res_workspace_user_batch_update(_ids):
    """
    Update a batch of ids.
    :param _ids: list of ids
    """
    gen = generators.WBWorkspaceUsersGenerator()
    for _id in _ids:
        res = gen.make_resource(_id)
        res.save()


class WBInstitutionalAffiliationsGenerator(generators.BaseGenerator):
    """
    Generate a workspace institutional affiliation object
    """
    ro_dao = None

    def make_resource(self, pk_id, backup=False):
        """
        Build a resource object from the given primary key id.
        :param pk_id: Primary key value.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_institutional_affiliations where id = :id'), {'id': pk_id}).first()
            data = self.ro_dao.to_dict(row)
            if not data:
                msg = f'Institutional affiliation id {pk_id} not found in workbench_institutional_affiliations table.'
                logging.error(msg)
                raise NotFound(msg)

            data['non_academic_affiliation'] = str(WorkbenchInstitutionNonAcademic(row.non_academic_affiliation))
            data['non_academic_affiliation_id'] = int(WorkbenchInstitutionNonAcademic(row.non_academic_affiliation))
            return generators.ResourceRecordSet(schemas.WorkbenchInstitutionalAffiliationsSchema, data)


def res_institutional_affiliations_batch_update(_ids):
    """
    Update a batch of ids.
    :param _ids: list of ids
    """

    gen = generators.WBInstitutionalAffiliationsGenerator()
    for _id in _ids:
        res = gen.make_resource(_id)
        res.save()


class WBResearcherGenerator(generators.BaseGenerator):
    """
    Generate a researcher resource object
    """
    ro_dao = None

    def make_resource(self, src_pk_id, backup=False):
        """
        Build a resource object from the given primary key id.
        :param src_pk_id: Primary key value.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from rdr.workbench_researcher where id = :id'), {'id': src_pk_id}).first()
            if not row:
                return None
            data = self.ro_dao.to_dict(row)
            if not data:
                msg = f'Researcher id {src_pk_id} not found in workbench_researcher table.'
                logging.error(msg)
                raise NotFound(msg)

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

            # New fields and sub-tables for PDR-826
            data['identifies_as_lgbtq'] = row.identifies_as_lgbtq
            # The 'lgbtq_identity' field seems to support free-text, forcing boolean.
            data['lgbtq_identity'] = 1 if row.lgbtq_identity else 0

            access_tier_short_names = json.loads(row.access_tier_short_names
                                                 if row.access_tier_short_names and
                                                    row.access_tier_short_names != 'null' else '[]')
            data['access_tier_short_name'] = [
                {
                    'access_tier_short_name': str(WorkbenchResearcherAccessTierShortName(v)),
                    'access_tier_short_name_id': int(WorkbenchResearcherAccessTierShortName(v))
                 } for v in access_tier_short_names
            ]
            data['dsv2_completion_time'] = row.dsv2_completion_time

            data['dsv2_disability_concentrating'] = str(
                WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_concentrating))
            data['dsv2_disability_concentrating_id'] = int(
                WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_concentrating))
            data['dsv2_disability_dressing'] = str(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_dressing))
            data['dsv2_disability_dressing_id'] = int(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_dressing))
            data['dsv2_disability_errands'] = str(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_errands))
            data['dsv2_disability_errands_id'] = int(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_errands))
            data['dsv2_disability_hearing'] = str(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_hearing))
            data['dsv2_disability_hearing_id'] = int(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_hearing))

            data['dsv2_disability_other'] = 1 if row.dsv2_disability_other else 0

            data['dsv2_disability_seeing'] = str(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_seeing))
            data['dsv2_disability_seeing_id'] = int(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_seeing))
            data['dsv2_disability_walking'] = str(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_walking))
            data['dsv2_disability_walking_id'] = int(WorkbenchResearcherYesNoPreferNot(row.dsv2_disability_walking))
            data['dsv2_disadvantaged'] = str(WorkbenchResearcherYesNoPreferNot(row.dsv2_disadvantaged))
            data['dsv2_disadvantaged_id'] = int(WorkbenchResearcherYesNoPreferNot(row.dsv2_disadvantaged))
            data['dsv2_education'] = str(WorkbenchResearcherEducationV2(row.dsv2_education))
            data['dsv2_education_id'] = int(WorkbenchResearcherEducationV2(row.dsv2_education))

            ethnic_categories = json.loads(
                row.dsv2_ethnic_categories if row.dsv2_ethnic_categories
                                              and row.dsv2_ethnic_categories != 'null' else '[]'
            )
            data['dsv2_ethnic_category'] = [
                {
                    'dsv2_ethnic_category': str(WorkbenchResearcherEthnicCategory(v)),
                    'dsv2_ethnic_category_id': int(WorkbenchResearcherEthnicCategory(v))
                } for v in ethnic_categories
            ]

            data['dsv2_ethnicity_aian_other'] = 1 if row.dsv2_ethnicity_aian_other else 0
            data['dsv2_ethnicity_asian_other'] = 1 if row.dsv2_ethnicity_asian_other else 0
            data['dsv2_ethnicity_other'] = 1 if row.dsv2_ethnicity_other else 0

            gender_identities = json.loads(
                row.dsv2_gender_identities if row.dsv2_gender_identities
                                              and row.dsv2_gender_identities != 'null' else '[]')
            data['dsv2_gender_identity'] = [
                {
                    'dsv2_gender_identity': str(WorkbenchResearcherGenderIdentity(v)),
                    'dsv2_gender_identity_id': int(WorkbenchResearcherGenderIdentity(v))
                } for v in gender_identities
            ]

            data['dsv2_gender_other'] = 1 if row.dsv2_gender_other else 0
            data['dsv2_orientation_other'] = 1 if row.dsv2_orientation_other else 0

            data['dsv2_sex_at_birth'] = str(WorkbenchResearcherSexAtBirthV2(row.dsv2_sex_at_birth))
            data['dsv2_sex_at_birth_id'] = int(WorkbenchResearcherSexAtBirthV2(row.dsv2_sex_at_birth))

            data['dsv2_sex_at_birth_other'] = 1 if row.dsv2_sex_at_birth_other else 0

            sexual_orientation = json.loads(
                row.dsv2_sexual_orientations if row.dsv2_sexual_orientations
                                                and row.dsv2_sexual_orientations != 'null' else '[]')
            data['dsv2_sexual_orientation'] = [
                {
                    'dsv2_sexual_orientation': str(WorkbenchResearcherSexualOrientationV2(v)),
                    'dsv2_sexual_orientation_id': int(WorkbenchResearcherSexualOrientationV2(v))
                } for v in sexual_orientation
            ]

            data['dsv2_year_of_birth'] = row.dsv2_year_of_birth
            data['dsv2_year_of_birth_prefer_not'] = row.dsv2_year_of_birth_prefer_not

            data['dsv2_ethnicity_black_other'] = 1 if row.dsv2_ethnicity_black_other else 0
            data['dsv2_ethnicity_hispanic_other'] = 1 if row.dsv2_ethnicity_hispanic_other else 0
            data['dsv2_ethnicity_mena_other'] = 1 if row.dsv2_ethnicity_mena_other else 0
            data['dsv2_ethnicity_nhpi_other'] = 1 if row.dsv2_ethnicity_nhpi_other else 0
            data['dsv2_ethnicity_white_other'] = 1 if row.dsv2_ethnicity_white_other else 0
            data['dsv2_survey_comments'] = 1 if row.dsv2_survey_comments else 0

            return generators.ResourceRecordSet(schemas.WorkbenchResearcherSchema, data)


def res_researcher_batch_update(_ids):
    """
    Update a batch of ids.
    :param _ids: list of ids
    """

    gen = generators.WBResearcherGenerator()
    for _id in _ids:
        res = gen.make_resource(_id)
        res.save()
