import json
import logging
import string

from sqlalchemy.sql import text

from marshmallow import validate

from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.resource import fields

# TODO: Rework these from BigQuery schemas to resource schemas...

class _QuestionnaireSchema:
    """
    Helper for dynamically generating a Schema for a specific questionnaire
    """
    _module = ''
    _excluded_fields = ()
    _errors = list()

    def __init__(self, module_name, excluded_fields=None, *args, **kwargs):
        """
        :param module_name: Name of questionnaire module.
        :param excluded_fields: A list of excluded fields.
        """
        self._module = module_name
        if excluded_fields:
            self._excluded_fields = excluded_fields
        super().__init__(*args, **kwargs)

    def get_module_name(self):
        """ Return the questionnaire module name """
        return self._module

    @staticmethod
    def field_name_is_valid(name):
        """
        Check that the field name meets naming requirements.
        :param name: field name to check
        :return: True if valid otherwise False, error message.
        """
        # Check and make sure there are no other characters that are not allowed.
        # Fields must contain only letters, numbers, and underscores, start with a letter or underscore,
        # and be at most 128 characters long.
        allowed_chars = string.ascii_letters + string.digits + '_'
        if not all(c in allowed_chars for c in name):
            return False, f'Field {name} contains invalid characters, skipping.'
        if len(name) > 64:
            return False, f'Field {name} must be less than 64 characters, skipping.'
        if name[:1] not in string.ascii_letters and name[:1] not in string.digits and name[:1] != '_':
            return False, f'Field {name} must start with a character, digit or underscore, skipping.'
        return True, ''

    def module_fields(self):
        """
        Look up questionnaire concept to get fields.
        :return: dict of fields
        """
        # Standard fields that must be in every questionnaire schema.
        _schema = {
            'id': fields.Int64(required=True),
            'created': fields.DateTime(required=True),
            'modified': fields.DateTime(required=True),
            'authored': fields.DateTime(),
            'language': fields.String(validate=validate.Length(max=2)),
            'participant_id': fields.String(validate=validate.Length(max=10), required=True),
            'questionnaire_response_id': fields.Int32(required=True),
            'questionnaire_id': fields.Int32(required=True),
            'external_id': fields.String(validate=validate.Length(max=100)),
            'status': fields.String(validate=validate.Length(max=50))
        }

        dao = ResourceDataDao(backup=True)

        # DEPRECATED after RDR 1.85.2:  Load module field data from the code table if available, using stored proc
        # results = dao.call_proc('sp_get_code_module_items', args=[self._module])

        # This query replaces the sp_get_code_module_items stored procedure, which does not support the
        # DRC-managed codebooks where codes may be shared between modules. Columns are returned in the
        # same order as the stored procedure returned them (as a debug aid for comparing results).
        _question_codes_sql = """
             select c.code_id,
                    c.parent_id,
                    c.topic,
                    c.code_type,
                    c.value,
                    c.display,
                    c.system,
                    c.mapped,
                    c.created,
                    c.code_book_id,
                    c.short_value
             from code c
             inner join (
                 select distinct qq.code_id
                 from questionnaire_question qq where qq.questionnaire_id in (
                     select qc.questionnaire_id from questionnaire_concept qc
                             where qc.code_id = (
                                 select code_id from code c2 where c2.value = :module_id and system = :system
                             )
                 )
             ) qq2 on qq2.code_id = c.code_id
             where c.system = :system
             order by c.code_id;
         """
        with dao.session() as session:
            results = session.execute(_question_codes_sql, {'module_id': self._module, 'system': PPI_SYSTEM})

            if results:
                for row in results:
                    # Verify field name meets BigQuery requirements.
                    name = row['value']
                    is_valid, msg = self.field_name_is_valid(name)
                    if not is_valid:
                        self._errors.append(msg)
                        continue

                    _schema[name] = fields.Text()

            # This query makes better use of the indexes.
            _sql_term = text("""
                select convert(qh.resource using utf8) as resource
                    from questionnaire_history qh
                    where qh.questionnaire_id = (
                        select max(questionnaire_id) as questionnaire_id
                        from questionnaire_concept qc
                                 inner join code c on qc.code_id = c.code_id
                        where qc.code_id in (select c1.code_id from code c1 where c1.value = :mod and system = :system)
                    );
            """)

            result = session.execute(_sql_term, {'mod': self._module, 'system': PPI_SYSTEM}).first()
            if not result:
                return _schema

            qn_mod = json.loads(result[0])
            if 'resourceType' not in qn_mod or 'group' not in qn_mod:
                return _schema

            for qn in qn_mod['group']['question']:
                # To support
                #   1) The user supplied answer,
                #   2) question skipped or
                #   3) user was not given this question.
                # We have to store all question responses as Strings in BigQuery.
                if not qn['concept'][0].get('code', None):
                    continue

                name = qn['concept'][0]['code']
                if name in self._excluded_fields:
                    continue

                # Verify field name meets BigQuery requirements.
                is_valid, msg = self.field_name_is_valid(name)
                if not is_valid:
                    logging.warning(msg)
                    continue

                # flag duplicate fields.
                found = False
                for fld in _schema:
                    if fld['name'].lower() == name.lower():
                        found = True
                        break

                if not found:
                    _schema[name] = fields.Text()

            # There seems to be duplicate column definitions we need to remove in some of the modules.
            # tmpflds = [i for n, i in enumerate(fields) if i not in fields[n + 1:]]
            # return tmpflds
            return _schema


#
# ConsentPII
#
class BQPDRConsentPIISchema(_QuestionnaireSchema):
    """ ConsentPII Module """
    _module = 'ConsentPII'
    _excluded_fields = (
        'ConsentPII_PIIName',
        'PIIName_First',
        'PIIName_Middle',
        'PIIName_Last',
        'ConsentPII_PIIAddress',
        'PIIAddress_StreetAddress',
        'PIIAddress_StreetAddress2',
        'StreetAddress_PIICity',
        'PIIContactInformation_Phone',
        'ConsentPII_EmailAddress',
        'EHRConsentPII_Signature',
        'ExtraConsent_CABoRSignature',
        'ExtraConsent_Signature',
        'ConsentPII_HelpWithConsentSignature',
        'PIIContactInformation_VerifiedPrimaryPhoneNumber',
        'PIIContactInformation_Email',
        'PIIBirthInformation_BirthDate',
        'ConsentPII_VerifiedPrimaryPhoneNumber'
    )


#
# TheBasics
#
class TheBasicsSchema(_QuestionnaireSchema):
    """ TheBasics Module """
    _module = 'TheBasics'
    _excluded_fields = (
        'TheBasics_CountryBornTextBox',
        'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox',
        'WhatTribeAffiliation_FreeText',
        'AIANNoneOfTheseDescribeMe_AIANFreeText',
        'NoneOfTheseDescribeMe_AsianFreeText',
        'BlackNoneOfTheseDescribeMe_BlackFreeText',
        'MENANoneOfTheseDescribeMe_MENAFreeText',
        'NHPINoneOfTheseDescribeMe_NHPIFreeText',
        'WhiteNoneOfTheseDescribeMe_WhiteFreeText',
        'HispanicNoneOfTheseDescribeMe_HispanicFreeText',
        'SpecifiedGender_SpecifiedGenderTextBox',
        'SomethingElse_SexualitySomethingElseTextBox',
        'SexAtBirthNoneOfThese_SexAtBirthTextBox',
        'LivingSituation_LivingSituationFreeText',
        'SocialSecurity_SocialSecurityNumber',
        'SecondaryContactInfo_FirstContactsInfo',
        'SecondaryContactInfo_PersonOneFirstName',
        'SecondaryContactInfo_PersonOneMiddleInitial',
        'SecondaryContactInfo_PersonOneLastName',
        'SecondaryContactInfo_PersonOneAddressOne',
        'SecondaryContactInfo_PersonOneAddressTwo',
        'SecondaryContactInfo_PersonOneEmail',
        'SecondaryContactInfo_PersonOneTelephone',
        'PersonOneAddress_PersonOneAddressCity',
        'SecondaryContactInfo_SecondContactsFirstName',
        'SecondaryContactInfo_SecondContactsMiddleInitial',
        'SecondaryContactInfo_SecondContactsLastName',
        'SecondaryContactInfo_SecondContactsAddressOne',
        'SecondaryContactInfo_SecondContactsAddressTwo',
        'SecondContactsAddress_SecondContactCity',
        'SecondaryContactInfo_SecondContactsEmail',
        'SecondaryContactInfo_SecondContactsNumber',
        'EmploymentWorkAddress_AddressLineOne',
        'EmploymentWorkAddress_AddressLineTwo',
        'EmploymentWorkAddress_City',
        'EmploymentWorkAddress_Country',
        'PersonOneAddress_PersonOneAddressZipCode',
        'SecondContactsAddress_SecondContactZipCode',
        'PersonOneAddress_PersonOneAddressZipCode',
        'SecondContactsAddress_SecondContactZipCode',
        'OtherHealthPlan_FreeText'
    )


#
# Lifestyle
#
class BQPDRLifestyleSchema(_QuestionnaireSchema):
    """ Lifestyle Module """
    _module = 'Lifestyle'
    _excluded_fields = (
        'OtherSpecify_OtherDrugsTextBox'
    )


#
# OverallHealthSchema
#
class BQPDROverallHealthSchema(_QuestionnaireSchema):
    """ OverallHealth Module """
    _module = 'OverallHealth'
    _excluded_fields = (
        'OrganTransplantDescription_OtherOrgan',
        'OrganTransplantDescription_OtherTissue',
        'OutsideTravel6Month_OutsideTravel6MonthWhereTraveled',
    )


#
# EHRConsentPII
#
class BQPDREHRConsentPIISchema(_QuestionnaireSchema):
    """ EHRConsentPII Module """
    _module = 'EHRConsentPII'
    _excluded_fields = (
        'EHRConsentPII_Signature',
        'EHRConsentPII_ILHIPPAWitnessSignature',
        'EHRConsentPII_HelpWithConsentSignature',
        '12MoEHRConsentPII_EmailCopy',
        '30MoEHRConsentPII_EmailCopy'
    )


#
# DVEHRSharing
#
class BQPDRDVEHRSharingSchema(_QuestionnaireSchema):
    """ DVEHRSharing Module """
    _module = 'DVEHRSharing'
    _excluded_fields = (
        'EHRConsentPII_Signature',
    )


#
# FamilyHistory
#
class BQPDRFamilyHistorySchema(_QuestionnaireSchema):
    """ FamilyHistory Module """
    _module = 'FamilyHistory'
    _excluded_fields = (
        'DaughterDiagnosisHistory_WhichConditions',
        'OtherCancer_DaughterFreeTextBox',
        'OtherCancer_SonFreeTextBox',
        'OtherCondition_DaughterFreeTextBox',
        'OtherCondition_SonFreeTextBox',
        'SonDiagnosisHistory_WhichConditions',
        'OtherCancer_GrandparentFreeTextBox',
        'OtherCondition_GrandparentFreeTextBox',
        'FatherDiagnosisHistory_WhichConditions',
        'MotherDiagnosisHistory_WhichConditions',
        'OtherCancer_FatherFreeTextBox',
        'OtherCancer_MotherFreeTextBox',
        'OtherCondition_FatherFreeTextBox',
        'OtherCondition_MotherFreeTextBox',
        'OtherCancer_SiblingFreeTextBox',
        'OtherCondition_SiblingFreeTextBox',
        'SiblingDiagnosisHistory_WhichConditions',
    )


#
# HealthcareAccess
#
class BQPDRHealthcareAccessSchema(_QuestionnaireSchema):
    """ HealthcareAccess Module """
    _module = 'HealthcareAccess'
    _excluded_fields = (
        'OtherDelayedMedicalCare_FreeText',
        'OtherInsuranceType_FreeText',
    )

#
# PersonalMedicalHistory
#
class BQPDRPersonalMedicalHistorySchema(_QuestionnaireSchema):
    """ PersonalMedicalHistory Module """
    _module = 'PersonalMedicalHistory'
    _excluded_fields = (
        'OtherHeartorBloodCondition_FreeTextBox',
        'OtherRespiratory_FreeTextBox',
        'OtherCancer_FreeTextBox',
        'OtherDigestiveCondition_FreeTextBox',
        'OtherDiabetes_FreeTextBox',
        'OtherHormoneEndocrine_FreeTextBox',
        'OtherThyroid_FreeTextBox',
        'OtherKidneyCondition_FreeTextBox',
        'OtherBoneJointMuscle_FreeTextBox',
        'OtherArthritis_FreeTextBox',
        'OtherHearingEye_FreeTextBox',
        'OtherInfectiousDisease_FreeTextBox',
        'OtherBrainNervousSystem_FreeTextBox',
        'OtherMentalHealthSubstanceUse_FreeTextBox',
        'OtherDiagnosis_FreeTextBox',
    )


#
# COPE May Survey
#
class BQPDRCOPEMaySchema(_QuestionnaireSchema):
    """ COPE Module """
    _module = 'COPE'
    _excluded_fields = ()

#
#  COPE Nov Survey
#
class BQPDRCOPENovSchema(_QuestionnaireSchema):
    """ COPE Module """
    _module = 'cope_nov'
    _excluded_fields = ()

#
#  COPE Dec Survey
#
class BQPDRCOPEDecSchema(_QuestionnaireSchema):
    """ COPE Module """
    _module = 'cope_dec'
    _excluded_fields = ()

#
#  COPE Feb Survey
#
class BQPDRCOPEFebSchema(_QuestionnaireSchema):
    """ COPE Module """
    _module = 'cope_feb'
    _excluded_fields = ()
