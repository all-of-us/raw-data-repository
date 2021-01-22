import json
import logging
import string

from sqlalchemy.sql import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQFieldModeEnum, BQFieldTypeEnum
from rdr_service.code_constants import PPI_SYSTEM

class _BQModuleSchema(BQSchema):
    """
    Helper for dynamically generating a BQSchema for a specific questionnaire
    """
    _module = ''
    _excluded_fields = ()

    def __init__(self):
        """ add the field list to our self. """
        fields = self.get_fields()
        for field in fields:
            self.__dict__[field['name']] = field

    def get_module_name(self):
        """ Return the questionnaire module name """
        return self._module

    @staticmethod
    def field_name_is_valid(name):
        """
        Check that the field name meets BigQuery naming requirements.
        :param name: field name to check
        :return: True if valid otherwise False, error message.
        """
        # Check and make sure there are no other characters that are not allowed.
        # Fields must contain only letters, numbers, and underscores, start with a letter or underscore,
        # and be at most 128 characters long.
        allowed_chars = string.ascii_letters + string.digits + '_'
        if not all(c in allowed_chars for c in name):
            message = f'Field {name} contains invalid characters, skipping.'

            # Skip codes that are already known to be problematic
            # TODO: alter the codes names instead of leaving them out (possibly something like
            #  "DV Consent Decision" to "DV_Consent_Decision"
            if name == 'DV Consent Decision':
                message = None

            return False, message
        if len(name) > 128:
            return False, f'Field {name} must be less than 128 characters, skipping.'
        if name[:1] not in string.ascii_letters and name[:1] != '_':
            # Disabled message, too many log entries.
            # return False, f'Field {name} must start with a character or underscore, skipping.'
            return False, None
        return True, ''

    def get_fields(self):
        """
        Look up questionnaire concept to get fields.
        :return: list of fields
        """
        fields = list()
        self._fields = list()
        # Standard fields that must be in every BigQuery table.
        fields.append({'name': 'id', 'type': BQFieldTypeEnum.INTEGER.name, 'mode': BQFieldModeEnum.REQUIRED.name})
        fields.append({'name': 'created', 'type': BQFieldTypeEnum.DATETIME.name,
                       'mode': BQFieldModeEnum.REQUIRED.name})
        fields.append(
            {'name': 'modified', 'type': BQFieldTypeEnum.DATETIME.name, 'mode': BQFieldModeEnum.REQUIRED.name})
        # Fields which apply to all module responses
        fields.append(
            {'name': 'authored', 'type': BQFieldTypeEnum.DATETIME.name, 'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'language', 'type': BQFieldTypeEnum.STRING.name, 'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'participant_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.REQUIRED.name})
        fields.append({'name': 'questionnaire_response_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.REQUIRED.name}),
        fields.append({'name': 'questionnaire_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.NULLABLE.name}),
        fields.append({'name': 'external_id', 'type': BQFieldTypeEnum.STRING.name,
                       'mode': BQFieldModeEnum.NULLABLE.name})


        dao = BigQuerySyncDao(backup=True)

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
            order by c.code_id
        """
        with dao.session() as session:
            results = session.execute(_question_codes_sql, {'module_id': self._module, 'system': PPI_SYSTEM})

            if results:
                for row in results:
                    # Verify field name meets BigQuery requirements.
                    name = row['value']
                    is_valid, msg = self.field_name_is_valid(name)
                    if not is_valid:
                        if msg:
                            logging.warning(msg)
                        continue

                    if name in self._excluded_fields:
                        continue

                    field = dict()
                    field['name'] = name
                    field['type'] = BQFieldTypeEnum.STRING.name
                    field['mode'] = BQFieldModeEnum.NULLABLE.name
                    field['enum'] = None
                    fields.append(field)

            # This query makes better use of the indexes.
            _sql_term = text("""
                select convert(qh.resource using utf8) as resource
                    from questionnaire_history qh
                    where qh.questionnaire_id = (
                        select max(questionnaire_id) as questionnaire_id
                        from questionnaire_concept qc
                                 inner join code c on qc.code_id = c.code_id
                        where qc.code_id in (
                            select c1.code_id from code c1 where c1.value = :mod and c1.system = :system
                        )
                    );
            """)

            result = session.execute(_sql_term, {'mod': self._module, 'system': PPI_SYSTEM}).first()
            if not result:
                return fields

            qn_mod = json.loads(result[0])
            if 'resourceType' not in qn_mod or 'group' not in qn_mod:
                return fields

            for qn in qn_mod['group']['question']:
                # To support 1) The user supplied answer,
                # 2) question skipped or
                # 3) user was not given this question. We
                # have to store all question responses as Strings in BigQuery.
                field = qn['concept'][0].get('code', None)
                if not field:
                    continue

                name = qn['concept'][0]['code']
                if name in self._excluded_fields:
                    continue

                # Verify field name meets BigQuery requirements.
                is_valid, msg = self.field_name_is_valid(name)
                if not is_valid:
                    if msg:
                        logging.warning(msg)
                    continue

                # flag duplicate fields.
                found = False
                for fld in fields:
                    if fld['name'] == name:
                        found = True
                        break

                if not found:
                    field = dict()
                    field['name'] = name
                    field['type'] = BQFieldTypeEnum.STRING.name
                    field['mode'] = BQFieldModeEnum.NULLABLE.name
                    field['enum'] = None
                    fields.append(field)

            # There seems to be duplicate column definitions we need to remove in some of the modules.
            # tmpflds = [i for n, i in enumerate(fields) if i not in fields[n + 1:]]
            # return tmpflds
            return fields


#
# ConsentPII
#
class BQPDRConsentPIISchema(_BQModuleSchema):
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


class BQPDRConsentPII(BQTable):
    """ PDR ConsentPII BigQuery Table """
    __tablename__ = 'pdr_mod_consentpii'
    __schema__ = BQPDRConsentPIISchema


class BQPDRConsentPIIView(BQView):
    """ PDR ConsentPII BigQuery View """
    __viewname__ = 'v_pdr_mod_consentpii'
    __viewdescr__ = 'PDR ConsentPII Module View'
    __table__ = BQPDRConsentPII
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# TheBasics
#
class BQPDRTheBasicsSchema(_BQModuleSchema):
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


class BQPDRTheBasics(BQTable):
    """ TheBasics BigQuery Table """
    __tablename__ = 'pdr_mod_thebasics'
    __schema__ = BQPDRTheBasicsSchema


class BQPDRTheBasicsView(BQView):
    """ PDR TheBasics BiqQuery View """
    __viewname__ = 'v_pdr_mod_thebasics'
    __viewdescr__ = 'PDR TheBasics Module View'
    __table__ = BQPDRTheBasics
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# Lifestyle
#
class BQPDRLifestyleSchema(_BQModuleSchema):
    """ Lifestyle Module """
    _module = 'Lifestyle'
    _excluded_fields = (
        'OtherSpecify_OtherDrugsTextBox'
    )


class BQPDRLifestyle(BQTable):
    """ Lifestyle BigQuery Table """
    __tablename__ = 'pdr_mod_lifestyle'
    __schema__ = BQPDRLifestyleSchema


class BQPDRLifestyleView(BQView):
    """ PDR TheBasics BiqQuery View """
    __viewname__ = 'v_pdr_mod_lifestyle'
    __viewdescr__ = 'PDR Lifestyle Module View'
    __table__ = BQPDRLifestyle
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# OverallHealthSchema
#
class BQPDROverallHealthSchema(_BQModuleSchema):
    """ OverallHealth Module """
    _module = 'OverallHealth'
    _excluded_fields = (
        'OrganTransplantDescription_OtherOrgan',
        'OrganTransplantDescription_OtherTissue',
        'OutsideTravel6Month_OutsideTravel6MonthWhereTraveled',
        'OtherOrgan_FreeTextBox',
        'OtherTissue_FreeTextBox'
    )


class BQPDROverallHealth(BQTable):
    """ OverallHealth BigQuery Table """
    __tablename__ = 'pdr_mod_overallhealth'
    __schema__ = BQPDROverallHealthSchema


class BQPDROverallHealthView(BQView):
    """ PDR OverallHealth BiqQuery View """
    __viewname__ = 'v_pdr_mod_overallhealth'
    __viewdescr__ = 'PDR OverallHealth Module View'
    __table__ = BQPDROverallHealth
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# EHRConsentPII
#
class BQPDREHRConsentPIISchema(_BQModuleSchema):
    """ EHRConsentPII Module """
    _module = 'EHRConsentPII'
    _excluded_fields = (
        'EHRConsentPII_Signature',
        'EHRConsentPII_ILHIPPAWitnessSignature',
        'EHRConsentPII_HelpWithConsentSignature',
        '12MoEHRConsentPII_EmailCopy',
        '30MoEHRConsentPII_EmailCopy'
    )


class BQPDREHRConsentPII(BQTable):
    """ EHRConsentPII BigQuery Table """
    __tablename__ = 'pdr_mod_ehrconsentpii'
    __schema__ = BQPDREHRConsentPIISchema


class BQPDREHRConsentPIIView(BQView):
    """ PDR EHRConsentPII BiqQuery View """
    __viewname__ = 'v_pdr_mod_ehrconsentpii'
    __viewdescr__ = 'PDR EHRConsentPII Module View'
    __table__ = BQPDREHRConsentPII
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# DVEHRSharing
#
class BQPDRDVEHRSharingSchema(_BQModuleSchema):
    """ DVEHRSharing Module """
    _module = 'DVEHRSharing'
    _excluded_fields = (
        'EHRConsentPII_Signature',
    )


class BQPDRDVEHRSharing(BQTable):
    """ DVEHRSharing BigQuery Table """
    __tablename__ = 'pdr_mod_dvehrsharing'
    __schema__ = BQPDRDVEHRSharingSchema


class BQPDRDVEHRSharingView(BQView):
    """ PDR DVEHRSharing BiqQuery View """
    __viewname__ = 'v_pdr_mod_dvehrsharing'
    __viewdescr__ = 'PDR DVEHRSharing Module View'
    __table__ = BQPDRDVEHRSharing
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# FamilyHistory
#
class BQPDRFamilyHistorySchema(_BQModuleSchema):
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


class BQPDRFamilyHistory(BQTable):
    """ FamilyHistory BigQuery Table """
    __tablename__ = 'pdr_mod_familyhistory'
    __schema__ = BQPDRFamilyHistorySchema


class BQPDRFamilyHistoryView(BQView):
    """ PDR FamilyHistory BiqQuery View """
    __viewname__ = 'v_pdr_mod_familyhistory'
    __viewdescr__ = 'PDR FamilyHistory Module View'
    __table__ = BQPDRFamilyHistory
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# HealthcareAccess
#
class BQPDRHealthcareAccessSchema(_BQModuleSchema):
    """ HealthcareAccess Module """
    _module = 'HealthcareAccess'
    _excluded_fields = (
        'OtherDelayedMedicalCare_FreeText',
        'OtherInsuranceType_FreeText',
    )


class BQPDRHealthcareAccess(BQTable):
    """ HealthcareAccess BigQuery Table """
    __tablename__ = 'pdr_mod_healthcareaccess'
    __schema__ = BQPDRHealthcareAccessSchema


class BQPDRHealthcareAccessView(BQView):
    """ PDR HealthcareAccess BiqQuery View """
    __viewname__ = 'v_pdr_mod_healthcareaccess'
    __viewdescr__ = 'PDR HealthcareAccess Module View'
    __table__ = BQPDRHealthcareAccess
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# PersonalMedicalHistory
#
class BQPDRPersonalMedicalHistorySchema(_BQModuleSchema):
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


class BQPDRPersonalMedicalHistory(BQTable):
    """ PersonalMedicalHistory BigQuery Table """
    __tablename__ = 'pdr_mod_personalmedicalhistory'
    __schema__ = BQPDRPersonalMedicalHistorySchema


class BQPDRPersonalMedicalHistoryView(BQView):
    """ PDR PersonalMedicalHistory BiqQuery View """
    __viewname__ = 'v_pdr_mod_personalmedicalhistory'
    __viewdescr__ = 'PDR PersonalMedicalHistory Module View'
    __table__ = BQPDRPersonalMedicalHistory
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# COPE May-July Survey
#
class BQPDRCOPEMaySchema(_BQModuleSchema):
    """ COPE May-July Module """
    _module = 'COPE'
    _excluded_fields = ()


class BQPDRCOPEMay(BQTable):
    """ COPE May-July BigQuery Table """
    __tablename__ = 'pdr_mod_cope_may'
    __schema__ = BQPDRCOPEMaySchema


class BQPDRCOPEMayView(BQView):
    """ PDR COPE May-July BiqQuery View """
    __viewname__ = 'v_pdr_mod_cope_may'
    __viewdescr__ = 'PDR COPE May Module View'
    __table__ = BQPDRCOPEMay
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# COPE Nov Survey
#
class BQPDRCOPENovSchema(_BQModuleSchema):
    """ COPE Nov Module """
    _module = 'cope_nov'  # Lowercase on purpose.
    _excluded_fields = ()


class BQPDRCOPENov(BQTable):
    """ COPE Nov BigQuery Table """
    __tablename__ = 'pdr_mod_cope_nov'
    __schema__ = BQPDRCOPENovSchema


class BQPDRCOPENovView(BQView):
    """ PDR COPE Nov BiqQuery View """
    __viewname__ = 'v_pdr_mod_cope_nov'
    __viewdescr__ = 'PDR COPE Nov Module View'
    __table__ = BQPDRCOPENov
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# COPE Dec Survey
#
class BQPDRCOPEDecSchema(_BQModuleSchema):
    """ COPE Dec Module """
    _module = 'cope_dec'  # Lowercase on purpose.
    _excluded_fields = ()


class BQPDRCOPEDec(BQTable):
    """ COPE Dec BigQuery Table """
    __tablename__ = 'pdr_mod_cope_dec'
    __schema__ = BQPDRCOPEDecSchema


class BQPDRCOPEDecView(BQView):
    """ PDR COPE Dec BiqQuery View """
    __viewname__ = 'v_pdr_mod_cope_dec'
    __viewdescr__ = 'PDR COPE Dec Module View'
    __table__ = BQPDRCOPEDec
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# COPE Feb Survey
#
class BQPDRCOPEFebSchema(_BQModuleSchema):
    """ COPE Feb Module """
    _module = 'cope_feb'  # Lowercase on purpose.
    _excluded_fields = ()


class BQPDRCOPEFeb(BQTable):
    """ COPE Feb BigQuery Table """
    __tablename__ = 'pdr_mod_cope_feb'
    __schema__ = BQPDRCOPEFebSchema


class BQPDRCOPEFebView(BQView):
    """ PDR COPE Feb BiqQuery View """
    __viewname__ = 'v_pdr_mod_cope_feb'
    __viewdescr__ = 'PDR COPE Feb Module View'
    __table__ = BQPDRCOPEFeb
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True
