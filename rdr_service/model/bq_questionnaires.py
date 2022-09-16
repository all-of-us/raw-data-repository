import json
import logging

from sqlalchemy.sql import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQFieldModeEnum, BQFieldTypeEnum
from rdr_service.code_constants import PPI_SYSTEM

#   NOTE:  IF NEW MODULE CLASSES ARE ADDED TO THIS FILE, ADD THE NEW TABLE CLASS TO "PDR_MODULE_LIST"
#          AT THE BOTTOM OF THIS FILE. BELOW IS A LIST FILE THAT USE THESE CLASSES.
#   rdr_service/resource/tasks.py
#   rdr_service/tools/tool_libs/resource_tool.py
#   rdr_service/model/__init__.py
#
class _BQModuleSchema(BQSchema):
    """
    Helper for dynamically generating a BQSchema for a specific questionnaire
    """
    _module = ''
    _force_boolean_fields = ()
    _excluded_fields = ()

    def __init__(self):
        """ add the field list to our self. """
        fields = self.get_fields()
        for field in fields:
            self.__dict__[field['name']] = field

    def get_module_name(self):
        """ Return the questionnaire module name """
        return self._module


    def get_fields(self):
        """
        Look up questionnaire concept to get fields.
        :return: list of fields
        """
        fields = list()
        self._fields = list()
        # Standard fields that must be in every BigQuery pdr_mod_* table.
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
                       'mode': BQFieldModeEnum.REQUIRED.name})
        fields.append({'name': 'questionnaire_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'external_id', 'type': BQFieldTypeEnum.STRING.name,
                       'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'status', 'type': BQFieldTypeEnum.STRING.name,
                       'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'status_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.NULLABLE.name})

        dao = BigQuerySyncDao(backup=True)

        # DEPRECATED after RDR 1.85.2:  Load module field data from the code table if available, using stored proc
        # results = dao.call_proc('sp_get_code_module_items', args=[self._module])

        # Set up to return fields that match what the deprecated sp_get_code_module_items stored proc returned
        # This part of the raw SQL statement is the same regardless of which query logic it is concatenated with
        select_clause_sql = """
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
        """
        # This query logic was used before DA-1884, and compiled known question codes from QuestionnaireResponse
        # payloads previously posted to RDR.  (This sometimes resulted in gaps where question codes that had not yet
        # been included in the received responses were not included in the get_fields() results )
        _existing_question_codes_sql = select_clause_sql + """

            from code c
            inner join (
                select distinct qq.code_id
                from questionnaire_question qq where qq.questionnaire_id in (
                    select qc.questionnaire_id from questionnaire_concept qc
                            where qc.code_id = (
                                select code_id from code c2 where c2.value = :module_id and c2.system = :system
                            )
                )
            ) qq2 on qq2.code_id = c.code_id
            where c.system = :system
            order by c.code_id
        """

        # With DA-1844, question codes can be determined from codebook survey data imported from REDCap
        # (or backfilled via python tool, for surveys that existed before codebook management shifted to REDCap)
        _survey_question_codes_sql = select_clause_sql + """

            from survey_question sq
            inner join code c on c.code_id = sq.code_id
            inner join survey s on s.id = sq.survey_id
            inner join code mc on mc.code_id = s.code_id
            where s.code_id = (
                 select code_id
                 from code ct
                 where ct.value = :module_id and ct.system = :system
            )
            order by c.code_id
        """

        with dao.session() as session:
            # A set of lowercase field strings to skip if we encounter them more than once when generating the field
            # list via multiple stages.   Also contains any modified field names generated by make_bq_field_name
            # Initialize to the list of excluded fields defined for this module class
            skip_fieldnames_lower = set([x.lower() for x in self._excluded_fields])
            force_boolean_lower = set([x.lower() for x in self._force_boolean_fields])
            for sql in [_survey_question_codes_sql, _existing_question_codes_sql]:
                results = session.execute(sql, {'module_id': self._module, 'system': PPI_SYSTEM})
                if results:
                    for row in results:
                        field_name = row['value']
                        if field_name.lower() in skip_fieldnames_lower:
                            continue

                        bq_field_name, msg = self.make_bq_field_name(field_name, row['short_value'])
                        if not bq_field_name:
                            if msg:
                                logging.warning(msg)
                            continue

                        if bq_field_name.lower() not in skip_fieldnames_lower:
                            # If field is in force boolean list, set to integer.
                            fld_type = BQFieldTypeEnum.INTEGER.name if bq_field_name.lower() in force_boolean_lower \
                                        else BQFieldTypeEnum.STRING.name
                            field = dict()
                            field['name'] = bq_field_name
                            field['type'] = fld_type
                            field['mode'] = BQFieldModeEnum.NULLABLE.name
                            field['enum'] = None
                            fields.append(field)
                            skip_fieldnames_lower.add(bq_field_name.lower())
                        else:
                            logging.warning(
                                f'Code "{field_name}" resulted in duplicate BQ field name "{bq_field_name}".'
                            )

                        skip_fieldnames_lower.add(field_name.lower())

            # This query makes better use of the indexes.  Intention is to check the most recent POST Questionnaire
            # payload for any codes not already found above because the RDR code table was potentially behind on
            # having the latest codebook imported?
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
                if name.lower() in skip_fieldnames_lower:
                    continue

                bq_field_name, msg = self.make_bq_field_name(name)
                if not bq_field_name:
                    if msg:
                        logging.warning(msg)
                    continue

                if bq_field_name.lower() not in skip_fieldnames_lower:
                    field = dict()
                    field['name'] = bq_field_name
                    field['type'] = BQFieldTypeEnum.STRING.name
                    field['mode'] = BQFieldModeEnum.NULLABLE.name
                    field['enum'] = None
                    fields.append(field)
                    skip_fieldnames_lower.add(bq_field_name.lower())
                else:
                    logging.warning(f'Code "{field_name}" resulted in duplicate BQ field name "{bq_field_name}".')

                skip_fieldnames_lower.add(name.lower())

            return fields


class BQModuleView(BQView):

    def __init__(self):
        # Note: Do not call base class init method.

        if not hasattr(self, '__table__'):
            raise ValueError('Class must have "__table__" properties defined.')

        if not self.__sql__:
            tbl = self.__table__()
            pk = ', '.join(self.__pk_id__) if isinstance(self.__pk_id__, list) else str(self.__pk_id__)
            fields = tbl.get_schema().get_fields()
            # Insert 'test_participant' select after 'participant_id' field.
            fields.insert(6, {'name': """(SELECT test_participant FROM `{project}`.{dataset}.pdr_participant p
                    WHERE p.participant_id = participant_id
                    ORDER BY modified desc LIMIT 1) AS test_participant""", 'type': 'INTEGER', 'mode': 'NULLABLE'})
            self.__sql__ = """
                SELECT {fields}
              """.format(fields=', '.join([f['name'] for f in fields]))

            self.__sql__ += """
                FROM (
                  SELECT *,
                      ROW_NUMBER() OVER (PARTITION BY %%pk_id%% ORDER BY modified desc) AS rn
                    FROM `{project}`.{dataset}.%%table%%
                ) t
                WHERE t.rn = 1
              """.replace('%%table%%', tbl.get_name()).replace('%%pk_id%%', pk)

#
# ConsentPII
#
class BQPDRConsentPIISchema(_BQModuleSchema):
    """ ConsentPII Module """
    _module = 'ConsentPII'
    _force_boolean_fields = (
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


class BQPDRConsentPIIView(BQModuleView):
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
    _force_boolean_fields = (
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
        'OtherHealthPlan_FreeText',
        'AIAN_Tribe'
    )


class BQPDRTheBasics(BQTable):
    """ TheBasics BigQuery Table """
    __tablename__ = 'pdr_mod_thebasics'
    __schema__ = BQPDRTheBasicsSchema


class BQPDRTheBasicsView(BQModuleView):
    """ PDR TheBasics BiqQuery View """
    __viewname__ = 'v_pdr_mod_thebasics'
    __viewdescr__ = 'PDR TheBasics Module View'
    __table__ = BQPDRTheBasics
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# Lifestyle
#
# Note:  Must add the comma after a single element in the _excluded_fields list so it is still treated like a
# list of strings in a comprehension (vs. a list of chars from the single string)
class BQPDRLifestyleSchema(_BQModuleSchema):
    """ Lifestyle Module """
    _module = 'Lifestyle'
    _force_boolean_fields = (
        'OtherSpecify_OtherDrugsTextBox',
    )


class BQPDRLifestyle(BQTable):
    """ Lifestyle BigQuery Table """
    __tablename__ = 'pdr_mod_lifestyle'
    __schema__ = BQPDRLifestyleSchema


class BQPDRLifestyleView(BQModuleView):
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
    _force_boolean_fields = (
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


class BQPDROverallHealthView(BQModuleView):
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
    _force_boolean_fields = (
        'EHRConsentPII_Signature',
        'EHRConsentPII_ILHIPPAWitnessSignature',
        'EHRConsentPII_HelpWithConsentSignature',
        '12MoEHRConsentPII_EmailCopy',
        '30MoEHRConsentPII_EmailCopy',
        'sensitivetype2_mentalhealth',
        'sensitivetype2_hivaids',
        'sensitivetype2_substanceuse',
        'sensitivetype2_genetictesting',
        'sensitivetype2_domesticviolence',
        'signature_type',
        'signature_draw'
    )


class BQPDREHRConsentPII(BQTable):
    """ EHRConsentPII BigQuery Table """
    __tablename__ = 'pdr_mod_ehrconsentpii'
    __schema__ = BQPDREHRConsentPIISchema


class BQPDREHRConsentPIIView(BQModuleView):
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
    _force_boolean_fields = (
        'EHRConsentPII_Signature',
    )


class BQPDRDVEHRSharing(BQTable):
    """ DVEHRSharing BigQuery Table """
    __tablename__ = 'pdr_mod_dvehrsharing'
    __schema__ = BQPDRDVEHRSharingSchema


class BQPDRDVEHRSharingView(BQModuleView):
    """ PDR DVEHRSharing BiqQuery View """
    __viewname__ = 'v_pdr_mod_dvehrsharing'
    __viewdescr__ = 'PDR DVEHRSharing Module View'
    __table__ = BQPDRDVEHRSharing
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True

#
#  GROR
#
class BQPDRGRORSchema(_BQModuleSchema):
    """ GROR Consent Module """
    _module = 'GROR'
    # Note:  These code values exist only in stable, and appear to have been deprecated including for similarly named
    # codes (e.g., HelpWithConsent_Name became HelpMeWithConsent_Name).  Exclude the deprecated fields for consistency
    # with stable and prod BQ schemas.
    _excluded_fields = (
        'ThinkItThrough',
        'GROR_ResultsConsent',              # This is a topic code, not a question code
        'ResultsConsent_HelpWithConsent',   # Only have ResultsConsent_HelpMeWithConsent in prod environment
        'HelpWithConsent_Name'              # Only have HelpMeWithConsent_Name in prod environment
    )
    _force_boolean_fields = (
        'ResultsConsent_Signature',
        'HelpMeWithConsent_Name',
        'other_concerns',
        'other_reasons'
    )

class BQPDRGROR(BQTable):
    """ GROR BigQuery Table """
    __tablename__ = 'pdr_mod_gror'
    __schema__ = BQPDRGRORSchema

class BQPDRGRORView(BQView):
    """ PDR GROR BiqQuery View """
    __viewname__ = 'v_pdr_mod_gror'
    __viewdescr__ = 'PDR GROR Consent Module View'
    __table__ = BQPDRGROR
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True

#
# FamilyHistory
#
class BQPDRFamilyHistorySchema(_BQModuleSchema):
    """ FamilyHistory Module """
    _module = 'FamilyHistory'
    _force_boolean_fields = (
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


class BQPDRFamilyHistoryView(BQModuleView):
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
    _force_boolean_fields = (
        'OtherDelayedMedicalCare_FreeText',
        'OtherInsuranceType_FreeText',
    )


class BQPDRHealthcareAccess(BQTable):
    """ HealthcareAccess BigQuery Table """
    __tablename__ = 'pdr_mod_healthcareaccess'
    __schema__ = BQPDRHealthcareAccessSchema


class BQPDRHealthcareAccessView(BQModuleView):
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
    _force_boolean_fields = (
        'OtherHeartorBloodCondition_FreeTextBox',
        'OtherHeartandBloodCondition_FreeTextBox',
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


class BQPDRPersonalMedicalHistoryView(BQModuleView):
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
    _force_boolean_fields = (
        'cdc_covid_19_7_xx22_date',
        'cope_a_126',
        'basics_xx',
        'basics_xx20',
        'cu_covid_cope_a_204',
        'basics_11a_cope_a_33',
        'ipaq_1_cope_a_24',
        'ipaq_2_cope_a_160',
        'ipaq_2_cope_a_85',
        'ipaq_3_cope_a_24',
        'ipaq_4_cope_a_160',
        'ipaq_4_cope_a_85',
        'ipaq_5_cope_a_24',
        'ipaq_6_cope_a_160',
        'ipaq_6_cope_a_85',
        'cope_a_160',
        'cope_a_85',
        'copect_50_xx19_cope_a_57',
        'copect_50_xx19_cope_a_198',
        'copect_50_xx19_cope_a_152',
        'lifestyle_2_xx12_cope_a_57',
        'lifestyle_2_xx12_cope_a_198',
        'lifestyle_2_xx12_cope_a_152',
        'tsu_ds5_13_xx42_cope_a_226',
        'eds_follow_up_1_xx'
    )


class BQPDRCOPEMay(BQTable):
    """ COPE May-July BigQuery Table """
    __tablename__ = 'pdr_mod_cope_may'
    __schema__ = BQPDRCOPEMaySchema


class BQPDRCOPEMayView(BQModuleView):
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
    _force_boolean_fields = (
        'msds_17_c',
        'cdc_covid_19_7_xx22_date',
        'cdc_covid_19_7_xx23_other_cope_a_204',
        'cope_a_126',
        'nhs_covid_fhc17b_cope_a_226',
        'cdc_covid_19_n_a2',
        'cdc_covid_19_n_a4',
        'cdc_covid_19_n_a8',
        'dmfs_29a',
        'basics_xx',
        'basics_xx20',
        'cu_covid_cope_a_204',
        'cope_aou_xx_2_a',
        'basics_11a_cope_a_33'
    )

class BQPDRCOPENov(BQTable):
    """ COPE Nov BigQuery Table """
    __tablename__ = 'pdr_mod_cope_nov'
    __schema__ = BQPDRCOPENovSchema


class BQPDRCOPENovView(BQModuleView):
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
    _force_boolean_fields = (
        'msds_17_c',
        'cdc_covid_19_7_xx22_date',
        'cdc_covid_19_7_xx23_other_cope_a_204',
        'cope_a_126',
        'nhs_covid_fhc17b_cope_a_226',
        'cdc_covid_19_n_a2',
        'cdc_covid_19_n_a4',
        'cdc_covid_19_n_a8',
        'dmfs_29a',
        'basics_xx',
        'basics_xx20',
        'cu_covid_cope_a_204',
        'cope_aou_xx_2_a',
        'basics_11a_cope_a_33'
    )


class BQPDRCOPEDec(BQTable):
    """ COPE Dec BigQuery Table """
    __tablename__ = 'pdr_mod_cope_dec'
    __schema__ = BQPDRCOPEDecSchema


class BQPDRCOPEDecView(BQModuleView):
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
    _force_boolean_fields = (
        'msds_17_c',
        'cdc_covid_19_7_xx22_date',
        'cdc_covid_19_7_xx23_other_cope_a_204',
        'cope_a_126',
        'nhs_covid_fhc17b_cope_a_226',
        'cdc_covid_19_n_a2',
        'cdc_covid_19_n_a4',
        'cdc_covid_19_n_a8',
        'cdc_covid_xx_b_other',
        'dmfs_29a',
        'basics_xx',
        'basics_xx20',
        'cu_covid_cope_a_204',
        'cope_aou_xx_2_a',
        'basics_11a_cope_a_33'
    )


class BQPDRCOPEFeb(BQTable):
    """ COPE Feb BigQuery Table """
    __tablename__ = 'pdr_mod_cope_feb'
    __schema__ = BQPDRCOPEFebSchema


class BQPDRCOPEFebView(BQModuleView):
    """ PDR COPE Feb BiqQuery View """
    __viewname__ = 'v_pdr_mod_cope_feb'
    __viewdescr__ = 'PDR COPE Feb Module View'
    __table__ = BQPDRCOPEFeb
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


class BQPDRCOPEVaccine1Schema(_BQModuleSchema):
    """ COPE Vaccine Survey 1 (initial) """
    _module = 'cope_vaccine1'
    _force_boolean_fields = (
        'cdc_covid_xx_b_firstdose_other',
        'cdc_covid_xx_symptom_cope_350',
        'cdc_covid_xx_b_seconddose_other',
        'cdc_covid_xx_symptom_seconddose_cope_350',
        'dmfs_29a',
        'dmfs_29_seconddose_other'
    )


class BQPDRCOPEVaccine1(BQTable):
    """ COPE Vaccine 1 BigQuery Table """
    __tablename__ = 'pdr_mod_cope_vaccine1'
    __schema__ = BQPDRCOPEVaccine1Schema


class BQPDRCOPEVaccine1View(BQModuleView):
    """ PDR COPE Vaccine1 BigQuery View """
    __viewname__ = 'v_pdr_mod_cope_vaccine1'
    __viewdescr__ = 'PDR COPE Vaccine1 Module View'
    __table__ = BQPDRCOPEVaccine1
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


class BQPDRCOPEVaccine2Schema(_BQModuleSchema):
    """ COPE Vaccine Survey 2 (Fall 2021) """
    _module = 'cope_vaccine2'
    _force_boolean_fields = (
        'cdc_covid_xx_b_firstdose_other',
        'cdc_covid_xx_symptom_cope_350',
        'cdc_covid_xx_b_seconddose_other',
        'cdc_covid_xx_symptom_seconddose_cope_350',
        'dmfs_29a',
        'dmfs_29_seconddose_other'
    )


class BQPDRCOPEVaccine2(BQTable):
    """ COPE Vaccine 2 BigQuery Table """
    __tablename__ = 'pdr_mod_cope_vaccine2'
    __schema__ = BQPDRCOPEVaccine2Schema


class BQPDRCOPEVaccine2View(BQModuleView):
    """ PDR COPE Vaccine2 BigQuery View """
    __viewname__ = 'v_pdr_mod_cope_vaccine2'
    __viewdescr__ = 'PDR COPE Vaccine2 Module View'
    __table__ = BQPDRCOPEVaccine2
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


# Note:  the StopParticipating and withdrawal_intro module codes are used for similar surveys that contain the
# same survey questions.
class BQPDRWithdrawalIntroSchema(_BQModuleSchema):
    """ Withdrawal Intro Module """
    _module = 'withdrawal_intro'


class BQPDRWithdrawalIntro(BQTable):
    """ Withdrawal Intro BigQuery Table """
    __tablename__ = 'pdr_mod_withdrawalintro'
    __schema__ = BQPDRWithdrawalIntroSchema


class BQPDRStopParticipatingSchema(_BQModuleSchema):
    """ Stop Participating Module """
    _module   = 'StopParticipating'


class BQPDRStopParticipating(BQTable):
    """ Stop Participating BigQuery Table """
    __tablename__ = 'pdr_mod_stopparticipating'
    __schema__ = BQPDRStopParticipatingSchema


# This view will combine results from both pdr_mod_withdrawalintro and pdr_mod_stopparticipating.  It will only include
# common fields that exist in both withdrawal modules.  The StopParticipating module has some extra codes
# (e.g., DeactivatedLogin2_Boolean) that don't apply to the new withdrawal questionnaire.
# TODO:  Confirm if withdrawalreasonother_text should be an excluded field in the base schemas
class BQPDRWithdrawalView(BQModuleView):
    """ PDR Withdrawal module BigQuery View """
    __viewname__ = 'v_pdr_mod_withdrawal'
    __viewdescr__ = 'PDR Withdrawal Module View'
    __table__ = BQPDRWithdrawalIntro
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    __sql__ = """
        SELECT
               id,
               created,
               modified,
               authored,
               `language`,
               participant_id,
               (SELECT test_participant FROM `{project}`.{dataset}.pdr_participant p
                    WHERE p.participant_id = participant_id
                    ORDER BY modified desc LIMIT 1) AS test_participant,
               questionnaire_response_id,
               questionnaire_id,
               external_id,
               status,
               status_id,
               StoppingMeans_StopOptions,
               withdrawal_screen2,
               withdrawalreasonother_text,
               withdrawalaianceremony
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY authored DESC) AS rn
                FROM `{project}`.{dataset}.pdr_mod_withdrawalintro ) wi
        WHERE wi.rn = 1
        UNION ALL
        SELECT
               id,
               created,
               modified,
               authored,
               `language`,
               participant_id,
               (SELECT test_participant FROM `{project}`.{dataset}.pdr_participant p
                    WHERE p.participant_id = participant_id
                    ORDER BY modified desc LIMIT 1) AS test_participant,
               questionnaire_response_id,
               questionnaire_id,
               external_id,
               status,
               status_id,
               StoppingMeans_StopOptions,
               withdrawal_screen2,
               withdrawalreasonother_text,
               withdrawalaianceremony
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY authored DESC) AS rn
                FROM `{project}`.{dataset}.pdr_mod_stopparticipating ) sp
        WHERE sp.rn = 1
    """
    _show_created = True


class BQPDRSDOHSchema(_BQModuleSchema):
    """ Social Determinants of Health Module """
    _module   = 'sdoh'
    _excluded_fields = ('sdoh_intro',)  # Not a topic or question code.
    _force_boolean_fields = ('sdoh_eds_follow_up_1_xx', 'urs_8c')


class BQPDRSDOH(BQTable):
    """ Social Determinants of Health BigQuery Table """
    __tablename__ = 'pdr_mod_sdoh'
    __schema__ = BQPDRSDOHSchema


class BQPDRSDOHView(BQModuleView):
    """ Social Determinants of Health BiqQuery View """
    __viewname__ = 'v_pdr_mod_sdoh'
    __viewdescr__ = 'PDR Social Determinants of Health Module View'
    __table__ = BQPDRSDOH
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


class BQPDRCOPEVaccine3Schema(_BQModuleSchema):
    """ COPE Vaccine Survey 3 (Winter 2021) """
    _module = 'cope_vaccine3'
    _force_boolean_fields = (
        'cdc_covid_xx_b_firstdose_other',
        'cdc_covid_xx_symptom_cope_350',
        'cdc_covid_xx_b_seconddose_other',
        'cdc_covid_xx_symptom_seconddose_cope_350',
        'dmfs_29a',
        'dmfs_29_seconddose_other',
        'cdc_covid_xx_b_dose3_other',
        'cdc_covid_xx_symptom_cope_350_dose3',
        'cdc_covid_xx_type_dose3_other',
        'dmfs_29_additionaldose_other',
        'cdc_covid_xx_b_dose4_other',
        'cdc_covid_xx_symptom_cope_350_dose4',
        'cdc_covid_xx_type_dose4_other',
        'cdc_covid_xx_b_dose5_other',
        'cdc_covid_xx_symptom_cope_350_dose5',
        'cdc_covid_xx_type_dose5_other',
        'cdc_covid_xx_b_dose6_other',
        'cdc_covid_xx_symptom_cope_350_dose6',
        'cdc_covid_xx_type_dose6_other',
        'cdc_covid_xx_b_dose7_other',
        'cdc_covid_xx_symptom_cope_350_dose7',
        'cdc_covid_xx_type_dose7_other',
        'cdc_covid_xx_b_dose8_other',
        'cdc_covid_xx_symptom_cope_350_dose8',
        'cdc_covid_xx_type_dose8_other',
        'cdc_covid_xx_b_dose9_other',
        'cdc_covid_xx_symptom_cope_350_dose9',
        'cdc_covid_xx_type_dose9_other',
        'cdc_covid_xx_b_dose10_other',
        'cdc_covid_xx_symptom_cope_350_dose10',
        'cdc_covid_xx_type_dose10_other',
        'cdc_covid_xx_b_dose11_other',
        'cdc_covid_xx_symptom_cope_350_dose11',
        'cdc_covid_xx_type_dose11_other',
        'cdc_covid_xx_b_dose12_other',
        'cdc_covid_xx_symptom_cope_350_dose12',
        'cdc_covid_xx_type_dose12_other',
        'cdc_covid_xx_b_dose13_other',
        'cdc_covid_xx_symptom_cope_350_dose13',
        'cdc_covid_xx_type_dose13_other',
        'cdc_covid_xx_b_dose14_other',
        'cdc_covid_xx_symptom_cope_350_dose14',
        'cdc_covid_xx_type_dose14_other',
        'cdc_covid_xx_b_dose15_other',
        'cdc_covid_xx_symptom_cope_350_dose15',
        'cdc_covid_xx_type_dose15_other',
        'cdc_covid_xx_b_dose16_other',
        'cdc_covid_xx_symptom_cope_350_dose16',
        'cdc_covid_xx_type_dose16_other',
        'cdc_covid_xx_b_dose17_other',
        'cdc_covid_xx_symptom_cope_350_dose17',
        'cdc_covid_xx_type_dose17_other'
    )


class BQPDRCOPEVaccine3(BQTable):
    """ COPE Vaccine 3 BigQuery Table """
    __tablename__ = 'pdr_mod_cope_vaccine3'
    __schema__ = BQPDRCOPEVaccine3Schema


class BQPDRCOPEVaccine3View(BQModuleView):
    """ PDR COPE Vaccine3 BigQuery View """
    __viewname__ = 'v_pdr_mod_cope_vaccine3'
    __viewdescr__ = 'PDR COPE Vaccine3 Module View'
    __table__ = BQPDRCOPEVaccine3
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


class BQPDRCOPEVaccine4Schema(_BQModuleSchema):
    """ COPE Vaccine Survey 4 (New Year 2022) """
    _module = 'cope_vaccine4'
    _force_boolean_fields = (
        'cdc_covid_xx_b_firstdose_other',
        'cdc_covid_xx_symptom_cope_350',
        'cdc_covid_xx_b_seconddose_other',
        'cdc_covid_xx_symptom_seconddose_cope_350',
        'dmfs_29a',
        'dmfs_29_seconddose_other',
        'cdc_covid_xx_b_dose3_other',
        'cdc_covid_xx_symptom_cope_350_dose3',
        'cdc_covid_xx_type_dose3_other',
        'dmfs_29_additionaldose_other',
        'cdc_covid_xx_b_dose4_other',
        'cdc_covid_xx_symptom_cope_350_dose4',
        'cdc_covid_xx_type_dose4_other',
        'cdc_covid_xx_b_dose5_other',
        'cdc_covid_xx_symptom_cope_350_dose5',
        'cdc_covid_xx_type_dose5_other',
        'cdc_covid_xx_b_dose6_other',
        'cdc_covid_xx_symptom_cope_350_dose6',
        'cdc_covid_xx_type_dose6_other',
        'cdc_covid_xx_b_dose7_other',
        'cdc_covid_xx_symptom_cope_350_dose7',
        'cdc_covid_xx_type_dose7_other',
        'cdc_covid_xx_b_dose8_other',
        'cdc_covid_xx_symptom_cope_350_dose8',
        'cdc_covid_xx_type_dose8_other',
        'cdc_covid_xx_b_dose9_other',
        'cdc_covid_xx_symptom_cope_350_dose9',
        'cdc_covid_xx_type_dose9_other',
        'cdc_covid_xx_b_dose10_other',
        'cdc_covid_xx_symptom_cope_350_dose10',
        'cdc_covid_xx_type_dose10_other',
        'cdc_covid_xx_b_dose11_other',
        'cdc_covid_xx_symptom_cope_350_dose11',
        'cdc_covid_xx_type_dose11_other',
        'cdc_covid_xx_b_dose12_other',
        'cdc_covid_xx_symptom_cope_350_dose12',
        'cdc_covid_xx_type_dose12_other',
        'cdc_covid_xx_b_dose13_other',
        'cdc_covid_xx_symptom_cope_350_dose13',
        'cdc_covid_xx_type_dose13_other',
        'cdc_covid_xx_b_dose14_other',
        'cdc_covid_xx_symptom_cope_350_dose14',
        'cdc_covid_xx_type_dose14_other',
        'cdc_covid_xx_b_dose15_other',
        'cdc_covid_xx_symptom_cope_350_dose15',
        'cdc_covid_xx_type_dose15_other',
        'cdc_covid_xx_b_dose16_other',
        'cdc_covid_xx_symptom_cope_350_dose16',
        'cdc_covid_xx_type_dose16_other',
        'cdc_covid_xx_b_dose17_other',
        'cdc_covid_xx_symptom_cope_350_dose17',
        'cdc_covid_xx_type_dose17_other'
    )


class BQPDRCOPEVaccine4(BQTable):
    """ COPE Vaccine 4 BigQuery Table """
    __tablename__ = 'pdr_mod_cope_vaccine4'
    __schema__ = BQPDRCOPEVaccine4Schema


class BQPDRCOPEVaccine4View(BQModuleView):
    """ PDR COPE Vaccine 4 BigQuery View """
    __viewname__ = 'v_pdr_mod_cope_vaccine4'
    __viewdescr__ = 'PDR COPE Vaccine4 Module View'
    __table__ = BQPDRCOPEVaccine4
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


class BQPDRPersonalFamilyHistorySchema(_BQModuleSchema):
    """  Personal and Family History Module """
    _module   = 'personalfamilyhistory'
    _force_boolean_fields = (
        'OtherCancer_FreeTextBox',
        'OtherCancer_MotherFreeTextBox',
        'OtherCancer_FatherFreeTextBox',
        'OtherCancer_SiblingFreeTextBox',
        'OtherCancer_DaughterFreeTextBox',
        'OtherCancer_SonFreeTextBox',
        'OtherCancer_GrandparentFreeTextBox',
        'OtherHeartorBloodCondition_FreeTextBox',
        'otherheartorbloodcondition_motherfreetextbox',
        'otherheartorbloodcondition_fatherfreetextbox',
        'otherheartorbloodcondition_siblingfreetextbox',
        'otherheartorbloodcondition_daughterfreetextbox',
        'otherheartorbloodcondition_sonfreetextbox',
        'otherheartorbloodcondition_grandparentfreetextbox',
        'OtherDigestiveCondition_FreeTextBox',
        'otherdigestivecondition_motherfreetextbox',
        'otherdigestivecondition_fatherfreetextbox',
        'otherdigestivecondition_siblingfreetextbox',
        'otherdigestivecondition_daughterfreetextbox',
        'otherdigestivecondition_sonfreetextbox',
        'otherdigestivecondition_grandparentfreetextbox',
        'OtherDiabetes_FreeTextBox',
        'otherdiabetes_motherfreetextbox',
        'otherdiabetes_fatherfreetextbox',
        'otherdiabetes_siblingfreetextbox',
        'otherdiabetes_daughterfreetextbox',
        'otherdiabetes_sonfreetextbox',
        'otherdiabetes_grandparentfreetextbox',
        'OtherThyroid_FreeTextBox',
        'otherthyroid_motherfreetextbox',
        'otherthyroid_fatherfreetextbox',
        'otherthyroid_siblingfreetextbox',
        'otherthyroid_daughterfreetextbox',
        'otherthyroid_sonfreetextbox',
        'otherthyroid_grandparentfreetextbox',
        'OtherHormoneEndocrine_FreeTextBox',
        'otherhormoneendocrine_motherfreetextbox',
        'otherhormoneendocrine_fatherfreetextbox',
        'otherhormoneendocrine_siblingfreetextbox',
        'otherhormoneendocrine_daughterfreetextbox',
        'otherhormoneendocrine_sonfreetextbox',
        'otherhormoneendocrine_grandparentfreetextbox',
        'OtherKidneyCondition_FreeTextBox',
        'otherkidneycondition_motherfreetextbox',
        'otherkidneycondition_fatherfreetextbox',
        'otherkidneycondition_siblingfreetextbox',
        'otherkidneycondition_daughterfreetextbox',
        'otherkidneycondition_sonfreetextbox',
        'otherkidneycondition_grandparentfreetextbox',
        'OtherRespiratory_FreeTextBox',
        'otherrespiratory_motherfreetextbox',
        'otherrespiratory_fatherfreetextbox',
        'otherrespiratory_siblingfreetextbox',
        'otherrespiratory_daughterfreetextbox',
        'otherrespiratory_sonfreetextbox',
        'otherrespiratory_grandparentfreetextbox',
        'OtherBrainNervousSystem_FreeTextBox',
        'otherbrainnervoussystem_motherfreetextbox',
        'otherbrainnervoussystem_fatherfreetextbox',
        'otherbrainnervoussystem_siblingfreetextbox',
        'otherbrainnervoussystem_daughterfreetextbox',
        'otherbrainnervoussystem_sonfreetextbox',
        'otherbrainnervoussystem_grandparentfreetextbox',
        'OtherMentalHealthSubstanceUse_FreeTextBox',
        'othermentalhealthsubstanceuse_motherfreetextbox',
        'othermentalhealthsubstanceuse_fatherfreetextbox',
        'othermentalhealthsubstanceuse_siblingfreetextbox',
        'othermentalhealthsubstanceuse_daughterfreetextbox',
        'othermentalhealthsubstanceuse_sonfreetextbox',
        'othermentalhealthsubstanceuse_grandparentfreetextb',
        'OtherArthritis_FreeTextBox',
        'otherarthritis_motherfreetextbox',
        'otherarthritis_fatherfreetextbox',
        'otherarthritis_siblingfreetextbox',
        'otherarthritis_daughterfreetextbox',
        'otherarthritis_sonfreetextbox',
        'otherarthritis_grandparentfreetextbox',
        'OtherBoneJointMuscle_FreeTextBox',
        'otherbonejointmuscle_motherfreetextbox',
        'otherbonejointmuscle_fatherfreetextbox',
        'otherbonejointmuscle_siblingfreetextbox',
        'otherbonejointmuscle_daughterfreetextbox',
        'otherbonejointmuscle_sonfreetextbox',
        'otherbonejointmuscle_grandparentfreetextbox',
        'OtherHearingEye_FreeTextBox',
        'otherhearingeye_motherfreetextbox',
        'otherhearingeye_fatherfreetextbox',
        'otherhearingeye_siblingfreetextbox',
        'otherhearingeye_daughterfreetextbox',
        'otherhearingeye_sonfreetextbox',
        'otherhearingeye_grandparentfreetextbox',
        'OtherDiagnosis_FreeTextBox',
        'otherdiagnosis_motherfreetextbox',
        'otherdiagnosis_fatherfreetextbox',
        'otherdiagnosis_siblingfreetextbox',
        'otherdiagnosis_daughterfreetextbox',
        'otherdiagnosis_sonfreetextbox',
        'otherdiagnosis_grandparentfreetextbox',
        'OtherInfectiousDisease_FreeTextBox'
    )


class BQPDRPersonalFamilyHistory(BQTable):
    """ Personal and Family History BigQuery Table """
    __tablename__ = 'pdr_mod_personalfamilyhistory'
    __schema__ = BQPDRPersonalFamilyHistorySchema


class BQPDRPersonalFamilyHistoryView(BQModuleView):
    """ Personal and Family History BiqQuery View """
    __viewname__ = 'v_pdr_mod_personalfamilyhistory'
    __viewdescr__ = 'PDR Personal and Family History Module View'
    __table__ = BQPDRPersonalFamilyHistory
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# GeneralFeedback
#
class BQPDRGeneralFeedbackSchema(_BQModuleSchema):
    """ GeneralFeedback Module """
    _module = 'GeneralFeedback'
    _force_boolean_fields = (
        'Other_OtherFreeTextBox',
        'OtherReasonToJoin_FreeTextBox',
        'OtherReasonWhyHard_FreeTextBox',
        'AccountCreation_IdeasForEasierAccountCreation',
        'OtherEaseOfUnderstanding_FreeTextBox',
        'GeneralConsent_HowToMakeConsentBetter',
        'GeneralConsent_OtherComments'
    )


class BQPDRGeneralFeedback(BQTable):
    """ GeneralFeedback BigQuery Table """
    __tablename__ = 'pdr_mod_general_feedback'
    __schema__ = BQPDRGeneralFeedbackSchema


class BQPDRGeneralFeedbackView(BQModuleView):
    """ PDR GeneralFeedback BiqQuery View """
    __viewname__ = 'v_pdr_mod_general_feedback'
    __viewdescr__ = 'PDR GeneralFeedback Module View'
    __table__ = BQPDRGeneralFeedback
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# PostPMBFeedback
#
class BQPDRPostPMBFeedbackSchema(_BQModuleSchema):
    """ PostPMBFeedback Module """
    _module = 'PostPMBFeedback'
    _force_boolean_fields = (
        'PostPMBFeedback_ProblemsComingToVisit',
        'PostPMBFeedback_LikedDislikedAboutVisit'
    )


class BQPDRPostPMBFeedback(BQTable):
    """ PostPMBFeedback BigQuery Table """
    __tablename__ = 'pdr_mod_post_pmb_feedback'
    __schema__ = BQPDRPostPMBFeedbackSchema


class BQPDRPostPMBFeedbackView(BQModuleView):
    """ PDR PostPMBFeedback BiqQuery View """
    __viewname__ = 'v_pdr_mod_post_pmb_feedback'
    __viewdescr__ = 'PDR PostPMBFeedback Module View'
    __table__ = BQPDRPostPMBFeedback
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True


#
# PPIModuleFeedback
#
class BQPDRPPIModuleFeedbackSchema(_BQModuleSchema):
    """ PPIModuleFeedback Module """
    _module = 'PPIModuleFeedback'
    _force_boolean_fields = (
        'PPIFeedback_WhatMadeItHardToUnderstand',
        'PPIFeedback_WhySkippingQuestions',
        'PPIFeedback_LikedDislikedAboutQuestions'
    )


class BQPDRPPIModuleFeedback(BQTable):
    """ PPIModuleFeedback BigQuery Table """
    __tablename__ = 'pdr_mod_ppi_module_feedback'
    __schema__ = BQPDRPPIModuleFeedbackSchema


class BQPDRPPIModuleFeedbackView(BQModuleView):
    """ PDR PPIModuleFeedback BiqQuery View """
    __viewname__ = 'v_pdr_mod_ppi_module_feedback'
    __viewdescr__ = 'PDR PPIModuleFeedback Module View'
    __table__ = BQPDRPPIModuleFeedback
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True

# PDR-861:  Add WEAR Consent module:
class BQPDRWearConsentSchema(_BQModuleSchema):
    """ WEAR Consent Module """
    _module = 'wear_consent'
    _force_boolean_fields = (
        'timeofday',
        'HelpMeWithConsent_Name'
    )

class BQPDRWearConsent(BQTable):
    """ PDR wear_consent BigQuery Table """
    __tablename__ = 'pdr_mod_wear_consent'
    __schema__ = BQPDRWearConsentSchema


class BQPDRWearConsentView(BQModuleView):
    """ PDR Wear_Consent BigQuery View """
    __viewname__ = 'v_pdr_mod_wear_consent'
    __viewdescr__ = 'PDR wear_consent Module View'
    __table__ = BQPDRWearConsent
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True

#
# PDR-1200: Life Functioning Survey
#
class BQPDRLifeFunctioningSurveySchema(_BQModuleSchema):
    """ Life Functioning Survey Module """
    _module = 'lfs'

class BQPDRLifeFunctioningSurvey(BQTable):
    """ PDR Life Functioning Survey BigQuery Table """
    __tablename__ = 'pdr_mod_life_functioning'
    __schema__ = BQPDRLifeFunctioningSurveySchema


class BQPDRLifeFunctioningSurveyView(BQModuleView):
    """ PDR Life Functioning Survey BigQuery View """
    __viewname__ = 'v_pdr_mod_life_functioning'
    __viewdescr__ = 'PDR Life Functioning Survey Module View'
    __table__ = BQPDRLifeFunctioningSurvey
    __pk_id__ = ['participant_id', 'questionnaire_response_id']
    _show_created = True

#
#
#
# List of modules classes that are sent to PDR.
#
# TODO: Include any new modules added PDR to this list.
PDR_MODULE_LIST = (
    BQPDRConsentPII,
    BQPDRTheBasics,
    BQPDRLifestyle,
    BQPDROverallHealth,
    BQPDREHRConsentPII,
    BQPDRDVEHRSharing,
    BQPDRGROR,
    BQPDRCOPEMay,
    BQPDRCOPENov,
    BQPDRCOPEDec,
    BQPDRCOPEFeb,
    BQPDRCOPEVaccine1,
    BQPDRCOPEVaccine2,
    BQPDRFamilyHistory,
    BQPDRPersonalMedicalHistory,
    BQPDRHealthcareAccess,
    BQPDRStopParticipating,
    BQPDRWithdrawalIntro,
    BQPDRSDOH,
    BQPDRCOPEVaccine3,
    BQPDRCOPEVaccine4,
    BQPDRPersonalFamilyHistory,
    BQPDRGeneralFeedback,
    BQPDRPostPMBFeedback,
    BQPDRPPIModuleFeedback,
    BQPDRWearConsent,
    BQPDRLifeFunctioningSurvey
)

# Create a dictionary of module codes and table object references.
PDR_CODE_TO_MODULE_LIST = dict(zip([m.__schema__._module for m in PDR_MODULE_LIST], PDR_MODULE_LIST))
