import json

from sqlalchemy.sql import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.model.bq_base import BQTable, BQSchema, BQView, BQFieldModeEnum, BQFieldTypeEnum


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

    def get_fields(self):
        """
        Look up a participant id who has submitted this module and then get the module response answers to use
        for creating the schema.
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
        fields.append(
            {'name': 'authored', 'type': BQFieldTypeEnum.DATETIME.name, 'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'language', 'type': BQFieldTypeEnum.STRING.name, 'mode': BQFieldModeEnum.NULLABLE.name})
        fields.append({'name': 'participant_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.REQUIRED.name})
        fields.append({'name': 'questionnaire_response_id', 'type': BQFieldTypeEnum.INTEGER.name,
                       'mode': BQFieldModeEnum.REQUIRED.name})

        dao = BigQuerySyncDao()

        _sql_term = text("""
        select convert(qh.resource using utf8) as resource 
          from questionnaire_concept qc inner join code c on qc.code_id = c.code_id
               inner join questionnaire_history qh on qc.questionnaire_id = qh.questionnaire_id and 
                          qc.questionnaire_version = qh.version
        where c.value = :mod
        order by qh.created desc limit 1;
    """)

        with dao.session() as session:

            # get a participant id that has submitted the module
            result = session.execute(_sql_term, {'mod': self._module}).first()
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

                field = dict()
                field['name'] = name
                field['type'] = BQFieldTypeEnum.STRING.name
                field['mode'] = BQFieldModeEnum.NULLABLE.name
                field['enum'] = None
                fields.append(field)

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
        'ExtraConsent_Signature'
    )


class BQPDRConsentPII(BQTable):
    """ PDR ConsentPII BigQuery Table """
    __tablename__ = 'pdr_mod_consentpii'
    __schema__ = BQPDRConsentPIISchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDRConsentPIIView(BQView):
    """ PDR ConsentPII BigQuery View """
    __viewname__ = 'v_pdr_mod_consentpii'
    __viewdescr__ = 'PDR ConsentPII Module View'
    __table__ = BQPDRConsentPII
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
        'SecondContactsAddress_SecondContactZipCode'
    )


class BQPDRTheBasics(BQTable):
    """ TheBasics BigQuery Table """
    __tablename__ = 'pdr_mod_thebasics'
    __schema__ = BQPDRTheBasicsSchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDRTheBasicsView(BQView):
    """ PDR TheBasics BiqQuery View """
    __viewname__ = 'v_pdr_mod_thebasics'
    __viewdescr__ = 'PDR TheBasics Module View'
    __table__ = BQPDRTheBasics
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
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDRLifestyleView(BQView):
    """ PDR TheBasics BiqQuery View """
    __viewname__ = 'v_pdr_mod_lifestyle'
    __viewdescr__ = 'PDR Lifestyle Module View'
    __table__ = BQPDRLifestyle
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
    )


class BQPDROverallHealth(BQTable):
    """ OverallHealth BigQuery Table """
    __tablename__ = 'pdr_mod_overallhealth'
    __schema__ = BQPDROverallHealthSchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDROverallHealthView(BQView):
    """ PDR OverallHealth BiqQuery View """
    __viewname__ = 'v_pdr_mod_overallhealth'
    __viewdescr__ = 'PDR OverallHealth Module View'
    __table__ = BQPDROverallHealth
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
        'EHRConsentPII_HelpWithConsentSignature'
    )


class BQPDREHRConsentPII(BQTable):
    """ EHRConsentPII BigQuery Table """
    __tablename__ = 'pdr_mod_ehrconsentpii'
    __schema__ = BQPDREHRConsentPIISchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDREHRConsentPIIView(BQView):
    """ PDR EHRConsentPII BiqQuery View """
    __viewname__ = 'v_pdr_mod_ehrconsentpii'
    __viewdescr__ = 'PDR EHRConsentPII Module View'
    __table__ = BQPDREHRConsentPII
    _show_created = True


#
# DVEHRConsentPII
#
class BQPDRDVEHRSharingSchema(_BQModuleSchema):
    """ EHRConsentPII Module """
    _module = 'DVEHRSharing'
    _excluded_fields = (
        'EHRConsentPII_Signature',
    )


class BQPDRDVEHRSharing(BQTable):
    """ DVEHRConsentPII BigQuery Table """
    __tablename__ = 'pdr_mod_dvehrsharing'
    __schema__ = BQPDRDVEHRSharingSchema
    __project_map__ = [
        ('all-of-us-rdr-prod', ('aou-pdr-data-prod', 'rdr_ops_data_view')),
    ]


class BQPDRDVEHRSharingView(BQView):
    """ PDR DVEHRConsentPII BiqQuery View """
    __viewname__ = 'v_pdr_mod_dvehrsharing'
    __viewdescr__ = 'PDR DVEHRConsentPII Module View'
    __table__ = BQPDRDVEHRSharing
    _show_created = True
