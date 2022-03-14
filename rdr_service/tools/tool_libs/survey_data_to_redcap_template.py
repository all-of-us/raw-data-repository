import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import logging
import sys
import json

from collections import OrderedDict

from rdr_service.model import BQ_TABLES
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.code_dao import CodeDao

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "survey-data-to-redcap"
tool_desc = "Extracts module response data into a REDCap-conformant format for REDCap import"

class SurveyToRedCapConversion(object):

    DEFAULT_REDCAP_TEMPLATE_RECORD = {
        "TheBasics":  OrderedDict([
            ('thebasics_birthplace', None),
            ('thebasics_countryborntextbox', None),
            ('race_whatraceethnicity___whatraceethnicity_aian', 0),
            ('race_whatraceethnicity___whatraceethnicity_asian', 0),
            ('race_whatraceethnicity___whatraceethnicity_black', 0),
            ('race_whatraceethnicity___whatraceethnicity_hispanic', 0),
            ('race_whatraceethnicity___whatraceethnicity_mena', 0),
            ('race_whatraceethnicity___whatraceethnicity_nhpi', 0),
            ('race_whatraceethnicity___whatraceethnicity_white', 0),
            ('race_whatraceethnicity___whatraceethnicity_raceethnicitynoneofthese', 0),
            ('race_whatraceethnicity___pmi_prefernottoanswer', 0),
            ('whatraceethnicity_raceethnicitynoneofthese', None),
            ('aian_aianspecific___aianspecific_americanindian', 0),
            ('aian_aianspecific___aianspecific_alaskanative', 0),
            ('aian_aianspecific___aianspecific_centralsouthamericanindian', 0),
            ('aian_aianspecific___aianspecific_aiannoneofthesedescribeme', 0),
            ('aian_tribe', None),
            ('aiannoneofthesedescribeme_aianfreetext', None),
            ('asian_asianspecific___asianspecific_asianspecificindian', 0),
            ('asian_asianspecific___asianspecific_cambodian', 0),
            ('asian_asianspecific___asianspecific_chinese', 0),
            ('asian_asianspecific___asianspecific_filipino', 0),
            ('asian_asianspecific___asianspecific_hmong', 0),
            ('asian_asianspecific___asianspecific_japanese', 0),
            ('asian_asianspecific___asianspecific_korean', 0),
            ('asian_asianspecific___asianspecific_pakastani', 0),
            ('asian_asianspecific___asianspecific_vietnamese', 0),
            ('asian_asianspecific___asianspecific_noneofthesedescribeme', 0),
            ('noneofthesedescribeme_asianfreetext', None),
            ('black_blackspecific___blackspecific_africanamerican', 0),
            ('black_blackspecific___blackspecific_barbadian', 0),
            ('black_blackspecific___blackspecific_caribbean', 0),
            ('black_blackspecific___blackspecific_ethiopian', 0),
            ('black_blackspecific___blackspecific_ghanaian', 0),
            ('black_blackspecific___blackspecific_haitian', 0),
            ('black_blackspecific___blackspecific_jamaican', 0),
            ('black_blackspecific___blackspecific_liberian', 0),
            ('black_blackspecific___blackspecific_nigerian', 0),
            ('black_blackspecific___blackspecific_somali', 0),
            ('black_blackspecific___blackspecific_southafrican', 0),
            ('black_blackspecific___blackspecific_blacknoneofthesedescribeme', 0),
            ('blacknoneofthesedescribeme_blackfreetext', 0),
            ('hispanic_hispanicspecific___hispanicspecific_colombian', 0),
            ('hispanic_hispanicspecific___hispanicspecific_cuban', 0),
            ('hispanic_hispanicspecific___hispanicspecific_dominican', 0),
            ('hispanic_hispanicspecific___hispanicspecific_ecuadorian', 0),
            ('hispanic_hispanicspecific___hispanicspecific_honduran', 0),
            ('hispanic_hispanicspecific___hispanicspecific_mexican', 0),
            ('hispanic_hispanicspecific___hispanicspecific_puertorican', 0),
            ('hispanic_hispanicspecific___hispanicspecific_salvadoran', 0),
            ('hispanic_hispanicspecific___hispanicspecific_spanish', 0),
            ('hispanic_hispanicspecific___hispanicspecific_hispanicnoneofthesedescribeme', 0),
            ('hispanicnoneofthesedescribeme_hispanicfreetext', None),
            ('mena_menaspecific___menaspecific_afghan', 0),
            ('mena_menaspecific___menaspecific_algerian', 0),
            ('mena_menaspecific___menaspecific_egypt', 0),
            ('mena_menaspecific___menaspecific_iranian', 0),
            ('mena_menaspecific___menaspecific_iraqi', 0),
            ('mena_menaspecific___menaspecific_israeli', 0),
            ('mena_menaspecific___menaspecific_lebanese', 0),
            ('mena_menaspecific___menaspecific_moroccan', 0),
            ('mena_menaspecific___menaspecific_syrian', 0),
            ('mena_menaspecific___menaspecific_tunisian', 0),
            ('mena_menaspecific___menaspecific_menanoneofthesedescribeme', 0),
            ('menanoneofthesedescribeme_menafreetext', None),
            ('nhpi_nhpispecific___nhpispecific_chamorro', 0),
            ('nhpi_nhpispecific___nhpispecific_chuukese', 0),
            ('nhpi_nhpispecific___nhpispecific_fijian', 0),
            ('nhpi_nhpispecific___nhpispecific_marshallese', 0),
            ('nhpi_nhpispecific___nhpispecific_nativehawaiian', 0),
            ('nhpi_nhpispecific___nhpispecific_palauan', 0),
            ('nhpi_nhpispecific___nhpispecific_samoan', 0),
            ('nhpi_nhpispecific___nhpispecific_tahitian', 0),
            ('nhpi_nhpispecific___nhpispecific_tongan', 0),
            ('nhpi_nhpispecific___nhpispecific_nhpinoneofthesedescribeme', 0),
            ('nhpinoneofthesedescribeme_nhpifreetext', None),
            ('white_whitespecific___whitespecific_dutch', 0),
            ('white_whitespecific___whitespecific_english', 0),
            ('white_whitespecific___whitespecific_european', 0),
            ('white_whitespecific___whitespecific_french', 0),
            ('white_whitespecific___whitespecific_german', 0),
            ('white_whitespecific___whitespecific_irish', 0),
            ('white_whitespecific___whitespecific_italian', 0),
            ('white_whitespecific___whitespecific_norwegian', 0),
            ('white_whitespecific___whitespecific_polish', 0),
            ('white_whitespecific___whitespecific_scottish', 0),
            ('white_whitespecific___whitespecific_spanish', 0),
            ('white_whitespecific___whitespecific_whitenoneofthesedescribeme', 0),
            ('whitenoneofthesedescribeme_whitefreetext', None),
            ('gender_genderidentity___genderidentity_man', 0),
            ('gender_genderidentity___genderidentity_woman', 0),
            ('gender_genderidentity___genderidentity_nonbinary', 0),
            ('gender_genderidentity___genderidentity_transgender', 0),
            ('gender_genderidentity___genderidentity_additionaloptions', 0),
            ('gender_genderidentity___pmi_prefernottoanswer', 0),
            ('gender_closergenderdescription___closergenderdescription_transman', 0),
            ('gender_closergenderdescription___closergenderdescription_transwoman', 0),
            ('gender_closergenderdescription___closergenderdescription_genderqueer', 0),
            ('gender_closergenderdescription___closergenderdescription_genderfluid', 0),
            ('gender_closergenderdescription___closergenderdescription_gendervariant', 0),
            ('gender_closergenderdescription___sexualitycloserdescription_twospirit', 0),
            ('gender_closergenderdescription___closergenderdescription_unsure', 0),
            ('gender_closergenderdescription___closergenderdescription_specifiedgender', 0),
            ('specifiedgender_specifiedgendertextbox', None),
            ('biologicalsexatbirth_sexatbirth', None),
            ('sexatbirthnoneofthese_sexatbirthtextbox', None),
            ('thebasics_sexualorientation___sexualorientation_gay', 0),
            ('thebasics_sexualorientation___sexualorientation_lesbian', 0),
            ('thebasics_sexualorientation___sexualorientation_straight', 0),
            ('thebasics_sexualorientation___sexualorientation_bisexual', 0),
            ('thebasics_sexualorientation___sexualorientation_none', 0),
            ('thebasics_sexualorientation___pmi_prefernottoanswer', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_queer', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_polyomnisapiopansexual', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_asexual', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_twospirit', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_notfiguredout', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_mostlystraight', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_nosexuality', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_nolabels', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_dontknow', 0),
            ('genderidentity_sexualitycloserdescription___sexualitycloserdescription_somethingelse', 0),
            ('somethingelse_sexualitysomethingelsetextbox', None),
            ('educationlevel_highestgrade', None),
            ('activeduty_activedutyservestatus', None),
            ('activeduty_avtivedutyservestatus', None),
            ('maritalstatus_currentmaritalstatus', None),
            ('livingsituation_howmanypeople', None),
            ('livingsituation_peopleunder18', None),
            ('insurance_healthinsurance', None),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_purchased', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_employerorunion', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_medicare', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_medicaid', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_military', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_va', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_indian', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_otherhealthplan', 0),
            ('healthinsurance_insurancetypeupdate___insurancetypeupdate_none', 0),
            ('otherhealthplan_freetext', None),
            ('disability_deaf', None),
            ('disability_blind', None),
            ('disability_difficultyconcentrating', None),
            ('disability_walkingclimbing', None),
            ('disability_dressingbathing', None),
            ('disability_errandsalone', None),
            ('employment_employmentstatus___employmentstatus_employedforwages', 0),
            ('employment_employmentstatus___employmentstatus_selfemployed', 0),
            ('employment_employmentstatus___employmentstatus_outofworkoneormore', 0),
            ('employment_employmentstatus___employmentstatus_outofworklessthanone', 0),
            ('employment_employmentstatus___employmentstatus_homemaker', 0),
            ('employment_employmentstatus___employmentstatus_student', 0),
            ('employment_employmentstatus___employmentstatus_retired', 0),
            ('employment_employmentstatus___employmentstatus_unabletowork', 0),
            ('employment_employmentstatus___pmi_prefernottoanswer', 0),
            ('employment_employmentworkaddress', None),
            ('employmentworkaddress_addresslineone', None),
            ('employmentworkaddress_addresslinetwo', None),
            ('employmentworkaddress_city', None),
            ('employmentworkaddress_state', None),
            ('employmentworkaddress_zipcode', None),
            ('employmentworkaddress_country', None),
            ('income_annualincome', None),
            ('homeown_currenthomeown', None),
            ('livingsituation_currentliving', None),
            ('livingsituation_livingsituationfreetext', None),
            ('livingsituation_howmanylivingyears', None),
            ('livingsituation_stablehouseconcern', None),
            ('socialsecurity_socialsecuritynumber', None),
            ('socialsecurity_socialsecuritynumber_text', None),
            ('secondarycontactinfo_persononefirstname', None),
            ('secondarycontactinfo_persononemiddleinitial', None),
            ('secondarycontactinfo_persononelastname', None),
            ('secondarycontactinfo_persononeaddressone', None),
            ('secondarycontactinfo_persononeaddresstwo', None),
            ('persononeaddress_persononeaddresscity', None),
            ('persononeaddress_persononeaddressstate', None),
            ('persononeaddress_persononeaddresszipcode', None),
            ('secondarycontactinfo_persononeemail', None),
            ('secondarycontactinfo_persononetelephone', None),
            ('secondarycontactinfo_persononerelationship', None),
            ('secondarycontactinfo_secondcontactsfirstname', None),
            ('secondarycontactinfo_secondcontactsmiddleinitial', None),
            ('secondarycontactinfo_secondcontactslastname', None),
            ('secondarycontactinfo_secondcontactsaddressone', None),
            ('secondarycontactinfo_secondcontactsaddresstwo', None),
            ('secondcontactsaddress_secondcontactcity', None),
            ('secondcontactsaddress_secondcontactstate', None),
            ('secondcontactsaddress_secondcontactzipcode', None),
            ('secondarycontactinfo_secondcontactsemail', None),
            ('secondarycontactinfo_secondcontactsnumber', None),
            ('secondarycontactinfo_secondcontactsrelationship', None),
        ])
    }

    parent_code_ordered_lists = {
        # This lays out the questions codes in the order they are listed in the REDCap template
        # referred to in DA-250.  That template follows a topic-based grouping/ordering
        #
        # NOTE:  Some codes/responses exist in RDR which may not be represented in the REDCap template (e.g.,
        # 'WhatTribeAffiliation_FreeText'.  Including here so we can catch those inconsistencies in the REDCap data
        # dictionary
        'TheBasics': [
            'TheBasics_Birthplace', 'TheBasics_CountryBornTextBox',
            'Race_WhatRaceEthnicity',
            'AIAN_AIANSpecific', 'AIANNoneOfTheseDescribeMe_AIANFreeText','AIAN_Tribe', 'WhatTribeAffiliation_FreeText',
            'Asian_AsianSpecific', 'NoneOfTheseDescribeMe_AsianFreeText',
            'Black_BlackSpecific', 'BlackNoneOfTheseDescribeMe_BlackFreeText',
            'Hispanic_HispanicSpecific', 'HispanicNoneOfTheseDescribeMe_HispanicFreeText',
            'MENA_MENASpecific', 'MENANoneOfTheseDescribeMe_MENAFreeText',
            'NHPI_NHPISpecific', 'NHPINoneOfTheseDescribeMe_NHPIFreeText',
            'White_WhiteSpecific', 'WhiteNoneOfTheseDescribeMe_WhiteFreeText',
            'RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox',
            'Gender_GenderIdentity', 'Gender_CloserGenderDescription', 'SpecifiedGender_SpecifiedGenderTextBox',
            'BiologicalSexAtBirth_SexAtBirth', 'SexAtBirthNoneOfThese_SexAtBirthTextBox',
            'TheBasics_SexualOrientation', 'GenderIdentity_SexualityCloserDescription',
            'SomethingElse_SexualitySomethingElseTextBox',
            'EducationLevel_HighestGrade',
            'ActiveDuty_AvtiveDutyServeStatus',   # NOTE:  REDCap has proper spelling ActiveDutyServeStatus
            'MaritalStatus_CurrentMaritalStatus',
            'LivingSituation_HowManyPeople',
            'LivingSituation_PeopleUnder18',
            'Insurance_HealthInsurance', 'HealthInsurance_HealthInsuranceType', 'HealthInsurance_InsuranceTypeUpdate',
            'OtherHealthPlan_FreeText',
            'Disability_Deaf', 'Disability_Blind', 'Disability_DifficultyConcentrating',
            'Disability_WalkingClimbing', 'Disability_DressingBathing', 'Disability_ErrandsAlone',
            'Employment_EmploymentStatus', 'Employment_EmploymentWorkAddress',
            'EmploymentWorkAddress_AddressLineOne', 'EmploymentWorkAddress_AddressLineTwo',
            'EmploymentWorkAddress_City', 'EmploymentWorkAddress_State',
            'EmploymentWorkAddress_ZipCode', 'EmploymentWorkAddress_Country',
            'Income_AnnualIncome', 'HomeOwn_CurrentHomeOwn',
            'LivingSituation_CurrentLiving', 'LivingSituation_LivingSituationFreeText',
            'LivingSituation_HowManyLivingYears', 'LivingSituation_StableHouseConcern',
            'SocialSecurity_SocialSecurityNumber', 'SocialSecurity_PreferNotToAnswer',
            'SecondaryContactInfo_PersonOneFirstName', 'SecondaryContactInfo_PersonOneMiddleInitial',
            'SecondaryContactInfo_PersonOneLastName', 'SecondaryContactInfo_PersonOneAddressOne',
            'SecondaryContactInfo_PersonOneAddressTwo', 'PersonOneAddress_PersonOneAddressCity',
            'PersonOneAddress_PersonOneAddressState', 'PersonOneAddress_PersonOneAddressZipCode',
            'SecondaryContactInfo_PersonOneEmail', 'SecondaryContactInfo_PersonOneTelephone',
            'SecondaryContactInfo_PersonOneRelationship', 'SecondaryContactInfo_SecondContactsFirstName',
            'SecondaryContactInfo_SecondContactsMiddleInitial', 'SecondaryContactInfo_SecondContactsLastName',
            'SecondaryContactInfo_SecondContactsAddressOne', 'SecondaryContactInfo_SecondContactsAddressTwo',
            'SecondContactsAddress_SecondContactCity', 'SecondContactsAddress_SecondContactState',
            'SecondContactsAddress_SecondContactZipCode', 'SecondaryContactInfo_SecondContactsEmail',
            'SecondaryContactInfo_SecondContactsNumber', 'SecondaryContactInfo_SecondContactsRelationship'
        ]
    }

    # Static class variables / lists that will be populated as values are discovered, so they can act as a class cache
    code_display_values = {}
    pdr_table_mod_classes = {}

    # For branching logic option menus, associate the codes for that option list to the "parent" question code
    prefix_to_parent_code_map = {
        "WhatRaceEthnicity_": "Race_WhatRaceEthnicity",
        "AIANSpecific_": "AIAN_AIANSpecific",
        "AsianSpecific_": "Asian_AsianSpecific",
        "BlackSpecific_": "Black_BlackSpecific",
        "HispanicSpecific_": "Hispanic_HispanicSpecific",
        "MENASpecific_": "MENA_MENASpecific",
        "NHPISpecific_": "NHPI_NHPISpecific",
        "WhiteSpecific_": "White_WhiteSpecific",
        "GenderIdentity_": "Gender_GenderIdentity",
        "CloserGenderDescription_": "Gender_CloserGenderDescription",
        "SexualOrientation_": "TheBasics_SexualOrientation",
        "SexualityCloserDescription_": "GenderIdentity_SexualityCloserDescription",
        "InsuranceType_": "Insurance_InsuranceType",
        "HealthInsuranceType_": "HealthInsurance_HealthInsuranceType",
        "InsuranceTypeUpdate_": "HealthInsurance_InsuranceTypeUpdate",
        "EmploymentStatus_": "Employment_EmploymentStatus",

    }

    def __init__(self, args, gcp_env: GCPEnvConfigObject, module='TheBasics'):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.set_bq_table(module)
        # Build the two dimensional array that can turned into a CSV suitable for REDCap import.  Initialize to
        # a list of rows where the first element is the REDCap field name
        self.redcap_export_rows = [['record_id', ]]
        for field_name in self.DEFAULT_REDCAP_TEMPLATE_RECORD[module]:
            self.redcap_export_rows.append([field_name, ])

    def _get_question_type(self, code):
        """
        Find the SurveyQuestionType for a question code value
        TODO:  If the module has had its codebook synced from REDCap, use  the RDR survey_question table details to
        determine SurveyQuestionType (e.g., RADIO, CHECKBOX, etc.)
        """
        pass

    def _generate_response_id_list(self, num_records=20, min_authored=None, max_authored=None):
        """
        Select a sample of questionnaire_response_id values for analysis
        """
        pass

    def get_pdr_bq_table_id(self):
        """ Return the table_id string associated with the module in the RDR bigquery_sync table generated records """
        if not self.bq_table:
            _logger.error('This object instance does not have a bq_table attribute set')
            return None
        return self.bq_table.get_name().lower()

    def set_bq_table(self, module):
        """ Set the instance bq_table variable with the appropriate _BQModuleSchema object """
        self.bq_table = None

        # Check the local class cache first for a matching module name
        if module in self.pdr_table_mod_classes.keys():
            self.bq_table = self.pdr_table_mod_classes[module]()
            return

        # If there was no match in the class cache, search the defined BQ_TABLES list for a match
        table_id = f'pdr_mod_{module.lower()}'
        for path, var_name in BQ_TABLES:
            mod = importlib.import_module(path, var_name)
            mod_class = getattr(mod, var_name)
            bq_table = mod_class()
            if bq_table.get_name().lower() == table_id:
                self.bq_table = bq_table
                # Cache the mod_class match for this module
                self.pdr_table_mod_classes[module] = mod_class
                return

        raise ValueError(f'A PDR BQ_TABLES table definition for module {module} was not found')

    def get_code_display_value(self, code):
        """
        Return the display string from the code table for the given code value.  Used for exporting responses to
        radio button questions
        """
        # Check the class cache first
        if code in self.code_display_values.keys():
            return self.code_display_values[code]

        with CodeDao().session() as session:
            row = session.execute(f'select display from code where value = "{code}"').first()

        if row:
            # Cache the display value for this code, for future use
            self.code_display_values[code] = row.display
            return row.display

        _logger.warning(f'Display value requested for Unknown code {code}')
        return None

    def is_freetext_code(self, code):
        """
        Use the _force_boolean_fields list from the PDR (BQ) Module Schema to identify freetext codes
        """
        if not self.bq_table:
            raise ValueError('The instance does not have a bq_table attribute set')

        return code in self.bq_table.__schema__._force_boolean_fields

    def get_redcap_fieldname(self, code, parent=None):
        """
        Based on the code string and parent code string, generate a REDCap field name
        Ex:  code = 'TheBasics_Birthplace' where parent_code = None: returns 'thebasics_birthplace'
             code = 'WhatRaceEthnicity_AIAN', where parent_code = 'Race_WhatRaceEthnicity':
                            returns  'race_whatraceethnicity___whatraceethnicity_aian'
        :param code: Value string from the RDR code table (code.value)
        :param parent_code: A parent question code value string from the RDR code table, if the code param is an
                            option menu answer code
        :return: A string in the expected REDCap field name format
        """
        if not parent:
            return code.lower()
        else:
            return "".join([parent.lower(), f'___{code.lower()}'])

    def redcap_prefer_not_to_answer(self, question_code):
        """
        The PMI_PreferNotToAnswer answer code is associated with multiple surveys and survey questions.
        When associated with a multi-select question option, the corresponding REDCap field name is based on the
        survey question code (e.g. race_whatraceethnicity___pmi_prefernottoanswer or
        gender_genderidentity___pmi_prefernottoanswer).  Those REDCap fields contain a value of 1 if
        the PMI_PreferNotToAnswer option was selected, or 0 otherwise.

        When associated with a single select/radio button question, the field name is the question code string and
        the value is the display string from the RDR code table for the PMI_PreferNotToAnswer code

        :return:  key, value where key is the REDCap field name and value is based on the question code type
        """

        answer_code = 'PMI_PreferNotToAnswer'
        # If the question code is one of the parent codes to an option code menu, map the field
        if question_code in self.prefix_to_parent_code_map.values():
            return "___".join([question_code.lower(), answer_code.lower()]), 1
        else:
            return question_code.lower(), self.get_code_display_value(answer_code)

    def add_redcap_export_row(self, module, response_id, generated_redcap_dict):
        """
        Iterate over the generated REDCap field/value dict and add the data to the export.  If no values were
        found in the PDR response data for a given REDCap field, then the default value for that field will be
        extracted from the DEFAULT_REDCAP_TEMPLATE_RECORD for that module/REDCap field name
        :param module:  The module name (e.g., 'TheBasics')
        :param response_id: questionnaire_response_id (biquery_sync pk_id) of the survey response
        :param generated_redcap_dict: The resulting key/value pairs from the transformed PDR response data
        """

        if module not in self.DEFAULT_REDCAP_TEMPLATE_RECORD.keys():
            raise ValueError(f'No default REDCap template definition exists for module {module}')

        for row in self.redcap_export_rows:
            field_name = row[0]
            if field_name == 'record_id':
                row.append(response_id)
            elif field_name in generated_redcap_dict.keys():
                row.append(generated_redcap_dict[field_name])
                # Delete recognized keys from the generated REDCap data dict after processing.  Any dict entries left
                # after finishing this for loop is a "non-conformant" field name not in the REDCap data dictionary
                del generated_redcap_dict[field_name]
            elif field_name in self.DEFAULT_REDCAP_TEMPLATE_RECORD[module].keys():
                row.append(self.DEFAULT_REDCAP_TEMPLATE_RECORD[module][field_name])
            else:
                row.append(None)
            # Capture the cumulative number of records in the export data (in case we need to backfill for leftover/
            # non-conformant fields
            row_length = len(row)

        # Add a row to the export rows for any new non-conformant fields and backfill previously processed records
        for field_name in generated_redcap_dict.keys():
            # First column in the row is the field name, rest need to be backfilled with None
            backfill_values = [None] * (row_length - 1)

            backfill_values[-1] = generated_redcap_dict[field_name]
            self.redcap_export_rows.append([field_name, ] + backfill_values)

        return

    def find_parent_question_code(self, code):
        """
        Find parent question code for a given code.
        TODO:  Convert the search of the hardcoded prefix_to_parent_code_map to use the survey_question_option and
        survey_question data in the RDR instead.  Must have imported the module codebook(s) from REDCap first
        """
        for prefix in self.prefix_to_parent_code_map.keys():
            if code.startswith(prefix):
                return self.prefix_to_parent_code_map[prefix]

        return None

    def get_module_response_dict(self, module, response_id, ro_session):
        """
        Get a PDR module data record (prepped for BigQuery/PostgreSQL) for the specified questionnaire_response_id
        TODO:  If/when the PDR generators are decoupled from RDR, then convert this to query all the
        questionnaire_response_answer data for the response_id.  May mean extending the ResponseValidator class code?
        """
        if not module or not response_id:
            print('Need module name and a questionnaire_response_id')
            return {}
        elif ro_session:
            table_id = self.get_pdr_bq_table_id()
            results = ro_session.execute(('select resource from bigquery_sync '
                                          f'where table_id="{table_id}" and pk_id = {response_id}')).first()
            rsp = json.loads(results.resource)
            return rsp
        else:
            pass

        return {}

    def map_response_to_redcap_dict(self, module, response_id, response_dict):
        """

        """
        redcap_fields = OrderedDict()
        print(f'\n==================\n{response_id}\n==================')
        for col in self.parent_code_ordered_lists[module]:
            # PDR data can have comma-separated code strings for answers to multiselect questions
            answers = list(str(response_dict[col]).split(',')) if response_dict[col] else []

            if self.is_freetext_code(col) and len(answers):
                # PDR data has already mapped null/skipped free text fields to 0 if no text was entered,
                # or 1 if text was present
                if answers[0].isnumeric() and int(answers[0]):
                    redcap_fields[self.get_redcap_fieldname(col)] = '(redacted)'
                else:
                    redcap_fields[self.get_redcap_fieldname(col)] = answers[0]
            else:
                for answer in answers:
                    parent = self.find_parent_question_code(answer)
                    if answer == 'PMI_PreferNotToAnswer':
                        key, value = self.redcap_prefer_not_to_answer(col)
                        redcap_fields[key] = value
                    elif parent:
                        # Multi-select option sections (have a parent) get a 1 as their value
                        redcap_fields[self.get_redcap_fieldname(answer, parent)] = 1
                    # For single-select/radio button questions, map numeric strings to ints?
                    elif answer.isnumeric():
                        redcap_fields[self.get_redcap_fieldname(col)] = int(answer)
                    # REDCap doesn't have a display value when radio button/single select questions are skipped
                    elif answer != 'PMI_Skip':
                        redcap_fields[self.get_redcap_fieldname(col)] = self.get_code_display_value(answer)


        for key in redcap_fields:
            print(f'{key}:   {redcap_fields[key]}')
            if key == 'employmentworkaddress_zipcode' and redcap_fields[key] == 'PMI':
                print('debug')

        print('\n\n')
        self.add_redcap_export_row(module, response_id, redcap_fields)

    def execute(self):
        """ Run the survey-to-redcap export conversion tool """

        self.gcp_env.activate_sql_proxy(replica=True)
        dao = BigQuerySyncDao()
        module = 'TheBasics'
        self.set_bq_table(module)
        with dao.session() as session:
            # TODO:  Replace with a call to _get_response_id_list based on parameters passed
            response_list = [996438929, 224078445, 622747617, 100178505, 100179607, 100275014, 100892279, 101083117,
                           101394766, 101657560, 102311593, 102479428, 102699093, 100120910, 100278026, 100393283,
                           100419634, 100428180, 100788745, 100801123, 100802451, 100830407,100868630,
                           ]
            for rsp_id in response_list:
                rsp = self.get_module_response_dict(module, rsp_id, session)
                self.map_response_to_redcap_dict(module, rsp_id, rsp)

        return 0

def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = SurveyToRedCapConversion(args, gcp_env)
        exit_code = process.execute()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
