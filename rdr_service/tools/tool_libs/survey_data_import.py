import csv
from datetime import datetime
import io
import json
import random
from typing import Dict, List

from google.cloud.storage import Blob

from rdr_service import code_constants
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.services.system_utils import list_chunks
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase, logger


tool_cmd = 'survey-data-import'
tool_desc = 'Import a batch of CSV files provided by PPSC containing survey responses from participants'


SURVEY_CODE_MAP = {  # maps the number at the start of the file to the intended survey
    '2001': code_constants.THE_BASICS_PPI_MODULE,                       # TheBasics
    '2002': code_constants.PEDIATRICS_BASICS,                           # ped_basics
    '2003': code_constants.OVERALL_HEALTH_PPI_MODULE,                   # OverallHealth         (no validation)
    '2004': code_constants.PEDIATRICS_OVERALL_HEALTH,                   # ped_overall_health
    '2005': code_constants.LIFESTYLE_PPI_MODULE,                        # Lifestyle             (no validation)
    # 2006 is undefined
    '2007': code_constants.HEALTHCARE_ACCESS_MODULE,                    # HealthcareAccess      (no validation)
    '2008': code_constants.LIFE_FUNCTIONING_SURVEY,                     # lfs
    '2009': code_constants.REMOTE_PM_MODULE,                            # pm_height_weight
    '2010': code_constants.EMOTIONAL_HEALTH_MODULE,                     # ehhwb
    '2011': code_constants.BEHAVIORAL_HEALTH_MODULE,                    # bhp
    '2012': code_constants.PEDIATRICS_ENVIRONMENTAL_HEALTH,             # ped_environmental_health
    '2013': code_constants.SOCIAL_DETERMINANTS_OF_HEALTH_MODULE,        # sdoh
}

QUALTRICS_QUESTION_CODE_MAP = {
    # 2001
    'COUNTRYBORN': 'thebasics_countryborntextbox',
    'WHATRACEETHNICITY': 'race_whatraceethnicity',
    'RACEETHNICITY_NONE_1': 'whatraceethnicity_raceethnicitynoneofthese',
    'AIAN_FREETEXT': 'aiannoneofthesedescribeme_aianfreetext',
    'AS_FREETEXT': 'noneofthesedescribeme_asianfreetext',
    'B_FREETEXT': 'blacknoneofthesedescribeme_blackfreetext',
    'HISPANIC_SPECIFIC': 'hispanic_hispanicspecific',
    'H_FREETEXT': 'hispanicnoneofthesedescribeme_hispanicfreetext',
    'MENA_FREETEXT': 'menanoneofthesedescribeme_menafreetext',
    'NHPI_FREETEXT': 'nhpinoneofthesedescribeme_nhpifreetext',
    'W_FREETEXT': 'whitenoneofthesedescribeme_whitefreetext',
    'GENDERIDENTITY': 'gender_genderidentity',
    'GENDER_CLOSERDESCR': 'gender_closergenderdescription',
    'SPECGEN_TEXTBOX': 'specifiedgender_specifiedgendertextbox',
    'SEXATBIRTH': 'biologicalsexatbirth_sexatbirth',
    'SEXATBIRTH_TEXTBOX': 'sexatbirthnoneofthese_sexatbirthtextbox',
    'SEXUALORIENTATION': 'thebasics_sexualorientation',
    'SEXORIEN_CLOSERDESCR': 'genderidentity_sexualitycloserdescription',
    'SOMETHINGELSETEXTBOX': 'somethingelse_sexualitysomethingelsetextbox',
    'EDULVL_HIGHESTGRADE': 'educationlevel_highestgrade',
    'ACTIVEDUTYSERVSTAT': 'activeduty_avtivedutyservestatus',
    'CURRMARSTAT': 'maritalstatus_currentmaritalstatus',
    'LIVINGSIT_NUMBER': 'livingsituation_howmanypeople',
    'LIVINGSIT_UNDER18': 'livingsituation_peopleunder18',
    'HEALTHINSURANCE': 'insurance_healthinsurance',
    'HEALTHINSURANCE_TYPE': 'healthinsurance_insurancetypeupdate',
    'INSTYPE_OTHER_TEXT': 'otherhealthplan_freetext',
    'DISABILITY_CONCENTR': 'disability_difficultyconcentrating',
    'DISABILITY_WALKCLIMB': 'disability_walkingclimbing',
    'DISABILITY_DRESSBATH': 'disability_dressingbathing',
    'DISABILITY_ERRALONE': 'disability_errandsalone',
    'EMPLOYSTAT': 'employment_employmentstatus',
    'EMPLOYWORKADDRESS': 'employment_employmentworkaddress',
    'WORKADDRESS_1': 'employmentworkaddress_addresslineone',
    'WORKADDRESS_2': 'employmentworkaddress_addresslinetwo',
    'WORKADDRESS_CITY': 'employmentworkaddress_city',
    'WORKADDRESS_STATE': 'employmentworkaddress_state',
    'WORKADDRESS_ZIPCODE': 'employmentworkaddress_zipcode',
    'WORKADDRESS_COUNTRY': 'employmentworkaddress_country',
    'EMPLOYWORKADDRESS_RE': 'employmentworkaddress_prefernottoanswer',
    'CURRENTHOMEOWN': 'homeown_currenthomeown',
    'LIVINGSIT_CURRENT': 'livingsituation_currentliving',
    'LIVINGSIT_FREETEXT': 'livingsituation_livingsituationfreetext',
    'LIVINGSIT_YEAR': 'livingsituation_howmanylivingyears',
    'STABLEHOUSE_CONCERN': 'livingsituation_stablehouseconcern',
    'SOCIALSECURITY': 'socialsecurity_socialsecuritynumber',
    'SOCIALSECURITY_TEXT': 'socialsecurity_socialsecuritynumber_text',
    'SOCIALSECURITY_REF': 'socialsecurity_prefernottoanswer',
    # 2002
    'PEDS_HIGHESTGRADE': 'educationlevel_highestgrade',
    'PEDS_MARITALSTAT': 'maritalstatus_currentmaritalstatus',
    'PEDS_LIVINGNUM': 'livingsituation_howmanypeople',
    'PEDS_UNDER18': 'livingsituation_peopleunder18',
    'PEDS_EMPLOYSTAT': 'employment_employmentstatus',
    'PEDS_HOMEOWN': 'homeown_currenthomeown',
    'PEDS_LIVINGSIT': 'livingsituation_currentliving',
    'PEDS_LIVINGSIT_FREE': 'livingsituation_livingsituationfreetext',
    'PEDS_LIVINGYRS': 'livingsituation_howmanylivingyears',
    'PEDS_BIRTHPLACE': 'thebasics_birthplace_ped',
    'PEDS_COUNTRYBORN': 'thebasics_countryborntextbox_ped',
    'PEDS_WHATRACEETH': 'race_whatraceethnicity_ped',
    'PEDS_RACEETH_NONE_1': 'whatraceethnicity_raceethnicitynoneofthese_ped',
    'PEDS_AIANSPEC': 'aian_aianspecific_ped',
    'PEDS_AIAN_FREETEXT': 'aiannoneofthesedescribeme_aianfreetext_ped',
    'PEDS_ASIANSPECIFIC': 'asian_asianspecific_ped',
    'PEDS_ASIAN_FREETEXT': 'noneofthesedescribeme_asianfreetext_ped',
    'PEDS_BLFREETEXT': 'blacknoneofthesedescribeme_blackfreetext_ped',
    'PEDS_HFREETEXT': 'hispanicnoneofthesedescribeme_hispanicfreetext_ped',
    'PEDS_MENAFREE': 'menanoneofthesedescribeme_menafreetext_ped',
    'PEDS_NHPIFREE': 'nhpinoneofthesedescribeme_nhpifreetext_ped',
    'PEDS_WHFREETEXT': 'whitenoneofthesedescribeme_whitefreetext_ped',
    'PEDS_SEXATBIRTH': 'biologicalsexatbirth_sexatbirth_ped',
    'PEDS_SEXATBIRTH_TEXT': 'sexatbirthnoneofthese_sexatbirthtextbox_ped',
    'PEDS_GESTAGE_FREE': 'echo_55_gestational_age',
    'PEDS_HEALTHINSURANCE': 'insurance_healthinsurance_ped',
    'PEDS_HEALTHADVICE': 'healthadvice_placeforhealthadvice_ped',
    'PEDS_WHATHEALTHADVICE': 'healthadvice_whatkindofplace_ped',
    'PEDS_SKIPMED': 'cantaffordcare_skippedmedtosavemoney_ped',
    'PEDS_LESSMED': 'cantaffordcare_tooklessmedtosavemoney_ped',
    'PEDS_PHARMRX': 'cantaffordcare_pharmacy_ped',
    'PEDS_LOWCOSTRX': 'cantaffordcare_lowercostrxtosavemoney',
    'PEDS_COUNTRYRX': 'cantaffordcare_boughtrxfromothercountry',
    'PEDS_ALTTHERAPY': 'cantaffordcare_alternativetherapies',
    'PEDS_CONDITION12': 'nsch_1_condition_12months',
    'PEDS_MOREMEDS_CON12': 'nsch_2_condition_12months',
    'PEDS_LIMITED_CON12': 'nsch_3_condition_12months',
    'PEDS_THERAPY_CON12': 'nsch_4_condition_12months',
    'NSCH_5_CONDITION': 'nsch_5_12months',
    'BLACK_BLACKSPEC_PED': 'black_blackspecific_ped',
    'HISPANIC_SPEC_PED': 'hispanic_hispanicspecific_ped',
    'MENA_MENASPEC_PED': 'mena_menaspecific_ped',
    'NHPI_NHPISPEC_PED': 'nhpi_nhpispecific_ped',
    'WHITE_WHITESPEC_PED': 'white_whitespecific_ped',
    'DELAYEDMEDCARE_PED': 'delayedmedicalcare_ped',
    'PEDS_DELAYRACEREL': 'healthproviderracereligion_delayedornocare_ped',
    'PEDS_DELAYRX': 'cantaffordcare_delayedfillingrxtosavemoney_ped',
    # 2003
    'OH_MEDFORMCONF': 'overallhealth_medicalformconfidence',
    'OH_HLTHMATASST': 'overallhealth_healthmaterialassistance',
    'OH_DIFFUNDINFO': 'overallhealth_difficultunderstandinfo',
    'OH_GENHEALTH': 'overallhealth_generalhealth',
    'OH_GENQUAL': 'overallhealth_generalquality',
    'OH_GENPHYSHLTH': 'overallhealth_generalphysicalhealth',
    'OH_GENMNTLHLTH': 'overallhealth_generalmentalhealth',
    'OH_SOCSAT': 'overallhealth_socialsatisfaction',
    'OH_EVDAYACT': 'overallhealth_everydayactivities',
    'OH_AVGPAIN7': 'overallhealth_averagepain7days',
    'OH_AVGFTG7': 'overallhealth_averagefatigue7days',
    'OH_GENSOC': 'overallhealth_generalsocial',
    'OH_EMOTNLPRBLMS7': 'overallhealth_emotionalproblem7days',
    'OH_MENSTRUALSTOPPED': 'overallhealth_menstrualstopped',
    'OH_PGNCYSTATUS': 'pregnancy_1pregnancystatus',
    'OH_MENSTRUALSTOPRSN': 'yesnone_menstrualstoppedreason',
    'OH_HYSTRCTMYHSTRY': 'overallhealth_hysterectomyhistory',
    'OH_HYSTRCTMYHSTRYAGE': 'overallhealth_hysterectomyhistoryage',
    'OH_OVAREMOVHIST': 'overallhealth_ovaryremovalhistory',
    'OH_OVAREMOVHISTAGE': 'overallhealthovaryremovalhistoryage',
    'OH_ORGNTRNSPLNT': 'overallhealth_organtransplant',
    'OH_ORGNTRNSPLNTDESC': 'organtransplant_organtransplantdescription',
    'OH_OTHERORGAN': 'otherorgan_freetextbox',
    'OH_OTHERTISSUE': 'othertissue_freetextbox',
    'OH_HRTTRSPLNTDATE': 'organtransplant_hearttransplantdate',
    'OH_KDNYTRSPLNTDATE': 'organtransplant_kidneytransplantdate',
    'OH_LVRTRSPLNTDATE': 'organtransplant_livertransplantdate',
    'OH_LNGTRSPLNTDATE': 'organtransplant_lungtransplantdate',
    'OH_PNCRSTRSPLNTDATE': 'organtransplant_pancreastransplantdate',
    'OH_INTSTNTRSPLNTDATE': 'organtransplant_intestinetransplantdate',
    'OH_OTHORGTRSPLNTDATE': 'organtransplant_otherorgantransplantdate',
    'OH_CRNTRSPLNTDATE': 'organtransplant_corneatransplantdate',
    'OH_BNTRSPLNTDATE': 'organtransplant_bonetransplantdate',
    'OH_VLVTRSPLNTDATE': 'organtransplant_valvetransplantdate',
    'OH_SKNTRSPLNTDATE': 'organtransplant_skintransplantdate',
    'OH_BVTRSPLNTDATE': 'organtransplant_bloodvesseltransplantdate',
    'OH_OTHTISTRSPLNTDATE': 'organtransplant_othertissuetransplantdate',
    'OH_OUTTRAVEL6MO': 'overallhealth_outsidetravel6month',
    'OH_OUTTRAVEL6WHERE': 'outsidetravel6month_outsidetravel6monthwheretraveled',
    'OH_TRAVEL6HOWLONG': 'outsidetravel6month_outsidetravel6monthhowlong',
    # 2004
    'OHP_GENHEALTH': 'overallhealth_generalhealth_ped',
    'OHP_GENQUALITY': 'overallhealth_generalquality_ped',
    'OHP_GENPHYSHEALTH': 'overallhealth_generalphysicalhealth_ped',
    'OHP_GENMENTALHEALTH': 'overallhealth_generalmentalhealth_ped',
    # 2005
    '100CIGSLIFETIME': 'smoking_100cigslifetime',
    'SMOKEFREQUENCY': 'smoking_smokefrequency',
    'SMOKESTARTAGE_1_TEXT': 'smoking_dailysmokestartingagenumber',
    'SMOKESTARTAGE': 'smoking_dailysmokestartingage',
    'SMOKEQUITATTEMPT': 'smoking_seriousquitattempt',
    'ATTEMPTQUIT_1_TEXT': 'attemptquitsmoking_completelyquitage',
    'ATTEMPTQUIT': 'attemptquitsmoking_completelyquit',
    'SMOKEYEARS_1_TEXT': 'smoking_numberofyearsnumber',
    'SMOKEYEARS': 'smoking_numberofyears',
    'SMOKECURRDAILY_1_TEXT': 'smoking_currentdailycigarettenumber',
    'SMOKECURRDAILY': 'smoking_currentdailycigarette',
    'SMOKEAVGDAILY_1_TEXT': 'smoking_averagedailycigarettenumber',
    'SMOKEAVGDAILY': 'smoking_averagedailycigarette',
    'ELECTRICSMOKE': 'electronicsmoking_electricsmokeparticipant',
    'ELECTRICSMOKE_FREQ': 'electronicsmoking_electricsmokefrequency',
    'CIGARSMOKE': 'cigarsmoking_cigarsmokeparticipant',
    'CIGARSMOKE_FREQ': 'cigarsmoking_currentcigarfrequency',
    'HOOKAHSMOKE': 'hookahsmoking_hookahsmokeparticipant',
    'HOOKAHSMOKE_FREQ': 'hookahsmoking_currenthookahfrequency',
    'TOBSMOKELESS': 'smokelesstobacco_smokelesstobaccoparticipant',
    'TOBSMOKELESS_FREQ': 'smokelesstobacco_smokelesstobaccofrequency',
    'ALCDRINK': 'alcohol_alcoholparticipant',
    'ALCDRINK_FREQ': 'alcohol_drinkfrequencypastyear',
    'ALCDRINK_AVG': 'alcohol_averagedailydrinkcount',
    'ALCDRINK_SIXORMORE': 'alcohol_6ormoredrinksoccurence',
    'RECDRUGUSED': 'recreationaldruguse_whichdrugsused',
    'OTHERDRUGS': 'otherspecify_otherdrugstextbox',
    '3MONTH_MARIJUANA': 'past3monthusefrequency_marijuana3monthuse',
    '3MONTH_COCAINE': 'past3monthusefrequency_cocaine3monthuse',
    '3MONTH_RXSTIMULANT': 'past3monthusefrequency_prescriptionstimulant3monthuse',
    '3MONTH_OTHSTIMULANT': 'past3monthusefrequency_otherstimulant3monthuse',
    '3MONTH_INHALANT': 'past3monthusefrequency_inhalant3monthuse',
    '3MONTH_SEDATIVE': 'past3monthusefrequency_sedative3monthuse',
    '3MONTH_HALLUCINOGEN': 'past3monthusefrequency_hallucinogen3monthuse',
    '3MONTH_STREETOPIOID': 'past3monthusefrequency_streetopioid3monthuse',
    '3MONTH_RXOPIOID': 'past3monthusefrequency_prescriptionopiod3monthuse',
    '3MONTH_OTHERDRUG': 'past3monthusefrequency_other3monthuse',
    # 2007
    'HCAU_INSURANCE': 'insurance_insuranceaccepted',
    'HCAU_COVERAGE': 'insurance_healthcarecoverage',
    'HCAU_HEALTHADVICE': 'healthadvice_placeforhealthadvice',
    'HCAU_ADVICEPLACE': 'healthadvice_whatkindofplace',
    'HCAU_PROFESSIONAL': 'healthadvice_spokentoprofessional',
    'HCAU_DOCTOR': 'healthadvice_spokentogeneraldoctor',
    'HCAU_NP': 'healthadvice_spokentonursepractitioner',
    'HCAU_OBGYN': 'healthadvice_spokentoobgyn',
    'HCAU_MNTHLTH': 'healthadvice_spokentomentalhealthprofessional',
    'HCAU_EYEDOC': 'healthadvice_spokentoeyedoctor',
    'HCAU_PODIATRIST': 'healthadvice_spokentopodiatrist',
    'HCAU_CHIRO': 'healthadvice_spokentochiropractor',
    'HCAU_PT': 'healthadvice_spokentophysicaltherapist',
    'HCAU_DENTIST': 'healthadvice_spokentodentist',
    'HCAU_MEDSPEC': 'healthadvice_spokentomedicalspecialist',
    'HCAU_TRADHEALER': 'healthadvice_spokentotraditionalhealer',
    'HCAU_DOCVISITS': 'healthadvice_generaldoctorvisits',
    'HCAU_NPVISITS': 'healthadvice_nursepractitionervisits',
    'HCAU_OBGYNVISITS': 'healthadvice_obgynvisits',
    'HCAU_MNTHLTHVISITS': 'healthadvice_mentalhealthprofessionalvisits',
    'HCAU_EYEDOCVISITS': 'healthadvice_eyedoctorvisits',
    'HCAU_PODIATRISTVIS': 'healthadvice_podiatristvisits',
    'HCAU_CHIROVISITS': 'healthadvice_chiropractorvisits',
    'HCAU_PTVISITS': 'healthadvice_physicaltherapistvisits',
    'HCAU_DENTVISITS': 'healthadvice_dentistvisits',
    'HCAU_MEDSPECVIS': 'healthadvice_medicalspecialistvisits',
    'HCAU_TRADHEALERVIS': 'healthadvice_traditionalhealervisits',
    'HCAU_PROVRESPECT': 'healthadvice_respectedbyprovider',
    'HCAU_OPINION': 'healthadvice_askedforopinion',
    'HCAU_UNDERSTAND': 'healthadvice_easeofunderstanding',
    'HCAU_DELAYTRANSP': 'delayedmedicalcare_transportation',
    'HCAU_DELAYRURAL': 'delayedmedicalcare_ruralarea',
    'HCAU_DELAYNERV': 'delayedmedicalcare_nervous',
    'HCAU_TIMEOFF': 'delayedmedicalcare_timeoffwork',
    'HCAU_CHILDCARE': 'delayedmedicalcare_childcare',
    'HCAU_ELDERLYCARE': 'delayedmedicalcare_elderlycare',
    'HCAU_COPAY': 'delayedmedicalcare_cantaffordcopay',
    'HCAU_DEDUCTIBLE': 'delayedmedicalcare_deductibletoohigh',
    'HCAU_OOP': 'delayedmedicalcare_hadtopayoutofpocket',
    'HCAU_DELAYOTHER_FREE': 'otherdelayedmedicalcare_freetext',
    'HCAU_PRESMEDS': 'cantaffordcare_prescriptionmedicines',
    'HCAU_COUNSELING': 'cantaffordcare_mentalhealthcounseling',
    'HCAU_EMCARE': 'cantaffordcare_emergencycare',
    'HCAU_DENTAL': 'cantaffordcare_dentalcare',
    'HCAU_EYECARE': 'cantaffordcare_eyeglasses',
    'HCAU_HCP': 'cantaffordcare_healthcareprovider',
    'HCAU_SPECIALIST': 'cantaffordcare_specialist',
    'HCAU_FOLLOWUP': 'cantaffordcare_followupcare',
    'HCAU_PAYWORRY': 'cantaffordcare_worriedaboutpaying',
    'HCAU_SKIPMEDS': 'cantaffordcare_skippedmedtosavemoney',
    'HCAU_TOOKLESSMEDS': 'cantaffordcare_tooklessmedtosavemoney',
    'HCAU_DELAYRXFILL': 'cantaffordcare_delayedfillingrxtosavemoney',
    'HCAU_LOWCOSTRX': 'cantaffordcare_lowercostrxtosavemoney',
    'HCAU_OTHERCOUNTRYRX': 'cantaffordcare_boughtrxfromothercountry',
    'HCAU_ALTTHERAPY': 'cantaffordcare_alternativetherapies',
    'HCAU_RELIMPORTANT': 'healthproviderracereligion_howimportant',
    'HCAU_RELOFTEN': 'healthproviderracereligion_howoften',
    'HCAU_RELDELAYED': 'healthproviderracereligion_delayedornocare',
    # 2008
    'LFS_CONCENTRATE': 'disability_difficultyconcentrating',
    'LFS_DISAB_WALKCLIMB': 'disability_walkingclimbing',
    'LFS_DISAB_DRESSBATH': 'disability_dressingbathing',
    'LFS_DISAB_ERRANDS': 'disability_errandsalone',
    # 2009
    'SELFREPORT_HTFT': 'self_reported_height_ft',
    'SELFREPORT_HTIN': 'self_reported_height_in',
    'SELFREPORT_HTCM': 'self_reported_height_cm',
    'SELFREPORT_WTLB': 'self_reported_weight_pounds',
    'SELFREPORT_WTKG': 'self_reported_weight_kg',
    # 2010
    'MHQUKB_31_AMITRIP': 'mhqukb_31_amitriptyline',
    # 2011
    #   note: all question codes match redcap definitions
    # 2012
    'PEDS_EE_LIVCOUNTRY': 'echo_2_country',
    'ECHO_2_CITY': 'echo_2_city',
    'PEDS_EE_LIVINGCITY': 'echo_2_city',
    'ECHO_52_HOME_FREQ': 'echo_52_home_frequency',
    'PEDS_EE_CIGS_DAY': 'echo_52_home_frequency_day',
    'PEDS_EE_CIGS_WEEK': 'echo_52_home_frequency_week',
    'PEDS_EE_CIGS_MONTH': 'echo_52_home_frequency_month',
    'ECHO_53_PROD_OTHER': 'echo_53_products_other',
    'ECHO_53_PROD_2_OTHER': 'echo_53_products_2_other',
    'ECHO_52_PRODUCTS_3': 'echo_52_home_products_3',
    'ECHO_52_PROD_3DAY': 'echo_52_home_products_3_day',
    'ECHO_52_PROD_3WEEK': 'echo_52_home_products_3_week',
    'ECHO_52_PROD_3MONTH': 'echo_52_home_products_3_month',
    'ECHO_53_PROD_4_OTHER': 'echo_53_products_4_other',
    'ECHO_52_HOME_PROD_4': 'echo_52_home_products_4',
    'ECHO_53_PROD_4DAY': 'echo_52_home_products_4_day',
    'ECHO_53_PROD_4WEEK': 'echo_52_home_products_4_week',
    'ECHO_53_PROD_4MONTH': 'echo_52_home_products_4_month',
    # 2013
    'SDOH_EDS_FOLLOW_UP_X': 'sdoh_eds_follow_up_1_xx'
}

NON_QUESTION_COLUMNS = {
    'StartDate',
    'EndDate',
    'Status',
    'IPAddress',
    'Progress',
    'Duration (in seconds)',
    'Finished',
    'RecordedDate',
    'ResponseId',
    'RecipientLastName',
    'RecipientFirstName',
    'RecipientEmail',
    'ExternalReference',
    'LocationLatitude',
    'LocationLongitude',
    'DistributionChannel',
    'UserLanguage',
    'LastModifiedDate',
    'META_INFO_Browser',
    'META_INFO_Version',
    'META_INFO_Operating System',
    'META_INFO_Resolution',
    'VERSION',
    'GEOPOSTAL',
    'GEOCITY',
    'GEOREGION',
    'GEOCOUNTRYNAME',
    'GEOCOUNTRYCODE',
    'ENV_RUN',
    'S3URL',
    'Q_CHL',
    'P_CONTACTID',
    'NORCINDEXID',
    'P_ID',
    'Q_URL',
    'P_DOB',
    'SHOW_REPORT'
}


class ResponseFileParser:
    def __init__(self, file_blob: Blob):
        file_data = io.StringIO(file_blob.download_as_string().decode('utf8'))
        self.reader = csv.DictReader(file_data)
        self._question_code_column_map: Dict[str, str] = {}
        self.question_codes = self._get_question_codes()
        self.blob = file_blob

    def _get_question_codes(self) -> List[str]:
        question_column_names = [
            name
            for name in self.reader.fieldnames
            if name not in NON_QUESTION_COLUMNS and not name.startswith('__js_')
        ]

        result = []
        for name in question_column_names:
            mapped_name = name.lower()
            if name in QUALTRICS_QUESTION_CODE_MAP:
                mapped_name = QUALTRICS_QUESTION_CODE_MAP[name].lower()

            result.append(mapped_name)
            self._question_code_column_map[mapped_name] = name
        return result

    def get_module_name(self) -> str:
        file_name = self.blob.name.split('/')[-1]
        module_identifier = file_name[:4]
        return SURVEY_CODE_MAP[module_identifier].lower()

    def generate_responses(self, questionnaire_proxy: 'QuestionnaireProxy') -> List[QuestionnaireResponse]:
        result = []
        for row in self.reader:
            response = QuestionnaireResponse(
                questionnaireId=questionnaire_proxy.questionnaire_id,
                questionnaireVersion=questionnaire_proxy.version_number,
                participantId=row['P_ID'][1:],
                authored=row['EndDate'],
                resource=json.dumps(row),
                externalId=row['ResponseId'],
                created=datetime.now()
            )
            for question_code in questionnaire_proxy.question_map:
                if question_code not in self._question_code_column_map:
                    continue

                column_name = self._question_code_column_map[question_code]
                if column_name in row:
                    answer_str = row[column_name]
                    if answer_str == "":
                        continue

                    answer = QuestionnaireResponseAnswer(
                        questionId=questionnaire_proxy.question_map[question_code]
                    )
                    answer.valueString = answer_str
                    response.answers.append(answer)
            result.append(response)
        return result


class QuestionnaireProxy:
    def __init__(self, module_code_id, session):
        query = session.query(
            QuestionnaireConcept.questionnaireId,
            QuestionnaireConcept.questionnaireVersion
        ).filter(
            QuestionnaireConcept.codeId == module_code_id
        ).order_by(
            QuestionnaireConcept.questionnaireId.desc(),
            QuestionnaireConcept.questionnaireVersion.desc()
        )
        concept: QuestionnaireConcept = query.first()

        self.questionnaire_id = concept.questionnaireId
        self.version_number = concept.questionnaireVersion

        question_query = session.query(
            QuestionnaireQuestion.questionnaireQuestionId,
            Code.value
        ).join(
            Code,
            QuestionnaireQuestion.codeId == Code.codeId
        ).filter(
            QuestionnaireQuestion.questionnaireId == self.questionnaire_id,
            QuestionnaireQuestion.questionnaireVersion == self.version_number
        )
        question_list = question_query.all()

        self.question_map = {
            code_str.lower(): question_id
            for question_id, code_str in question_list
        }


class SurveyDataImport(ToolBase):
    def run(self):
        super().run()

        with self.get_session() as session:
            db_codes = self._get_db_codes(session)
            for blob in self._get_response_blobs():
                print('--------------------\n')
                print(blob.name)
                self._process_response_blob(blob, db_codes, session)

    def _get_response_blobs(self) -> List[Blob]:
        directory_path = self.args.path
        path_parts = directory_path.split('/')
        storage_provider = GoogleCloudStorageProvider()

        # todo: load all of the code values from the db (into a dict with keys as lowered values and values as code_id)

        results = []
        for blob in storage_provider.list(bucket_name=path_parts[0], prefix='/'.join(path_parts[1:])):
            if blob.name.endswith('_data.csv'):
                results.append(blob)

        return results

    @classmethod
    def _process_response_blob(cls, blob: Blob, db_codes: Dict[str, Code], session):
        parser = ResponseFileParser(blob)
        logger.info(f'Module name: {parser.get_module_name()}\n')
        logger.info(f'Question codes:\n{sorted(parser.question_codes)}\n')

        unregonized_question_codes = []
        recognized_question_codes = []
        for question_code in parser.question_codes:
            if question_code not in db_codes:
                unregonized_question_codes.append(question_code)
            else:
                recognized_question_codes.append(question_code)
        if unregonized_question_codes:
            logger.warning(f'Unrecognized question codes:\n{", ".join(sorted(unregonized_question_codes))}\n')

        module_code = db_codes[parser.get_module_name()]
        questionnaire = QuestionnaireProxy(module_code.codeId, session)

        extra_codes = []
        for question_code in recognized_question_codes:
            if question_code not in questionnaire.question_map:
                extra_codes.append(question_code)
        if extra_codes:
            logger.warning(f'Question codes not found in module definition:\n{", ".join(sorted(extra_codes))}\n')

        missing_codes = []
        for question_code in questionnaire.question_map:
            if question_code not in recognized_question_codes:
                missing_codes.append(question_code)
        if missing_codes:
            logger.warning(f'Question codes not found in data file:\n{", ".join(sorted(missing_codes))}\n')

        responses = parser.generate_responses(questionnaire)
        response_count = len(responses)
        logger.info(f'\nfound {response_count} responses')
        logger.info(f'generating ids...')
        response_ids = cls._generate_questionnaire_response_ids(response_count, session)
        for index, response in enumerate(responses):
            response.questionnaireResponseId = response_ids[index]

        logger.info(f'saving responses...')
        count = 0
        for subset in list_chunks(responses, 20):
            response_answer_map: Dict[QuestionnaireResponse, List[QuestionnaireResponseAnswer]] = {}
            for response in subset:
                response_answer_map[response] = response.answers
                response.answers = []

            session.add_all(subset)
            session.flush()

            answer_value_strings = []
            for response, answer_list in response_answer_map.items():
                for answer in answer_list:
                    answer_value_strings.append(
                        f"({response.questionnaireResponseId}, {answer.questionId}, '{answer.valueString}')"
                    )

            batch_ans_sql = """
                insert into questionnaire_response_answer (
                    questionnaire_response_id, question_id, value_string
                ) values
            """ + ', '.join(answer_value_strings)
            session.execute(batch_ans_sql)

            session.flush()
            count += len(subset)
            print(f"Flushed {count} of {response_count}                     ", end="\r", flush=True)

        print()
        session.commit()

    @classmethod
    def _get_db_codes(cls, session) -> Dict[str, Code]:
        codes = session.query(
            Code.codeId,
            Code.value
        ).all()
        return {
            code.value.lower(): code
            for code in codes
        }

    @classmethod
    def _generate_questionnaire_response_ids(cls, count, session) -> List[int]:
        result = []
        batch_size = 2000

        while len(result) < count:
            ids_to_check = [
                random.randrange(100_000_000, 999_999_999)
                for _i in range(batch_size)
            ]

            sequence_select_str = ' union all '.join([
                f'select {id_val} id' for id_val in ids_to_check
            ])
            query = f"""
                select *
                from ({sequence_select_str}) possible_ids
                where id not in (
                    select qr.questionnaire_response_id
                    from questionnaire_response qr
                )
                ;
            """
            query_result = session.execute(query)
            for value in query_result:
                val_not_in_db = value[0]
                if val_not_in_db not in result:
                    result.append(value[0])

        return result


def add_additional_arguments(parser):
    parser.add_argument(
        '--path',
        required=True,
        help="Directory containing the CSV files to import (it'll have a bunch "
             "of folders named a number from 2001 to 2013)"
    )


def run():
    return cli_run(tool_cmd, tool_desc, SurveyDataImport, add_additional_arguments)
