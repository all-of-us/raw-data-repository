#
# Fake questionnaire data generator.
#

# pylint: disable=superfluous-parens
import copy
import datetime
import logging
import random
import string
import time

import clock
from data_gen.generators.base_gen import BaseGen
from dateutil.parser import parse
from lib_fhir.fhirclient_1_0_6.models.fhirdate import FHIRDate
from lib_fhir.fhirclient_1_0_6.models.fhirreference import FHIRReference
from lib_fhir.fhirclient_1_0_6.models.questionnaire import Questionnaire
from lib_fhir.fhirclient_1_0_6.models.questionnaireresponse import QuestionnaireResponse, \
  QuestionnaireResponseGroup, QuestionnaireResponseGroupQuestion, \
  QuestionnaireResponseGroupQuestionAnswer
from services.system_utils import make_api_request
from services.gcp_utils import gcp_make_auth_header

_logger = logging.getLogger('rdr_logger')


class QuestionnaireGen(BaseGen):
  """
  fake questionnaire response data generator.
  """
  _rdr_host = 'localhost'

  _github_host = 'raw.githubusercontent.com'
  _github_url = 'all-of-us-terminology/api-payloads/master/questionnaire_payloads'
  _gen_url = 'rdr/v1/SpecDataGen'
  _codebook = None  # stores the CodeBook object

  _module = None
  _module_codebook = None
  _module_questions = None

  _module_data = {
    'ConsentPII': {'file': 'base_consent.json'},
    'TheBasics': {'file': 'basics.json'},
    'EHRConsentPII': {'file': 'ehr_consent.json'},
    'Lifestyle': {'file': 'lifestyle.json'},
    'OverallHealth': {'file': 'overall_health.json'}
  }
  # https://www.hl7.org/fhir/valueset-item-type.html
  _answer_types = ['boolean', 'decimal', 'integer', 'date', 'dateTime', 'time', 'string',
                   'text', 'url', 'choice', 'quantity']

  _participant_id = None
  _overrides = None
  _code_id = None

  def __init__(self, codebook, rdr_host=None):
    """
    Initialize the QuestionnaireGen object. Try to re-use to save on loading files.
    :param codebook: CodeBook object
    :param rdr_host: rdr service host
    """
    super(QuestionnaireGen, self).__init__()

    if rdr_host:
      self._rdr_host = rdr_host
    if not codebook:
      _logger.error('invalid codebook parameter.')

    self._codebook = codebook

  def new(self, module, participant_id, overrides=None):
    """
    Return a new QuestionnaireGen object with the assigned data.
    :param module: module name
    :param participant_id: participant id
    :param overrides: a dict of question id and answers
    :return: QuestionnaireGen object
    """
    if not module or not isinstance(module, str):
      _logger.error('invalid module name parameter')
      return None

    if not self._load_module(module):
      return None

    clone = self.__class__(self._codebook)
    clone._module = module
    clone._participant_id = participant_id
    clone._overrides = overrides
    # shallow copy some class objects so the the parent is updated
    clone._module_data = copy.copy(self._module_data)
    clone._module_questions = copy.copy(self._module_questions)
    clone._app_data = copy.copy(self._app_data)

    return clone

  def _load_module(self, module):
    """
    Load and prepare object for this module
    :param module: module name
    :return: True if module is loaded or False if not.
    """
    # Only reset if we need to.
    if self._module == module:
      return True

    self._module = module
    self._module_codebook = self._codebook.get_concept(module)
    self._module_questions = None
    retry = 5

    while retry > 0:
      self._module_questions = self._download_questions(module)
      if self._module_questions:
        break
      retry -= 1
      time.sleep(2)

    if not self._module_questions:
      _logger.error('failed to retrieve module questions.')
      return False
    # Load the questionnaire question codes into the module
    if 'questions' not in self._module_data[module]:
      self._module_data[module]['questions'] = self._get_module_question_codes()

    return True

  def make_fhir_document(self):
    """
    Create a FHIR questionnaire response object, populate it with answers and return it.
    :return: fhir questionnaire response object
    """
    if not self._module_questions:
      raise NotImplementedError('only objects created with new() method may call this method.')

    q_resp = QuestionnaireResponse()
    q_resp.authored = FHIRDate(clock.CLOCK.now().isoformat())
    q_resp.status = 'completed'
    q_resp.subject = FHIRReference({'reference': 'Patient/{0}'.format(self._participant_id)})
    q_resp.questionnaire = FHIRReference({'reference':
      'Questionnaire/{0}/_history/{1}'.format(self._module_questions.id, self._module_questions.version)})

    qnans = list()

    # additional answers for specific modules
    if self._module == 'ConsentPII':
      qnans += self._answers_for_consentpii()
    if self._module == 'TheBasics':
      qnans += self._answers_for_the_basics()
    if self._module == 'EHRConsentPII':
      qnans += self._answers_for_ehr_consentpii()
    if self._module == 'Lifestyle':
      qnans += self._answers_for_lifestyle()
    if self._module == 'OverallHealth':
      qnans += self._answers_for_overall_health()

    # remove any invalid answers
    qnans = [x for x in qnans if x]

    # Handle question overrides
    if self._overrides and isinstance(self._overrides, list):
      qnans = self._apply_overrides(qnans, self._overrides)
    if len(qnans) == 0:
      return None

    q_group = QuestionnaireResponseGroup()
    q_group.question = qnans
    q_resp.group = q_group
    response = q_resp.as_json()
    # json_str = json.dumps(data)

    return response

  def _apply_overrides(self, qnans, overrides):
    """
    Apply any overrides to the final
    :param qnans: list of QuestionnaireResponseGroupQuestion objects
    :param overrides: list of tuples containing question code and answer
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    for (qn, an) in overrides:
      # check to make sure question code is in this module.
      if qn not in self._module_data[self._module]['questions']:
        continue
      question = self._find_question(qn)
      qnan = self._answer_question(question, an)

      # look for existing question in list and remove it.
      index = -1
      for x in range(0, len(qnans)):
        if qnans[x].linkId == qnan.linkId:
          index = x
          break

      if index > -1:
        del qnans[index]
      qnans.append(qnan)

    return qnans

  def _make_contact_info(self, state_question=None):
    """
    Make first name, middle name, last name, phone number and email
    :param state_question: question object to use for looking up state ids
    :return: dict of values
    """
    data = dict()
    data['first'] = random.choice(self._app_data['first_names'])
    data['middle'] = random.choice(self._app_data['middle_names'])
    data['last'] = random.choice(self._app_data['last_names'])
    data['email'] = '{0}.{1}@fakeexample.com'.format(data['first'], data['last'])
    data['phone'] = '{0:03}-{1:03}-{2:04}'.format(random.randint(100, 999), random.randint(0, 999),
                                                  random.randint(0, 9999))
    street_info = random.choice(self._app_data['street_names']).split('|')
    data['street1'] = street_info[0]
    if len(street_info) > 1:
      data['street2'] = street_info[1]
    data['city'] = random.choice(self._app_data['city_names'])
    data['zip'], data['state'] = random.choice(self._app_data['zipcodes']).split(',')

    # attempt to look up the state choice value
    if state_question:
      states = self._get_question_choices(state_question)
      for state in states:
        if state.endswith(data['state']):
          data['state'] = state
          break

    # Find a birth date between the 105 years and 12 years old.
    days_sub = random.randint((12 * 365), (105 * 365))
    data['dob'] = (clock.CLOCK.now() - datetime.timedelta(days=days_sub)).date()

    return data

  def _weighted_choice(self, choices):
    """
    Return a choice from a weighted set of choices.
    https://stackoverflow.com/questions/3679694/a-weighted-version-of-random-choice
    :param choices: a list of tuples containing choice and weight
    :return: weighted choice
    """
    total = sum(w for c, w in choices)
    r = random.uniform(0, total)
    upto = 0
    for c, w in choices:
      if upto + w >= r:
        return c
      upto += w
    assert False, "Shouldn't get here"

  def _answers_for_consentpii(self):
    """
    Create answers for ConsentPII module.
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    qnans = list()

    contact = self._make_contact_info(self._find_question('StreetAddress_PIIState'))
    qnans.append(
      self._answer_question(self._find_question('PIIName_First'), contact['first']))
    qnans.append(
      self._answer_question(self._find_question('PIIName_Middle'), contact['middle']))
    qnans.append(
      self._answer_question(self._find_question('PIIName_Last'), contact['last']))
    qnans.append(
      self._answer_question(self._find_question('ConsentPII_EmailAddress'), contact['email']))
    qnans.append(
      self._answer_question(self._find_question('PIIContactInformation_Email'), contact['email']))
    qnans.append(
      self._answer_question(self._find_question('PIIContactInformation_Phone'), contact['phone']))
    qnans.append(
      self._answer_question(self._find_question('ConsentPII_VerifiedPrimaryPhoneNumber'),
                            contact['phone']))
    qnans.append(
      self._answer_question(self._find_question('PIIAddress_StreetAddress'), contact['street1']))
    qnans.append(
      self._answer_question(self._find_question('PIIAddress_StreetAddress2'), contact['street1']))
    qnans.append(
      self._answer_question(self._find_question('StreetAddress_PIICity'), contact['city']))
    qnans.append(
      self._answer_question(self._find_question('StreetAddress_PIIState'), contact['state']))
    qnans.append(
      self._answer_question(self._find_question('StateOfResidence'), contact['state']))
    qnans.append(
      self._answer_question(self._find_question('ReceiveCare_PIIState'), contact['state']))
    qnans.append(
      self._answer_question(self._find_question('StreetAddress_PIIZIP'), contact['zip']))
    qnans.append(
      self._answer_question(self._find_question('PIIBirthInformation_BirthDate'),
                            contact['dob'].isoformat()))
    qnans.append(
      self._answer_question(self._find_question('ExtraConsent_WelcomeVideo'), clock.CLOCK.now()))
    qnans.append(
      self._answer_question(self._find_question('ExtraConsent_AgreeToConsent'), clock.CLOCK.now()))

    # q = self._find_question('Language')
    # q = self._find_question('Language_SpokenWrittenLanguage')

    optional = [
      'ExtraConsent_21YearsofAge', 'ExtraConsent_19YearsofAge',
      'ExtraConsent_HealthRecordVideo', 'ExtraConsent_CABoRSignature',
      'ExtraConsent__CABoRTodaysDate', 'VeteransHealthAdministration',
      'ExtraConsent_KeepinginTouchVideo', 'ExtraConsent_HealthDataVideo',
      'ExtraConsent_PhysicalMeausrementsVideo', 'ExtraConsent_SamplesVideo',
      'ExtraConsent_DNAVideo', 'ExtraConsent_FitnessTrackerVideo',
      'ExtraConsent_OtherHealthDataVideo',
      'ExtraConsent_DataSharingVideo', 'ExtraConsent_RisktoPrivacyVideo',
      'ExtraConsent_EmailACopyToMe', 'ExtraConsent_AllOfUsPurpose', 'ExtraConsent_TakePartAllOfUs',
      'ExtraConsent_CanIWithdraw', 'ExtraConsent_PrivacyGuaranteeQuestion',
      'ExtraConsent_ReadyOrNeedHelp', 'ExtraConsent_AgreeToConsent', 'ExtraConsent_Signature',
      'ExtraConsent_TodaysDate', 'ConsentPII_HelpWithConsent',
      'ConsentPII_HelpWithConsentSignature']

    # Loop through the random questions and get answers for them.
    for qn_code in optional:
      qn = self._find_question(qn_code)
      if not qn:
        _logger.warning('question code not found in module [{0}].'.format(qn_code))
        continue
      results = self._create_random_answer(qn)
      if results:
        for result in results:
          qnans.append(self._answer_question(qn, result))

    return qnans

  def _answers_for_the_basics(self):
    """
    Create answers for TheBasics module.
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    qnans = list()

    # answer ethnicity question
    question = self._find_question('Race_WhatRaceEthnicity')
    answer = random.choice(self._get_question_choices(question))
    qnans.append(self._answer_question(question, answer))

    # see if we need to get an ethnicity sub category question answer
    if answer == 'Asian_AsianSpecific':
      question = self._find_question('Asian_AsianSpecific')
      qnans.append(random.choice(question, random.choice(self._get_question_choices(question))))
    elif answer == 'Black_BlackSpecific':
      question = self._find_question('Black_BlackSpecific')
      qnans.append(random.choice(question, random.choice(self._get_question_choices(question))))
    elif answer == 'Hispanic_HispanicSpecific':
      question = self._find_question('Hispanic_HispanicSpecific')
      qnans.append(random.choice(question, random.choice(self._get_question_choices(question))))
    elif answer == 'MENA_MENASpecific':
      question = self._find_question('MENA_MENASpecific')
      qnans.append(random.choice(question, random.choice(self._get_question_choices(question))))
    elif answer == 'NHPI_NHPISpecific':
      question = self._find_question('NHPI_NHPISpecific')
      qnans.append(random.choice(question, random.choice(self._get_question_choices(question))))
    elif answer == 'White_WhiteSpecific':
      question = self._find_question('White_WhiteSpecific')
      qnans.append(random.choice(question, random.choice(self._get_question_choices(question))))
    if random.random() > 0.6:
      contact = self._make_contact_info(
        self._find_question('PersonOneAddress_PersonOneAddressState'))

      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_PersonOneFirstName'), contact['first']))
      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_SecondContactsMiddleInitial'),
        contact['middle'][:1]))
      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_PersonOneLastName'), contact['last']))
      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_PersonOneTelephone'), contact['phone']))
      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_PersonOneEmail'), contact['email']))

      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_PersonOneAddressOne'), contact['street1']))
      qnans.append(self._answer_question(
        self._find_question('SecondaryContactInfo_PersonOneAddressTwo'), contact['street1']))
      qnans.append(self._answer_question(
        self._find_question('PersonOneAddress_PersonOneAddressCity'), contact['city']))
      qnans.append(self._answer_question(
        self._find_question('PersonOneAddress_PersonOneAddressState'), contact['state']))
      qnans.append(self._answer_question(
        self._find_question('PersonOneAddress_PersonOneAddressZipCode'), contact['zip']))
    else:
      if random.random() > 0.5:
        qnans.append(self._answer_question(
          self._find_question('SecondaryContactInfo_PersonOnePreferNotToAnswer'), True))

    # figure out work place location
    if random.random() > 0.04:
      # "EmploymentWorkAddress_AddressLineOne", "EmploymentWorkAddress_AddressLineTwo",
      # "EmploymentWorkAddress_City", "EmploymentWorkAddress_State",
      # "EmploymentWorkAddress_ZipCode", "EmploymentWorkAddress_Country",
      # "Employment_EmploymentWorkAddress",
      pass

    # Make a weighted choice for gender
    question = self._find_question('Gender_GenderIdentity')
    gender_codes = self._get_question_choices(question)
    gender_choices = list()

    for code in gender_codes:
      gender_choices.append(
        (code, 5.0) if code in ['GenderIdentity_Man', 'GenderIdentity_Woman'] else (code, 0.08))

    gender = self._weighted_choice(gender_choices)
    suffix = gender.split('_')[1]
    qnans.append(self._answer_question(question, gender))

    # Make a weighted choice for birth sex
    question = self._find_question('BiologicalSexAtBirth_SexAtBirth')
    sex_codes = self._get_question_choices(question)
    sex_choices = list()

    for code in sex_codes:
      sex_choices.append((code, 9.0) if suffix in code else (code, 0.4))
    qnans.append(self._answer_question(question, self._weighted_choice(sex_choices)))
    # Make a weighted choice about sexual orientation.
    question = self._find_question('TheBasics_SexualOrientation')
    orient_codes = self._get_question_choices(question)
    orient_choices = list()

    for code in orient_codes:
      orient_choices.append((code, 9.0) if code == 'SexualOrientation_Straight' else (code, 0.5))
    qnans.append(self._answer_question(question, self._weighted_choice(orient_choices)))
    # Make a weighted choice for education.
    question = self._find_question('EducationLevel_HighestGrade')
    edu_codes = self._get_question_choices(question)
    edu_choices = list()

    for code in edu_codes:
      edu_choices.append(
        (code, 3.5) if code in ['HighestGrade_TwelveOrGED', 'HighestGrade_CollegeOnetoThree',
                                'HighestGrade_CollegeGraduate'] else (code, 0.5))
    qnans.append(self._answer_question(question, self._weighted_choice(edu_choices)))
    # Make annual income choice.
    question = self._find_question('Income_AnnualIncome')
    income_choices = self._get_question_choices(question)
    qnans.append(self._answer_question(question, random.choice(income_choices)))

    optional = [
      "TheBasics_CountryBornTextBox", "ActiveDuty_AvtiveDutyServeStatus",
      "MaritalStatus_CurrentMaritalStatus", "LivingSituation_HowManyPeople",
      "LivingSituation_PeopleUnder18",
      "Insurance_HealthInsurance", "HealthInsurance_InsuranceTypeUpdate",
      "OtherHealthPlan_FreeText", "Employment_EmploymentStatus",
      "HomeOwn_CurrentHomeOwn", "LivingSituation_CurrentLiving",
      "LivingSituation_LivingSituationFreeText",
      "LivingSituation_HowManyLivingYears", "LivingSituation_StableHouseConcern"]

    # Loop through the random questions and get answers for them.
    for qn_code in optional:
      qn = self._find_question(qn_code)
      if not qn:
        _logger.warning('question code not found in module [{0}].'.format(qn_code))
        continue
      results = self._create_random_answer(qn)
      if results:
        for result in results:
          qnans.append(self._answer_question(qn, result))

    return qnans

  def _answers_for_ehr_consentpii(self):
    """
    Create additional answers for EHRConsentPII module
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    qnans = self._answer_all_module_questions()

    question = self._find_question('EHRConsentPII_JoinAllOfUs')
    qnans.append(self._answer_question(question, True))

    return qnans

  def _answers_for_lifestyle(self):
    """
    Create additional answers for Lifestyle module
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    qnans = self._answer_all_module_questions()
    return qnans

  def _answers_for_overall_health(self):
    """
    Create additional answers for OverallHealth module
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    qnans = self._answer_all_module_questions()
    return qnans

  def _create_random_answer(self, question):
    """
    Create a random answer for the given question
    :param question: question object from self.module_questions object
    :return: list of answer values
    """
    if not question:
      return None

    answers = list()
    qn_code = question.concept[0].code

    if not qn_code:
      _logger.warning('link id is not a question [{0}].'.format(question.linkId))
      return answers

    # see if this question is listed in the answer_specs data
    for item in self._app_data['answer_specs']:
      if qn_code in item['question_code']:
        if random.random() > (float(item['num_participants']) / 100.0):

          # if this question is answered more than once, choose a random number of answers.
          na = float(item['num_answers'])
          nqr = float(item['num_questionnaire_responses'])
          count = 1

          if na > nqr:
            rand_val = random.random() * (na / nqr)
            if rand_val > 1:
              # Set the answer count to 2 or more
              count = int(1 + max(1, rand_val))
          # pylint: disable=unused-variable
          for x in range(0, count):
            answer = self._choose_random_answer(question, item)
            if answer:
              answers.append(answer)

        return answers

    # decide if we are going to answer this question or not
    if random.random() < 0.7:
      return answers

    # choose a random answer for all other questions
    answers.append(self._choose_random_answer(question))

    return answers

  def _choose_random_answer(self, question, item=None):
    """
    Generate an answer to the spec question.
    :param question: question object from self.module_questions object
    :param item: item from self._data_lists['answer_specs']
    :return: answer
    """
    if question.type not in self._answer_types:
      _logger.debug('unhandled or invalid question answer type [{0}].'.format(question.type))
      return None

    answer = None

    if question.type == 'boolean':
      if item:  # handle spec item
        if int(item['boolean_answer_count']) > 0:
          answer = (random.random() < 0.5)
      else:
        answer = (random.random() < 0.5)

    elif question.type == 'decimal':
      if item:
        if int(item['decimal_answer_count']) > 0:
          answer = round(random.uniform(float(item['min_decimal_answer']),
                                        float(item['max_decimal_answer'])), 1)
      else:
        answer = round(random.uniform(0.0, 20.0), 1)

    elif question.type == 'integer':
      if item:
        if int(item['integer_answer_count']) > 0:
          answer = random.randint(int(item['min_integer_answer']), int(item['max_integer_answer']))
      else:
        answer = random.randint(0, 100)

    elif question.type == 'date':
      if item:
        if int(item['date_answer_count']) > 0:
          min_date = parse(item['min_date_answer'])
          max_date = parse(item['max_date_answer'])
          days_diff = (max_date - min_date).days
          answer = (min_date + datetime.timedelta(days=random.randint(0, days_diff))).isoformat()
      else:
        days_sub = random.randint(1, (5 * 365))
        answer = (clock.CLOCK.now() - datetime.timedelta(days=days_sub)).isoformat()

    elif question.type == 'dateTime':
      if item:
        if int(item['datetime_answer_count']) > 0:
          min_date = parse(item['min_datetime_answer'])
          max_date = parse(item['max_datetime_answer'])
          seconds_diff = (max_date - min_date).total_seconds()
          answer = (min_date +
                    datetime.timedelta(seconds=random.randint(0, seconds_diff))).isoformat()
      else:
        days_sub = random.randint(1, (5 * 365))
        answer = (clock.CLOCK.now() - datetime.timedelta(days=days_sub)).isoformat()

    elif question.type == 'time':
      pass

    elif question.type == 'string':
      if item:
        if int(item['string_answer_count']) > 0:
          answer = ' '.join([random.choice(self._app_data['latin_words'])
                             for _ in xrange(random.randint(1, 5))])
      else:
        answer = ' '.join([random.choice(self._app_data['latin_words'])
                           for _ in xrange(random.randint(1, 5))])

    elif question.type == 'text':
      answer = ' '.join([random.choice(self._app_data['latin_words'])
                         for _ in xrange(random.randint(1, 5))])

    elif question.type == 'choice':
      choices = self._get_question_choices(question)
      if item:
        if int(item['code_answer_count']) > 0:
          answer = random.choice(choices)
      else:
        answer = random.choice(choices)

    elif question.type == 'url' or question.type == 'uri':
      bucket = 'gs://notarealbucket.example.com/{0}'.format(
        ''.join([random.choice(string.lowercase) for _ in xrange(20)]))
      if item:
        if int(item['uri_answer_count']) > 0:
          answer = bucket
      else:
        answer = bucket

    elif question.type == 'quantity':
      pass

    return answer

  def _answer_question(self, question, answer, quantity=None):
    """
    Setup a question answer.
    :param question: question object from self.module_questions object
    :param answer: answer value
    :param quantity: models.quantity.Quantity object
    :return: QuestionnaireResponseGroupQuestion object
    """
    if question.type not in self._answer_types:
      _logger.debug('unhandled or invalid question answer type [{0}].'.format(question.type))
      return None

    # setup answer object
    an = QuestionnaireResponseGroupQuestionAnswer()

    # store answer
    if question.type == 'boolean':
      an.valueBoolean = False
      if isinstance(answer, str):
        if answer.lower() in ['yes', 'true']:
          an.valueBoolean = True
      if isinstance(answer, bool):
        an.valueBoolean = answer

    elif question.type == 'decimal':
      an.valueDecimal = float(answer)

    elif question.type == 'integer':
      an.valueInteger = int(answer)

    elif question.type == 'date':
      if isinstance(answer, str):
        an.valueDate = FHIRDate(answer)
      else:
        an.valueDate = FHIRDate(answer.isoformat())

    elif question.type == 'dateTime':
      if isinstance(answer, str):
        an.valueDateTime = FHIRDate(answer)
      else:
        an.valueDateTime = FHIRDate(answer.isoformat())

    elif question.type == 'time':
      if isinstance(answer, str):
        an.valueTime = FHIRDate(answer)
      else:
        an.valueTime = FHIRDate(answer.isoformat())

    elif question.type == 'string':
      an.valueString = answer

    elif question.type == 'text':
      an.valueString = answer

    elif question.type == 'url' or question.type == 'uri':
      an.valueUri = answer

    elif question.type == 'choice':
      if answer in self._get_question_choices(question):
        an.valueString = answer

    elif question.type == 'quantity':
      an.valueQuantity = quantity

    if not an:
      return None

    qn = QuestionnaireResponseGroupQuestion()
    qn.linkId = question.linkId
    qn.answer = list()
    qn.answer.append(an)

    return qn

  def _download_questions_from_github(self, module):
    """
    Download fhir questionnaires from github.
    :return: fhir questionnaire object
    """
    _host = 'raw.githubusercontent.com'
    _url = 'all-of-us-terminology/api-payloads/master/questionnaire_payloads/'

    for key, data in self._module_data.items():
      if key.lower() == module.lower():

        code, resp = make_api_request(_host, '{0}/{1}'.format(_url, data['file']))

        if code != 200:
          _logger.error('failed to get module questions from github.')
          return None

        questions = Questionnaire(resp, strict=False)
        if questions:
          return questions

    _logger.error('failed to parse the module questions data.')
    return None

  def _download_questions(self, module):
    """
    Download fhir questionnaire from rdr service.
    :param module: questionnaire module name
    :return: fhir questionnaire object
    """
    data = dict()
    data['api'] = 'Questionnaire?concept={0}'.format(module)
    data['timestamp'] = clock.CLOCK.now().isoformat()
    data['method'] = 'GET'

    code, resp = make_api_request(
                    self._rdr_host, self._gen_url, req_type='POST', json_data=data, headers=gcp_make_auth_header())

    if code != 200:
      _logger.error('failed to get module questionnaire [Http {0}: {1}.'.format(code, resp))
      return None

    questions = Questionnaire(resp, strict=False)
    if questions:
      return questions

    _logger.error('failed to parse the module questionnaire data.')
    return None


  def _get_module_question_codes(self):
    """
    Return all of the question code ids in the module
    :return: list
    """
    codes = list()

    for item in self._module_questions.group.question:
      qn_code = item.concept[0].code

      if qn_code == 'null':
        _logger.warning('question code id set to null in module.')
        continue

      codes.append(qn_code)

    return codes

  def _get_question_choices(self, question):
    """
    Return a list of possible choices for the given question
    :param question: question object from self.module_questions object
    :return: list
    """
    if question.type != 'choice':
      _logger.warning('question type is not a choice question type.')
      return list()

    choices = list()
    for choice in question.option:
      choices.append(choice.code)

    return choices

  def _find_question(self, question_code):
    """
    Get the specific question from the available module questions.
    :param question_code: question code id string
    :return: QuestionnaireGroupQuestion object
    """
    if not self._module_questions:
      _logger.error('module questions not initialized.')
      return None

    for question in self._module_questions.group.question:
      if not question.concept[0].code:
        continue
      if 'Language' in question.concept[0].code:
        print(question.concept[0].code)
      if question_code == question.concept[0].code:
        return question

    return None

  def _answer_all_module_questions(self):
    """
    Randomly answer all module questions.
    :return: list of QuestionnaireResponseGroupQuestion objects
    """
    qnans = list()

    for question in self._module_questions.group.question:
      qn_code = question.concept[0].code
      # print(qn_code)

      if qn_code == 'null':
        _logger.warning('question code id set to null in module.')
        continue

      # create a random answer for the spec question.
      results = self._create_random_answer(question)
      for result in results:
        qnans.append(self._answer_question(question, result))

    return qnans
