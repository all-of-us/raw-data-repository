
from code_constants import RACE_QUESTION_CODE, RACE_WHITE_CODE, RACE_AIAN_CODE
from model.code import CodeType
from unit_test_util import SqlTestBase

from dao.code_dao import CodeDao
from test_data import consent_code


class SchemaTest(SqlTestBase):

  def setUp(self):
    super(SchemaTest, self).setUp(use_mysql=True)

    self.code_dao = CodeDao()


  def _setup_codes(self):

    self.consent_code_id = self.code_dao.insert(consent_code()).codeId

    self.setup_codes([RACE_QUESTION_CODE], CodeType.QUESTION)
    self.setup_codes([RACE_AIAN_CODE, RACE_WHITE_CODE], CodeType.ANSWER)

  def test_fn_get_code_id_from_key(self):

    self._setup_codes()

    engine = self.database.get_engine()
    result = engine.execute("select fn_get_code_id_from_key('WhatRaceEthnicity_AIAN')").fetchone()
    self.assertEquals(3, result[0])

    result = engine.execute("select fn_get_code_id_from_key('WhatRaceEthnicity_White')").fetchone()
    self.assertEquals(4, result[0])

  def test_fn_get_code_module(self):

    self._setup_codes()

    # Test each level of a Topic
    #  ConsentPII              := level 1
    #  ConsentPII_PIIAddress   := level 2
    #  StateOfResidence        := level 3
    #  SOR_AK                  := level 4

    engine = self.database.get_engine()
    result = engine.execute("select fn_get_code_module('ConsentPII')").fetchone()
    self.assertEquals('ConsentPII', result[0])

    result = engine.execute("select fn_get_code_module('ConsentPII_PIIAddress')").fetchone()
    self.assertEquals('ConsentPII', result[0])

    result = engine.execute("select fn_get_code_module('StateOfResidence')").fetchone()
    self.assertEquals('ConsentPII', result[0])

    result = engine.execute("select fn_get_code_module('SOR_AK')").fetchone()
    self.assertEquals('ConsentPII', result[0])


    result = engine.execute("select fn_get_code_module_id('ConsentPII')").fetchone()
    self.assertIsNotNone(result)
    code_id = result[0]
    self.assertGreater(code_id, 0)

    result = engine.execute("select fn_get_code_module_id('ConsentPII_PIIAddress')").fetchone()
    self.assertEquals(code_id, result[0])

    result = engine.execute("select fn_get_code_module_id('StateOfResidence')").fetchone()
    self.assertEquals(code_id, result[0])

    result = engine.execute("select fn_get_code_module_id('SOR_AK')").fetchone()
    self.assertEquals(code_id, result[0])

