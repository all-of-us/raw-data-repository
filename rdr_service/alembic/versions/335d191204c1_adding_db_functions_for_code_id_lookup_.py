"""Adding DB functions for code_id lookup and determining participant ethnicity

Revision ID: 335d191204c1
Revises: 1e944af3ad04
Create Date: 2019-01-10 13:05:48.899901

"""
from alembic import op

from rdr_service.dao.alembic_utils import ReplaceableObject

# revision identifiers, used by Alembic.
revision = "335d191204c1"
down_revision = "1e944af3ad04"
branch_labels = None
depends_on = None

fn_get_code_id_from_key = ReplaceableObject(
    "fn_get_code_id_from_key",
    """
  (code_value VARCHAR(80))
  RETURNS INT
  READS SQL DATA
  BEGIN
    # Return the record code_id for the given key from the code table.
    DECLARE result INT;
    SET result = (SELECT code_id FROM code
                    WHERE `value` = code_value ORDER BY code_id DESC LIMIT 1);
    RETURN result;
  END
  """,
)

fn_get_participant_ethnicity = ReplaceableObject(
    "fn_get_participant_ethnicity",
    """
    (participant INT, code_id INT)
    RETURNS CHAR(1)
    READS SQL DATA
    BEGIN
      # Determine if the participant's selected ethnicity matches the given id from the code table.
      # Use fn_get_code_id_from_key() to get the code_id value from a code table key value.
      # Returns: 'Y' or 'N'
      DECLARE result CHAR(1);
      SET result = (
        SELECT
          CASE
             WHEN
               (SELECT count(1)
                FROM questionnaire_response qr
                   INNER JOIN questionnaire_response_answer qra
                              ON qra.questionnaire_response_id = qr.questionnaire_response_id
                   INNER JOIN questionnaire_question qq
                              ON qra.question_id = qq.questionnaire_question_id
                WHERE qr.participant_id = participant
                  AND qq.code_id = fn_get_code_id_from_key('Race_WhatRaceEthnicity')
                  AND qra.value_code_id = code_id
                  AND qra.end_time IS NULL) > 0 THEN 'Y'
             ELSE 'N' END
        );
      RETURN result;
    END
    """,
)


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    op.create_fn(fn_get_code_id_from_key)
    op.create_fn(fn_get_participant_ethnicity)


def downgrade_rdr():
    op.drop_fn(fn_get_code_id_from_key)
    op.drop_fn(fn_get_participant_ethnicity)


def upgrade_metrics():
    pass


def downgrade_metrics():
    pass


def unittest_schemas():
    schemas = list()

    schemas.append("DROP FUNCTION IF EXISTS `{0}`".format(fn_get_code_id_from_key.name))
    schemas.append("CREATE FUNCTION `{0}` {1}".format(fn_get_code_id_from_key.name, fn_get_code_id_from_key.sqltext))

    schemas.append("DROP FUNCTION IF EXISTS `{0}`".format(fn_get_participant_ethnicity.name))
    schemas.append(
        "CREATE FUNCTION `{0}` {1}".format(fn_get_participant_ethnicity.name, fn_get_participant_ethnicity.sqltext)
    )

    return schemas
