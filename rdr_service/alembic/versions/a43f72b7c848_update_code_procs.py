"""update code procs

Revision ID: a43f72b7c848
Revises: 3adfe155c68b
Create Date: 2019-04-15 14:55:40.416929

"""
from alembic import op

from rdr_service.dao.alembic_utils import ReplaceableObject

# revision identifiers, used by Alembic.
revision = "a43f72b7c848"
down_revision = "3adfe155c68b"
branch_labels = None
depends_on = None

sp_get_code_module_items = ReplaceableObject(
    "sp_get_code_module_items",
    """
 (IN module VARCHAR(80))
 BEGIN
   # Return all of the codebook items (topics, questions, answers) related to the passed
   # module name.
   SELECT @code_id := code_id FROM code WHERE `value` = module and parent_id is NULL;

   SELECT a.code_id, a.parent_id, a.topic, a.code_type, a.`value`, a.display, a.`system`, a.mapped, a.created, a.code_book_id, a.short_value
   FROM (
      SELECT t1.*, '0' AS sort_id
      FROM code t1
      WHERE t1.code_id = @code_id
      UNION ALL
      SELECT t2.*, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value) AS sort_id
      FROM code t1
               INNER JOIN code t2 on t2.parent_id = t1.code_id
      WHERE t1.code_id = @code_id
      UNION ALL
      SELECT t3.*, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value, LPAD(t3.code_id, 8, '0')) AS sort_id
      FROM code t1
               INNER JOIN code t2 on t2.parent_id = t1.code_id
               INNER JOIN code t3 on t3.parent_id = t2.code_id
      WHERE t1.code_id = @code_id
      UNION ALL
      SELECT t4.*, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value, LPAD(t3.code_id, 8, '0'), t3.value)
      FROM code t1
               INNER JOIN code t2 on t2.parent_id = t1.code_id
               INNER JOIN code t3 on t3.parent_id = t2.code_id
               INNER JOIN code t4 on t4.parent_id = t3.code_id
      WHERE t1.code_id = @code_id
   ) a
   ORDER BY a.sort_id, a.code_id;

 END
""",
)

sp_get_questionnaire_answers = ReplaceableObject(
    "sp_get_questionnaire_answers",
    """
(IN module VARCHAR(80), IN participant_id INT)
  BEGIN
    # Dynamically pivot the questionnaire answers for the given participant and module.
    # Results are ordered by 'created' descending.
    DECLARE CONTINUE HANDLER FOR 1064 SET @sql = NULL;
    DECLARE CONTINUE HANDLER FOR 1243 SELECT 1 AS 'invalid_code_id' FROM dual WHERE FALSE;

    SET @sql = '';
    SELECT @module := COALESCE(c.value, 0), @code_id := COALESCE(c.code_id, 0)
    FROM code c
    WHERE c.value = module;

    SELECT @sql := CONCAT(@sql, IF(@sql = '', '', ', '), temp.output)
    FROM (
         SELECT DISTINCT CONCAT('GROUP_CONCAT(IF(code_id = ', code_id, ', answer, NULL) SEPARATOR ",") AS ',
                                `value`) as output
         FROM (
              SELECT a.*
              FROM (
                  SELECT t1.code_id, t1.value, t1.display, t1.code_type, t1.parent_id, '0' AS sort_id
                  FROM code t1
                  WHERE t1.code_id = @code_id
                  UNION ALL
                  SELECT t2.code_id, t2.value, t2.display, t2.code_type, t2.parent_id, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value) AS sort_id
                  FROM code t1
                           INNER JOIN code t2 on t2.parent_id = t1.code_id
                  WHERE t1.code_id = @code_id
                  UNION ALL
                  SELECT t3.code_id, t3.value, t3.display, t3.code_type, t3.parent_id, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value, LPAD(t3.code_id, 8, '0')) AS sort_id
                  FROM code t1
                           INNER JOIN code t2 on t2.parent_id = t1.code_id
                           INNER JOIN code t3 on t3.parent_id = t2.code_id
                  WHERE t1.code_id = @code_id
                  UNION ALL
                  SELECT t4.code_id, t4.value, t4.display, t4.code_type, t4.parent_id, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value, LPAD(t3.code_id, 8, '0'), t3.value)
                  FROM code t1
                           INNER JOIN code t2 on t2.parent_id = t1.code_id
                           INNER JOIN code t3 on t3.parent_id = t2.code_id
                           INNER JOIN code t4 on t4.parent_id = t3.code_id
                  WHERE t1.code_id = @code_id
              ) a
              ORDER BY a.sort_id, a.code_id
          ) b
         WHERE b.code_type = 3
     ) AS temp;

    SET @sql = CONCAT('
     SELECT
        a.questionnaire_id,
        a.questionnaire_response_id,
        a.created,
        ', @code_id, ' as code_id,
        a.version,
        a.authored,
        a.language,
        a.participant_id,
        ''', @module, ''' as module,
        ', @sql, '
        FROM (
           SELECT qr.questionnaire_id,
                  qr.questionnaire_response_id,
                  qr.created,
                  qq.code_id,
                  q.version,
                  qr.authored,
                  qr.language,
                  qr.participant_id,
              COALESCE((SELECT c.value from code c where c.code_id = qra.value_code_id),
                         qra.value_integer, qra.value_decimal,
                         qra.value_boolean, qra.value_string, qra.value_system,
                         qra.value_uri, qra.value_date, qra.value_datetime) as answer
           FROM questionnaire_response qr
              INNER JOIN questionnaire_response_answer qra
                         ON qra.questionnaire_response_id = qr.questionnaire_response_id
              INNER JOIN questionnaire_question qq
                         ON qra.question_id = qq.questionnaire_question_id
              INNER JOIN questionnaire q
                         ON qq.questionnaire_id = q.questionnaire_id
           WHERE qr.participant_id = ', participant_id, ' AND
               --
                 qr.questionnaire_id IN (
                   SELECT q.questionnaire_id
                     FROM questionnaire q
                       INNER JOIN questionnaire_concept qc
                               ON q.questionnaire_id = qc.questionnaire_id AND q.version = qc.questionnaire_version
                     WHERE qc.code_id = ', @code_id, '
           )
        ) a
        GROUP BY a.questionnaire_response_id, a.version
        ORDER BY a.created DESC
   ');

    -- select @sql;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END
""",
)

participant_answers_view = ReplaceableObject(
    "participant_answers_view",
    """
  SELECT
      qr.participant_id,
      code.value AS module,
      (SELECT c.value FROM code c WHERE c.code_id = qq.code_id) AS question_code,
      COALESCE((SELECT c.value FROM code c WHERE c.code_id = qra.value_code_id),
          qra.value_boolean, qra.value_date, qra.value_datetime, qra.value_decimal, qra.value_integer,
          qra.value_string, qra.value_system, qra.value_uri) AS answer,
      qr.questionnaire_response_id,
      qr.authored,
      qr.created
  FROM questionnaire_response_answer qra
      INNER JOIN questionnaire_response qr ON qr.questionnaire_response_id = qra.questionnaire_response_id
      INNER JOIN questionnaire_question qq ON qra.question_id = qq.questionnaire_question_id
      INNER JOIN questionnaire_concept qc ON qc.questionnaire_id = qr.questionnaire_id
      INNER JOIN code ON qc.code_id = code.code_id
  ORDER BY qr.participant_id, qr.created DESC, question_code
""",
)


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_rdr():
    op.replace_sp(sp_get_code_module_items, replaces="ed28b84f061e.sp_get_code_module_items")
    op.replace_sp(sp_get_questionnaire_answers, replaces="1338221caf81.sp_get_questionnaire_answers")
    op.create_view(participant_answers_view)


def downgrade_rdr():
    op.replace_sp(sp_get_code_module_items, replace_with="ed28b84f061e.sp_get_code_module_items")
    op.replace_sp(sp_get_questionnaire_answers, replace_with="1338221caf81.sp_get_questionnaire_answers")
    op.drop_view(participant_answers_view)


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def unittest_schemas():
    schemas = list()

    schemas.append("DROP PROCEDURE IF EXISTS `{0}`".format(sp_get_code_module_items.name))
    schemas.append(
        "CREATE PROCEDURE `{0}` {1}".format(sp_get_code_module_items.name, sp_get_code_module_items.sqltext)
    )

    schemas.append("DROP PROCEDURE IF EXISTS `{0}`".format(sp_get_questionnaire_answers.name))
    schemas.append(
        "CREATE PROCEDURE `{0}` {1}".format(sp_get_questionnaire_answers.name, sp_get_questionnaire_answers.sqltext)
    )

    return schemas
