"""update_sp_get_code_module

Revision ID: 7c0585790bc8
Revises: 377e8ced743f
Create Date: 2021-08-31 16:26:41.493837

"""
from alembic import op
from rdr_service.dao.alembic_utils import ReplaceableObject

# revision identifiers, used by Alembic.
revision = '7c0585790bc8'
down_revision = '377e8ced743f'
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

    select code_id,
       parent_id,
       topic,
       code_type,
       `value`,
       display,
       `system`,
       mapped,
       created,
       code_book_id,
       short_value
    from code where code_id in
    (
        select distinct s.code_id
        from survey s
        where s.code_id=@code_id
        UNION
        select distinct sq.code_id
        from survey s, survey_question sq
        where s.id=sq.survey_id
        and s.code_id=@code_id
        UNION
        select distinct sqo.code_id
        from survey s, survey_question sq, survey_question_option sqo
        where s.id=sq.survey_id and sq.id=sqo.question_id
        and s.code_id=@code_id
        )
    order by code_id;
  END;
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
                    select code_id, value
                    from code where code_id in
                    (
                        select distinct sq.code_id
                        from survey s, survey_question sq
                        where s.id=sq.survey_id
                        and s.code_id=@code_id
                    )
                    order by code_id
                  ) b
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
  END;

""",
)

def upgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"upgrade_{engine_name}"]()
    else:
        pass


def downgrade(engine_name):
    if engine_name == "rdr" or engine_name == "metrics":
        globals()[f"downgrade_{engine_name}"]()
    else:
        pass



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.replace_sp(sp_get_code_module_items, replaces="7ab9205d1bc6.sp_get_code_module_items")
    op.replace_sp(sp_get_questionnaire_answers, replaces="a43f72b7c848.sp_get_questionnaire_answers")
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.replace_sp(sp_get_code_module_items, replace_with="7ab9205d1bc6.sp_get_code_module_items")
    op.replace_sp(sp_get_questionnaire_answers, replace_with="a43f72b7c848.sp_get_questionnaire_answers")
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
