"""New stored procs for surveys

Revision ID: ed28b84f061e
Revises: d5cbabd682e5
Create Date: 2019-02-28 15:07:07.650442

"""
from alembic import op
import sqlalchemy as sa
import model.utils
from dao.alembic_utils import ReplaceableObject


from participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from participant_enums import MetricSetType, MetricsKey
from model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus
from model.code import CodeType

# revision identifiers, used by Alembic.
revision = 'ed28b84f061e'
down_revision = 'd5cbabd682e5'
branch_labels = None
depends_on = None

sp_get_code_module_items = ReplaceableObject('sp_get_code_module_items',
"""  
  (IN module VARCHAR(80))
  BEGIN
    # Resurn all of the codebook items (topics, questions, answers) releated to the passed 
    # module name.
    SELECT @code_id := code_id FROM code WHERE `value` = module and parent_id is NULL;
  
    SELECT a.*
    FROM (
       SELECT c1.*
          FROM code c1
          WHERE c1.code_id = @code_id
       UNION ALL
       SELECT c2.*
          FROM code c2 INNER JOIN code m1 ON c2.parent_id = m1.code_id
          WHERE m1.code_id = @code_id
       UNION ALL
       SELECT c3.*
          FROM code c3
            INNER JOIN code m1 ON c3.parent_id = m1.code_id
            INNER JOIN code m2 ON m1.parent_id = m2.code_id
          WHERE m2.code_id = @code_id
    ) a
    ORDER BY
      a.code_id;
  
  END
""")

sp_get_questionnaire_answers = ReplaceableObject('sp_get_questionnaire_answers',
"""  
  (IN participant_id INT, IN id INT)  
  BEGIN  
    # Dynamically pivot the questionnaire answers for the given participant and module.
    # Results are ordered by 'created' descending.
    DECLARE CONTINUE HANDLER FOR 1064 SET @sql = NULL;
    DECLARE CONTINUE HANDLER FOR 1243 SELECT 1 AS 'invalid_code_id' FROM dual WHERE FALSE;
  
    SET @sql = '';
    SELECT @module := COALESCE(c.value, 0), @code_id := COALESCE(c.code_id, 0) FROM code c WHERE c.code_id = id;
  
    SELECT @sql := CONCAT(@sql, IF(@sql = '', '', ', '), temp.output)
    FROM (
       SELECT DISTINCT CONCAT('MAX(IF(code_id = ', code_id, ', answer, NULL)) AS ', `value`) as output
       FROM (
          SELECT a.*
            FROM (
               SELECT c1.code_id, c1.value, c1.display, c1.code_type, c1.parent_id
                  FROM code c1
                  WHERE c1.code_id = @code_id
               UNION ALL
               SELECT c2.code_id, c2.value, c2.display, c2.code_type, c2.parent_id
                  FROM code c2 INNER JOIN code m1 ON c2.parent_id = m1.code_id
                  WHERE m1.code_id = @code_id
               UNION ALL
               SELECT c3.code_id, c3.value, c3.display, c3.code_type, c3.parent_id
                  FROM code c3
                    INNER JOIN code m1 ON c3.parent_id = m1.code_id
                    INNER JOIN code m2 ON m1.parent_id = m2.code_id
                  WHERE m2.code_id = @code_id
            ) a
            ORDER BY
              a.code_id
       ) b
       WHERE b.code_type = 3
     ) AS temp;
  
    SET @sql = CONCAT('
      SELECT
         a.questionnaire_id,
         a.questionnaire_response_id,
         a.created,
         a.participant_id,
         ', @code_id, ' as code_id,
         ''', @module, ''' as module,
         ', @sql, '
         FROM (
            SELECT qr.questionnaire_id,
                   qr.questionnaire_response_id,
                   qr.created,
                   qr.participant_id,
                   qq.code_id,
               COALESCE(qra.value_string, qra.value_integer, qra.value_decimal,
                        qra.value_boolean, qra.value_code_id, qra.value_system,
                        qra.value_uri, qra.value_date, qra.value_datetime) as answer
            FROM questionnaire_response qr
               INNER JOIN questionnaire_response_answer qra
                          ON qra.questionnaire_response_id = qr.questionnaire_response_id
               INNER JOIN questionnaire_question qq
                          ON qra.question_id = qq.questionnaire_question_id
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
         GROUP BY a.questionnaire_response_id
         ORDER BY a.created DESC
    ');
  
  PREPARE stmt FROM @sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;
  
  END
""")


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_sp(sp_get_code_module_items)
    op.create_sp(sp_get_questionnaire_answers)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_sp(sp_get_code_module_items)
    op.drop_sp(sp_get_questionnaire_answers)
    # ### end Alembic commands ###


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

  schemas.append('DROP PROCEDURE IF EXISTS `{0}`'.format(sp_get_code_module_items.name))
  schemas.append('CREATE PROCEDURE `{0}` {1}'.format(
                  sp_get_code_module_items.name, sp_get_code_module_items.sqltext))

  schemas.append('DROP PROCEDURE IF EXISTS `{0}`'.format(sp_get_questionnaire_answers.name))
  schemas.append('CREATE PROCEDURE `{0}` {1}'.format(
                  sp_get_questionnaire_answers.name, sp_get_questionnaire_answers.sqltext))

  return schemas
