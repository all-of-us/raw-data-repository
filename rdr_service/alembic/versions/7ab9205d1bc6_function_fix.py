"""function fix

Revision ID: 7ab9205d1bc6
Revises: da3c59138805
Create Date: 2020-02-20 10:27:24.133396

"""
from alembic import op

from rdr_service.dao.alembic_utils import ReplaceableObject


# revision identifiers, used by Alembic.
revision = '7ab9205d1bc6'
down_revision = 'da3c59138805'
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
      UNION ALL
      SELECT t5.*, CONCAT(LPAD(t2.code_id, 8, '0'), t2.value, LPAD(t3.code_id, 8, '0'), t3.value)
      FROM code t1
               INNER JOIN code t2 on t2.parent_id = t1.code_id
               INNER JOIN code t3 on t3.parent_id = t2.code_id
               INNER JOIN code t4 on t4.parent_id = t3.code_id
               INNER JOIN code t5 on t5.parent_id = t4.code_id
      WHERE t1.code_id = @code_id
   ) a
   ORDER BY a.sort_id, a.code_id;

 END
""",
)



def upgrade(engine_name):
    globals()[f"upgrade_{engine_name}"]()


def downgrade(engine_name):
    globals()[f"downgrade_{engine_name}"]()



def upgrade_rdr():
    op.replace_sp(sp_get_code_module_items, replaces="a43f72b7c848.sp_get_code_module_items")

def downgrade_rdr():
    op.replace_sp(sp_get_code_module_items, replace_with="a43f72b7c848.sp_get_code_module_items")


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
