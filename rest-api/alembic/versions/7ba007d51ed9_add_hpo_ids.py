"""Add HPO IDs

Revision ID: 7ba007d51ed9
Revises: 3130b3100bd1
Create Date: 2017-02-15 16:18:39.016650

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7ba007d51ed9'
down_revision = '3130b3100bd1'
branch_labels = None
depends_on = None


def upgrade():
    hpo_table = sa.Table('hpo', sa.MetaData(),
      sa.Column('hpo_id', sa.Integer(), autoincrement=False, nullable=False),
      sa.Column('name', sa.String(length=20), nullable=True),
      sa.PrimaryKeyConstraint('hpo_id'),
      sa.UniqueConstraint('name')
    )

    # Insert our HPO IDs into the HPO table.    
    op.bulk_insert(hpo_table,
    [
        {'hpo_id': 0, 'name': 'UNSET' },
        {'hpo_id': 1, 'name': 'PITT' },
        {'hpo_id': 2, 'name': 'COLUMBIA' },
        {'hpo_id': 3, 'name': 'ILLNOIS' },
        {'hpo_id': 4, 'name': 'AZ_TUCSON' },
        {'hpo_id': 5, 'name': 'COMM_HEALTH' },
        {'hpo_id': 6, 'name': 'SAN_YSIDRO' },
        {'hpo_id': 7, 'name': 'CHEROKEE' },
        {'hpo_id': 8, 'name': 'EAU_CLAIRE' },
        {'hpo_id': 9, 'name': 'HRHCARE' },
        {'hpo_id': 10, 'name': 'JACKSON' },
        {'hpo_id': 11, 'name': 'GEISINGER' },
        {'hpo_id': 12, 'name': 'CAL_PMC' },
        {'hpo_id': 13, 'name': 'NE_PMC' },
        {'hpo_id': 14, 'name': 'TRANS_AM' },
        {'hpo_id': 15, 'name': 'VA' }
    ])


def downgrade():    
    pass
    
