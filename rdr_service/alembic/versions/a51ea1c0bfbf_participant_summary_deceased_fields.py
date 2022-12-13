"""participant_summary_deceased_fields

Revision ID: a51ea1c0bfbf
Revises: d5d97368b14d
Create Date: 2020-08-04 12:10:33.604849

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import DeceasedStatus

# revision identifiers, used by Alembic.
revision = 'a51ea1c0bfbf'
down_revision = '7029234abc61'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('date_of_death', sa.Date(), nullable=True))
    op.add_column('participant_summary', sa.Column('deceased_authored', rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.add_column('participant_summary', sa.Column('deceased_status', rdr_service.model.utils.Enum(DeceasedStatus), nullable=False))
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('participant_summary', 'deceased_status')
    op.drop_column('participant_summary', 'deceased_authored')
    op.drop_column('participant_summary', 'date_of_death')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
