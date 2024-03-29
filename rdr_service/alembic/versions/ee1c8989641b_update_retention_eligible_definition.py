"""update_retention_eligible_definition

Revision ID: ee1c8989641b
Revises: cd009f1475ff
Create Date: 2020-09-18 12:03:23.571349

"""
from alembic import op
import sqlalchemy as sa
import rdr_service.model.utils


from rdr_service.participant_enums import PhysicalMeasurementsStatus, QuestionnaireStatus, OrderStatus
from rdr_service.participant_enums import WithdrawalStatus, WithdrawalReason, SuspensionStatus, QuestionnaireDefinitionStatus
from rdr_service.participant_enums import EnrollmentStatus, Race, SampleStatus, OrganizationType, BiobankOrderStatus
from rdr_service.participant_enums import OrderShipmentTrackingStatus, OrderShipmentStatus
from rdr_service.participant_enums import MetricSetType, MetricsKey, GenderIdentity
from rdr_service.model.base import add_table_history_table, drop_table_history_table
from rdr_service.model.code import CodeType
from rdr_service.model.site_enums import SiteStatus, EnrollingStatus, DigitalSchedulingStatus, ObsoleteStatus

# revision identifiers, used by Alembic.
revision = 'ee1c8989641b'
down_revision = 'cb2e353a154f'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('participant_summary', sa.Column('baseline_questionnaires_first_complete_authored',
                                                   rdr_service.model.utils.UTCDateTime(), nullable=True))
    op.execute("""
        ALTER TABLE participant_summary MODIFY COLUMN retention_eligible_status smallint GENERATED ALWAYS AS (
        CASE WHEN
          consent_for_study_enrollment = 1
          AND (
            consent_for_electronic_health_records_first_yes_authored is not null OR
            consent_for_dv_electronic_health_records_sharing = 1
          )
          AND questionnaire_on_the_basics = 1
          AND questionnaire_on_overall_health = 1
          AND questionnaire_on_lifestyle = 1
          AND samples_to_isolate_dna = 1
          AND withdrawal_status = 1
          AND suspension_status = 1
          AND deceased_status = 0
        THEN 2 ELSE 1
        END
        ) STORED;
    """)

    op.execute("""
        ALTER TABLE participant_summary MODIFY COLUMN retention_eligible_time datetime GENERATED ALWAYS AS (
        CASE WHEN retention_eligible_status = 2 AND
          COALESCE(sample_status_1ed10_time, sample_status_2ed10_time, sample_status_1ed04_time,
                 sample_status_1sal_time, sample_status_1sal2_time, 0) != 0
        THEN GREATEST(
            GREATEST(consent_for_study_enrollment_first_yes_authored,
             COALESCE(baseline_questionnaires_first_complete_authored,
                 GREATEST(questionnaire_on_the_basics_authored,
                          questionnaire_on_overall_health_authored,
                          questionnaire_on_lifestyle_authored)
             ),
             COALESCE(consent_for_electronic_health_records_first_yes_authored,
             consent_for_study_enrollment_first_yes_authored),
             COALESCE(consent_for_dv_electronic_health_records_sharing_authored,
             consent_for_study_enrollment_first_yes_authored)
            ),
            LEAST(COALESCE(sample_status_1ed10_time, '9999-01-01'),
                COALESCE(sample_status_2ed10_time, '9999-01-01'),
                COALESCE(sample_status_1ed04_time, '9999-01-01'),
                COALESCE(sample_status_1sal_time, '9999-01-01'),
                COALESCE(sample_status_1sal2_time, '9999-01-01')
            )
        ) ELSE NULL
        END
        ) STORED;
    """)
    # ### end Alembic commands ###


def downgrade_rdr():
    # ### commands auto generated by Alembic - please adjust! ###

    op.execute("""
        ALTER TABLE participant_summary MODIFY COLUMN retention_eligible_status smallint GENERATED ALWAYS AS (
        CASE WHEN
          consent_for_study_enrollment = 1
          AND (consent_for_electronic_health_records = 1 OR consent_for_dv_electronic_health_records_sharing = 1)
          AND questionnaire_on_the_basics = 1
          AND questionnaire_on_overall_health = 1
          AND questionnaire_on_lifestyle = 1
          AND samples_to_isolate_dna = 1
          AND withdrawal_status = 1
          AND suspension_status = 1
          AND deceased_status = 0
        THEN 2 ELSE 1
        END
        ) STORED;
        """)
    op.execute("""
        ALTER TABLE participant_summary MODIFY COLUMN retention_eligible_time datetime GENERATED ALWAYS AS (
        CASE WHEN retention_eligible_status = 2 AND
          COALESCE(sample_status_1ed10_time, sample_status_2ed10_time, sample_status_1ed04_time,
                 sample_status_1sal_time, sample_status_1sal2_time, 0) != 0
        THEN GREATEST(
            GREATEST (consent_for_study_enrollment_authored,
             questionnaire_on_the_basics_authored,
             questionnaire_on_overall_health_authored,
             questionnaire_on_lifestyle_authored,
             COALESCE(consent_for_electronic_health_records_authored, consent_for_study_enrollment_authored),
             COALESCE(consent_for_dv_electronic_health_records_sharing_authored, consent_for_study_enrollment_authored)
            ),
            LEAST(COALESCE(sample_status_1ed10_time, '9999-01-01'),
                COALESCE(sample_status_2ed10_time, '9999-01-01'),
                COALESCE(sample_status_1ed04_time, '9999-01-01'),
                COALESCE(sample_status_1sal_time, '9999-01-01'),
                COALESCE(sample_status_1sal2_time, '9999-01-01')
            )
        )
        ELSE NULL
        END
        ) STORED;
        """)
    op.drop_column('participant_summary', 'baseline_questionnaires_first_complete_authored')
    # ### end Alembic commands ###


def upgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_metrics():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
