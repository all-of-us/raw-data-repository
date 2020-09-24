#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from sqlalchemy.sql import text

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.participant_enums import (
    GenomicSetStatus as GenomicSetStatusEnum,
    GenomicSetMemberStatus as GenomicSetMemberStatusEnum,
    GenomicValidationFlag as GenomicValidationFlagEnum,
    GenomicSubProcessStatus as GenomicSubProcessStatusEnum,
    GenomicSubProcessResult as GenomicSubProcessResultEnum,
    GenomicJob as GenomicJobEnum,
    GenomicWorkflowState as GenomicWorkflowStateEnum
)
from rdr_service.resource import generators, schemas


class GenomicSetSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicSet resource object
    """

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        ro_dao = ResourceDataDao(backup=backup)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_set where id = :id'), {'id': _pk}).first()
            data = ro_dao.to_dict(row)

            # Populate Enum fields.
            if data['genomic_set_status']:
                enum = GenomicSetStatusEnum(data['genomic_set_status'])
                data['genomic_set_status'] = str(enum)
                data['genomic_set_status_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicSetSchema, data)


def genomic_set_update(_pk):
    """
    Generate GenomicSet resource record.
    :param _pk: Primary Key
    """
    gen = GenomicSetSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save()


class GenomicSetMemberSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicSetMember resource object
    """

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        ro_dao = ResourceDataDao(backup=backup)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_set_member where id = :id'), {'id': _pk}).first()
            data = ro_dao.to_dict(row)

            # Populate Enum fields.
            if data['validation_status']:
                enum = GenomicSetMemberStatusEnum(data['validation_status'])
                data['validation_status'] = str(enum)
                data['validation_status_id'] = int(enum)
            # validation_flags contains a comma delimited list of enum values.
            if data['validation_flags']:
                flags = data['validation_flags'].strip().split(',')
                data['validation_flags'] = ','.join([str(GenomicValidationFlagEnum(int(f))) for f in flags])
            if data['genomic_workflow_state']:
                enum = GenomicWorkflowStateEnum(data['genomic_workflow_state'])
                data['genomic_workflow_state'] = str(enum)
                data['genomic_workflow_state_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicSetMemberSchema, data)


def genomic_set_member_update(_pk):
    """
    Generate GenomicSetMember Resource record.
    :param _pk: Primary Key
    """
    gen = GenomicSetMemberSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save()


class GenomicJobRunSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicJobRun resource object
    """

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        ro_dao = ResourceDataDao(backup=backup)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_job_run where id = :id'), {'id': _pk}).first()
            data = ro_dao.to_dict(row)
            # Populate Enum fields.
            # column data is in 'job_id', instead of 'job' for this one.
            if data['job_id']:
                enum = GenomicJobEnum(data['job_id'])
                data['job'] = str(enum)
                data['job_id'] = int(enum)
            if data['run_status']:
                enum = GenomicSubProcessStatusEnum(data['run_status'])
                data['run_status'] = str(enum)
                data['run_status_id'] = int(enum)
            if data['run_result']:
                enum = GenomicSubProcessResultEnum(data['run_result'])
                data['run_result'] = str(enum)
                data['run_result_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicJobRunSchema, data)


def genomic_job_run_update(_pk):
    """
    Generate GenomicJobRun record.
    :param _pk: Primary Key
    """
    gen = GenomicJobRunSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save()


class GenomicGCValidationMetricsSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicGCValidationMetrics resource object
    """

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        ro_dao = ResourceDataDao(backup=backup)
        with ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_gc_validation_metrics where id = :id'),
                                     {'id': _pk}).first()
            data = ro_dao.to_dict(row)

            return generators.ResourceRecordSet(schemas.GenomicGCValidationMetricsSchema, data)


def genomic_gc_validation_metrics_update(_pk):
    """
    Generate GenomicGCValidationMetrics resource record.
    :param _pk: Primary Key
    """
    gen = GenomicGCValidationMetricsSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save()
