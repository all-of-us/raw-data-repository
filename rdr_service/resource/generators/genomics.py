#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import re

from dateutil.parser import parse as dt_parse
from sqlalchemy.sql import text

from rdr_service.dao.resource_dao import ResourceDataDao
from rdr_service.genomic_enums import GenomicSetStatus as GenomicSetStatusEnum, \
    GenomicSetMemberStatus as GenomicSetMemberStatusEnum, GenomicValidationFlag as GenomicValidationFlagEnum, \
    GenomicJob as GenomicJobEnum, GenomicWorkflowState as GenomicWorkflowStateEnum, \
    GenomicSubProcessStatus as GenomicSubProcessStatusEnum, GenomicSubProcessResult as GenomicSubProcessResultEnum, \
    GenomicManifestTypes as GenomicManifestTypesEnum, GenomicContaminationCategory as GenomicContaminationCategoryEnum,\
    GenomicQcStatus as GenomicQcStatusEnum
from rdr_service.resource import generators, schemas


class GenomicSetSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicSet resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_set where id = :id'), {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)

            # Populate Enum fields. Note: When Enums have a possible zero value, explicitly check for None.
            if data['genomic_set_status'] is not None:
                enum = GenomicSetStatusEnum(data['genomic_set_status'])
                data['genomic_set_status'] = str(enum)
                data['genomic_set_status_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicSetSchema, data)


def genomic_set_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicSet resource record.
    :param _pk: Primary Key
    :param gen: GenomicSetSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicSetSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_set_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicSetSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_set_update(_pk, gen=gen, w_dao=w_dao)


class GenomicSetMemberSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicSetMember resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_set_member where id = :id'), {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)

            # Convert biobank_id to integer value (if present and if it is a valid biobank_id )
            try:
                bb_id = data['biobank_id']
                data['biobank_id_str'] = bb_id
                # genomic_set_member table may contain pseudo-biobank_id values for control samples (e.g., HG-001)
                # Only populate the PDR biobank_id integer field if there is a valid-looking AoU biobank ID
                # (9 digits or one leading alpha char + 9 digits, where leading char will be stripped)
                if bb_id and re.match("[A-Za-z]?\\d{9}", bb_id):
                    data['biobank_id'] = int(re.sub("[^0-9]", "", bb_id))
                else:
                    data['biobank_id'] = None
            except KeyError:
                pass

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

            # QC Status
            if data['qc_status']:
                enum = GenomicQcStatusEnum(data['qc_status'])
                data['qc_status'] = str(enum)
                data['qc_status_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicSetMemberSchema, data)


def genomic_set_member_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicSetMember Resource record.
    :param _pk: Primary Key
    :param gen: GenomicSetMemberSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicSetMemberSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_set_member_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicSetMemberSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_set_member_update(_pk, gen=gen, w_dao=w_dao)


class GenomicJobRunSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicJobRun resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_job_run where id = :id'), {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)
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


def genomic_job_run_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicJobRun record.
    :param _pk: Primary Key
    :param gen: GenomicJobRunSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicJobRunSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_job_run_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicJobRunSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_job_run_update(_pk, gen=gen, w_dao=w_dao)


class GenomicFileProcessedSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicFileProcessed resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_file_processed where id = :id'), {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)
            # Populate Enum fields.
            if data['file_status']:
                enum = GenomicSubProcessStatusEnum(data['file_status'])
                data['file_status'] = str(enum)
                data['file_status_id'] = int(enum)
            if data['file_result']:
                enum = GenomicSubProcessResultEnum(data['file_result'])
                data['file_result'] = str(enum)
                data['file_result_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicFileProcessedSchema, data)


def genomic_file_processed_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicFileProcessed record.
    :param _pk: Primary Key
    :param gen: GenomicFileProcessedSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicFileProcessedSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_file_processed_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicFileProcessedSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_file_processed_update(_pk, gen=gen, w_dao=w_dao)


class GenomicManifestFileSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicManifestFile resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_manifest_file where id = :id'), {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)
            # Populate Enum fields.
            if data['manifest_type_id']:
                enum = GenomicManifestTypesEnum(data['manifest_type_id'])
                data['manifest_type'] = str(enum)
                data['manifest_type_id_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicManifestFileSchema, data)


def genomic_manifest_file_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicManifestFile record.
    :param _pk: Primary Key
    :param gen: GenomicManifestFileSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicManifestFileSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_manifest_file_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicManifestFileSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_manifest_file_update(_pk, gen=gen, w_dao=w_dao)


class GenomicManifestFeedbackSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicManifestFeedback resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from genomic_manifest_feedback where id = :id'),
                {'id': _pk}
            ).first()
            data = self.ro_dao.to_dict(row)

            return generators.ResourceRecordSet(schemas.GenomicManifestFeedbackSchema, data)


def genomic_manifest_feedback_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicManifestFeedback record.
    :param _pk: Primary Key
    :param gen: GenomicManifestFeedbackSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicManifestFeedbackSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_manifest_feedback_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicManifestFeedbackSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_manifest_feedback_update(_pk, gen=gen, w_dao=w_dao)


class GenomicGCValidationMetricsSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicGCValidationMetrics resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_gc_validation_metrics where id = :id'),
                                     {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)

            # Populate Enum fields.
            if data['contamination_category']:
                enum = GenomicContaminationCategoryEnum(data['contamination_category'])
                data['contamination_category'] = str(enum)
                data['contamination_category_id'] = int(enum)

            return generators.ResourceRecordSet(schemas.GenomicGCValidationMetricsSchema, data)


def genomic_gc_validation_metrics_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicGCValidationMetrics resource record.
    :param _pk: Primary Key
    :param gen: GenomicGCValidationMetricsSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicGCValidationMetricsSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_gc_validation_metrics_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicGCValidationMetricsSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_gc_validation_metrics_update(_pk, gen=gen, w_dao=w_dao)


class GenomicUserEventMetricsSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicUserEventMetrics resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from user_event_metrics where id = :id'),
                                     {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)
            data['created_at'] = dt_parse(data['created_at'])

            return generators.ResourceRecordSet(schemas.GenomicUserEventMetricsSchema, data)


def genomic_user_event_metrics_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicGCValidationMetrics resource record.
    :param _pk: Primary Key
    :param gen: GenomicUserEventMetricsSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicUserEventMetricsSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_user_event_metrics_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicUserEventMetricsSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_user_event_metrics_update(_pk, gen=gen, w_dao=w_dao)


class GenomicInformingLoopSchemaGenerator(generators.BaseGenerator):
    """
    Generate a GenomicInformingLoop resource object
    """
    ro_dao = None

    def make_resource(self, _pk, backup=False):
        """
        Build a resource object from the given primary key id.
        :param _pk: Primary key value from rdr table.
        :param backup: if True, get from backup database instead of Primary.
        :return: resource object
        """
        if not self.ro_dao:
            self.ro_dao = ResourceDataDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_informing_loop where id = :id'),
                                     {'id': _pk}).first()
            data = self.ro_dao.to_dict(row)
            return generators.ResourceRecordSet(schemas.GenomicInformingLoopSchema, data)


def genomic_informing_loop_update(_pk, gen=None, w_dao=None):
    """
    Generate GenomicInformingLoop resource record.
    :param _pk: Primary Key
    :param gen: GenomicInformingLoopSchemaGenerator object
    :param w_dao: Writable DAO object.
    """
    if not gen:
        gen = GenomicInformingLoopSchemaGenerator()
    res = gen.make_resource(_pk)
    res.save(w_dao=w_dao)


def genomic_informing_loop_batch_update(_pk_ids):
    """
    Generate a batch of ids.
    :param _pk_ids: list of pk ids.
    """
    gen = GenomicInformingLoopSchemaGenerator()
    w_dao = ResourceDataDao()
    for _pk in _pk_ids:
        genomic_informing_loop_update(_pk, gen=gen, w_dao=w_dao)
