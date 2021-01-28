#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime
from sqlalchemy.sql import text

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao, BigQueryGenerator
from rdr_service.model.bq_base import BQRecord
from rdr_service.model.bq_genomics import \
    BQGenomicSetSchema, BQGenomicSet, \
    BQGenomicSetMemberSchema, BQGenomicSetMember, \
    BQGenomicJobRunSchema, BQGenomicJobRun, \
    BQGenomicGCValidationMetricsSchema, BQGenomicGCValidationMetrics, \
    GenomicSetStatusEnum, GenomicSetMemberStatusEnum, GenomicValidationFlag, GenomicSubProcessStatusEnum, \
    GenomicSubProcessResultEnum, GenomicJobEnum, GenomicWorkflowStateEnum, BQGenomicFileProcessedSchema, \
    BQGenomicFileProcessed, GenomicQcStatusEnum, GenomicContaminationCategoryEnum, GenomicManifestTypesEnum, \
    BQGenomicManifestFileSchema, BQGenomicManifestFile, BQGenomicManifestFeedbackSchema, BQGenomicManifestFeedback


class BQGenomicSetGenerator(BigQueryGenerator):
    """
    Generate a GenomicSet BQRecord object
    """

    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_set where id = :id'), {'id': _id}).first()
            data = self.ro_dao.to_dict(row)

            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']
            data['orig_created'] = data['created']
            data['orig_modified'] = data['modified']
            data['created'] = data['modified'] = datetime.utcnow()

            # Populate Enum fields.
            if data['genomic_set_status']:
                enum = GenomicSetStatusEnum(data['genomic_set_status'])
                data['genomic_set_status'] = enum.name
                data['genomic_set_status_id'] = enum.value

            return BQRecord(schema=BQGenomicSetSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_set_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate GenomicSet record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicSetGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicSet, w_dao=w_dao, w_session=w_session, project_id=project_id)


def bq_genomic_set_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicSetGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_set_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQGenomicSetMemberSchemaGenerator(BigQueryGenerator):
    """
    Generate a GenomicSetMember BQRecord object
    """
    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_set_member where id = :id'), {'id': _id}).first()
            data = self.ro_dao.to_dict(row)

            # Set biobank_id_str and delete biobank_id
            try:
                data['biobank_id_str'] = data['biobank_id']
                del data['biobank_id']
            except KeyError:
                pass

            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']
            data['orig_created'] = data['created']
            data['orig_modified'] = data['modified']

            # Populate Enum fields.
            if data['validation_status']:
                enum = GenomicSetMemberStatusEnum(data['validation_status'])
                data['validation_status'] = enum.name
                data['validation_status_id'] = enum.value
            # validation_flags contains a comma delimited list of enum values.
            if data['validation_flags']:
                flags = data['validation_flags'].strip().split(',')
                data['validation_flags'] = ','.join([GenomicValidationFlag(int(f)).name for f in flags])
            if data['genomic_workflow_state']:
                enum = GenomicWorkflowStateEnum(data['genomic_workflow_state'])
                data['genomic_workflow_state'] = enum.name
                data['genomic_workflow_state_id'] = enum.value

            # QC Status
            if data['qc_status']:
                enum = GenomicQcStatusEnum(data['qc_status'])
                data['qc_status'] = enum.name
                data['qc_status_id'] = enum.value

            return BQRecord(schema=BQGenomicSetMemberSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_set_member_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate GenomicSetMember record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicSetMemberSchemaGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicSetMember, w_dao=w_dao, w_session=w_session, project_id=project_id)


def bq_genomic_set_member_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicSetMemberSchemaGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_set_member_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQGenomicJobRunSchemaGenerator(BigQueryGenerator):
    """
    Generate a GenomicJobRun BQRecord object
    """
    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_job_run where id = :id'), {'id': _id}).first()
            data = self.ro_dao.to_dict(row)

            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']

            # Populate Enum fields.
            # column data is in 'job_id', instead of 'job' for this one.
            if data['job_id']:
                enum = GenomicJobEnum(data['job_id'])
                data['job'] = enum.name
                data['job_id'] = enum.value
            if data['run_status']:
                enum = GenomicSubProcessStatusEnum(data['run_status'])
                data['run_status'] = enum.name
                data['run_status_id'] = enum.value
            if data['run_result']:
                enum = GenomicSubProcessResultEnum(data['run_result'])
                data['run_result'] = enum.name
                data['run_result_id'] = enum.value

            return BQRecord(schema=BQGenomicJobRunSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_job_run_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate GenomicJobRun record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicJobRunSchemaGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicJobRun, w_dao=w_dao, w_session=w_session, project_id=project_id)


def bq_genomic_job_run_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicJobRunSchemaGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_job_run_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQGenomicFileProcessedSchemaGenerator(BigQueryGenerator):
    """
    Generate a BQGenomicFileProcessed BQRecord object
    """
    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_file_processed where id = :id'), {'id': _id}).first()
            data = self.ro_dao.to_dict(row)

            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']

            # Populate Enum fields.
            if data['file_status']:
                enum = GenomicSubProcessStatusEnum(data['file_status'])
                data['file_status'] = enum.name
                data['file_status_id'] = enum.value
            if data['file_result']:
                enum = GenomicSubProcessResultEnum(data['file_result'])
                data['file_result'] = enum.name
                data['file_result_id'] = enum.value

            return BQRecord(schema=BQGenomicFileProcessedSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_file_processed_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate BQGenomicFileProcessed record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicFileProcessedSchemaGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicFileProcessed, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_genomic_file_processed_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicFileProcessedSchemaGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_file_processed_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQGenomicManifestFileSchemaGenerator(BigQueryGenerator):
    """
    Generate a BQGenomicManifestFile BQRecord object
    """
    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_manifest_file where id = :id'), {'id': _id}).first()
            data = self.ro_dao.to_dict(row)

            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']

            # Populate Enum fields.
            if data['manifest_type_id']:
                enum = GenomicManifestTypesEnum(data['manifest_type_id'])
                data['manifest_type'] = enum.name
                data['manifest_type_id'] = enum.value

            return BQRecord(schema=BQGenomicManifestFileSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_manifest_file_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate BQGenomicManifestFile record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicManifestFileSchemaGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicManifestFile, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_genomic_manifest_file_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicManifestFileSchemaGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_manifest_file_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQGenomicManifestFeedbackSchemaGenerator(BigQueryGenerator):
    """
    Generate a BQGenomicManifestFeedback BQRecord object
    """
    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(
                text('select * from genomic_manifest_feedback where id = :id'),
                {'id': _id}
            ).first()
            data = self.ro_dao.to_dict(row)

            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']

            return BQRecord(schema=BQGenomicManifestFeedbackSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_manifest_feedback_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate BQGenomicManifestFeedback record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicManifestFeedbackSchemaGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicManifestFeedback, w_dao=w_dao,
                          w_session=w_session, project_id=project_id)


def bq_genomic_manifest_feedback_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicManifestFeedbackSchemaGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_manifest_feedback_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)


class BQGenomicGCValidationMetricsSchemaGenerator(BigQueryGenerator):
    """
    Generate a GenomicGCValidationMetrics BQRecord object
    """
    ro_dao = None

    def make_bqrecord(self, _id, convert_to_enum=False, backup=False):
        """
        Build a BQRecord object from the given primary key id.
        :param _id: Primary key value from rdr table.
        :param convert_to_enum: If schema field description includes Enum class info, convert value to Enum.
        :param backup: if True, get from backup database
        :return: BQRecord object
        """
        if not self.ro_dao:
            self.ro_dao = BigQuerySyncDao(backup=backup)

        with self.ro_dao.session() as ro_session:
            row = ro_session.execute(text('select * from genomic_gc_validation_metrics where id = :id'),
                                     {'id': _id}).first()
            data = self.ro_dao.to_dict(row)
            # PDR-149:  Preserve id values from RDR
            data['orig_id'] = data['id']
            data['orig_created'] = data['created']
            data['orig_modified'] = data['modified']

            # Populate Enum fields.
            if data['contamination_category']:
                enum = GenomicContaminationCategoryEnum(data['contamination_category'])
                data['contamination_category'] = enum.name
                data['contamination_category_id'] = enum.value

            return BQRecord(schema=BQGenomicGCValidationMetricsSchema, data=data, convert_to_enum=convert_to_enum)


def bq_genomic_gc_validation_metrics_update(_id, project_id=None, gen=None, w_dao=None):
    """
    Generate GenomicGCValidationMetrics record for BQ.
    :param _id: Primary Key
    :param project_id: Override the project_id
    :param gen: BQGenomicSetGenerator object
    :param w_dao: writeable dao object.
    """
    if not gen:
        gen = BQGenomicGCValidationMetricsSchemaGenerator()
    if not w_dao:
        w_dao = BigQuerySyncDao()

    bqr = gen.make_bqrecord(_id)
    with w_dao.session() as w_session:
        gen.save_bqrecord(_id, bqr, bqtable=BQGenomicGCValidationMetrics, w_dao=w_dao, w_session=w_session,
                          project_id=project_id)


def bq_genomic_gc_validation_metrics_batch_update(_ids, project_id=None):
    """
    Update a batch of ids.
    :param _ids: list of ids
    :param project_id: Override the project_id
    """
    gen = BQGenomicGCValidationMetricsSchemaGenerator()
    w_dao = BigQuerySyncDao()
    for _id in _ids:
        bq_genomic_gc_validation_metrics_update(_id, project_id=project_id, gen=gen, w_dao=w_dao)
