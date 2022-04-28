# pylint: disable=unused-import
import faker
import os
import logging

from sqlalchemy import literal
from sqlalchemy.inspection import inspect
from rdr_service import clock
from rdr_service.dao import database_factory
from rdr_service.dao.genomic_datagen_dao import GenomicDateGenCaseTemplateDao, GenomicDataGenRunDao, \
    GenomicDataGenMemberRunDao, GenomicDataGenOutputTemplateDao, GenomicDataGenManifestSchemaDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicJobRunDao, GenomicResultWorkflowStateDao, \
    GenomicGCValidationMetricsDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider, ManifestCompiler
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessStatus, GenomicSubProcessResult, \
    ResultsWorkflowState, GenomicManifestTypes
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.genomics import (
    GenomicSetMember,
    GenomicResultWorkflowState,
    GenomicGCValidationMetrics,
    GenomicCVLAnalysis,
    GenomicCVLSecondSample
)
from tests.helpers.data_generator import DataGenerator


class GeneratorMixin:

    @staticmethod
    def _get_clean_field_list(string):
        return [val for val in string.split('%') if val != '']

    @staticmethod
    def validate_template_records(
        records,
        template_type,
        validation_step,
        project='cvl',
        external_values=None
    ):
        if not records:
            raise Exception(f"{template_type} template records were not found for project: {project}")

        if validation_step == 'generation':
            source_types = ['system', 'external', 'literal', 'calculated']
            for source_type in source_types:
                source_records = list(filter(lambda x: x.field_source == source_type, records))
                for record in source_records:
                    value = record.field_value
                    source = record.field_source

                    if value and source == 'system':
                        raise Exception(f'System sources cannot have values, Record ID: {record.id}')
                    if not value and (source == 'literal' or source == 'calculated' or source == 'external'):
                        raise Exception(f'Literal/Calculated sources require value, Record ID: {record.id}')
                    if source == 'external' and external_values.get(value) is None:
                        raise Exception(f'External key was not found, Record ID: {record.id}')

        if validation_step == 'output':
            source_types = ['literal', 'model']
            for source_type in source_types:
                source_records = list(filter(lambda x: x.source_type == source_type, records))
                for record in source_records:
                    value = record.source_value
                    source = record.source_type
                    if not value:
                        raise Exception(f'Literal/Model sources require value, Record ID: {record.id}')
                    if source == 'model':
                        if '.' not in value and len(value.split('.')) != 2:
                            raise Exception(f'Model source format is incorrect, Record ID: {record.id}')


class ParticipantGenerator(GeneratorMixin):
    def __init__(
        self,
        project='cvl',
        logger=None
    ):
        self.project = project
        self.num_participants = None
        self.template_type = None
        self.external_values = None
        self.logger = logger or logging

        self.member_ids = []
        self.default_template_records = []
        self.template_records = []
        self.default_table_map = {}

    def __enter__(self):
        self.data_generator = self.initialize_data_generator()
        self.genomic_set = self._set_genomic_set()

        # init daos
        self.participant_dao = ParticipantDao()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.genomic_set_member_dao = GenomicSetMemberDao()
        self.genomic_gc_validation_metrics_dao = GenomicGCValidationMetricsDao()

        self.base_daos = list(filter(lambda x: '_dao' in x and 'datagen' not in x, vars(self).keys()))

        for dao in self.base_daos:
            map_key = dao.split('_dao')[0]
            self.default_table_map[map_key] = {}

        self.datagen_template_dao = GenomicDateGenCaseTemplateDao()
        self.datagen_run_dao = GenomicDataGenRunDao()
        self.datagen_member_run_dao = GenomicDataGenMemberRunDao()

        run_obj = self.datagen_run_dao.model_type()
        run_obj.project_name = self.project
        self.run_obj = self.datagen_run_dao.insert(run_obj)

        return self

    def __exit__(self, *_, **__):
        ...

    @staticmethod
    def initialize_data_generator():
        session = database_factory.get_database().make_session()
        fake = faker.Faker()
        return DataGenerator(session, fake)

    @staticmethod
    def convert_case(string_value):
        converted = string_value[0].lower() + string_value.title()[1:].replace("_", "")
        if 'Ror' in converted:
            converted, _ = converted.split('Ror')
            converted = converted + 'ROR'
        return converted

    def build_participant_default(self):
        if not self.default_template_records:
            self.default_template_records = self.datagen_template_dao.get_default_template_records(
                project=self.project
            )
            # will throw exception for invalid expected data struct
            self.validate_template_records(
                records=self.default_template_records,
                template_type=self.template_type,
                validation_step='generation',
                external_values=self.external_values
            )

        base_participant = None
        for table in self.default_table_map:
            # make sure it has generator, will throw exception if not
            generator_method = self._get_generator_method(table)

            if table == 'participant':
                if os.environ.get('UNITTEST_FLAG') == "1":
                    base_participant = generator_method()
                else:
                    participant_id = self.genomic_set_member_dao.get_random_id()
                    biobank_id = self.genomic_set_member_dao.get_random_id()
                    research_id = self.genomic_set_member_dao.get_random_id()
                    base_participant = generator_method(
                        participantId=participant_id,
                        biobankId=biobank_id,
                        researchId=research_id
                    )
                self.default_table_map[table]['obj'] = base_participant
                continue

            current_table_defaults = list(
                filter(lambda x: x.rdr_field.split('.')[0].lower() == table, self.default_template_records)
            )

            if current_table_defaults:
                attr_dict = self._get_type_attr_dict(current_table_defaults)

                if table == 'participant_summary' and base_participant:
                    attr_dict['participant'] = base_participant
                if table == 'genomic_set_member' and self.genomic_set:
                    attr_dict['genomicSetId'] = self.genomic_set.id

                try:
                    generated_obj = generator_method(**attr_dict)
                    self.default_table_map[table]['obj'] = generated_obj
                    if table == 'genomic_set_member':
                        self.member_ids.append(generated_obj.id)

                except Exception as error:
                    raise Exception(f'Error when inserting default records: {error}')

    def build_participant_type_records(self):
        if self.template_type == 'default':
            return

        if not self.template_records:
            self.template_records = self.datagen_template_dao.get_template_records_template(
                project=self.project,
                template_type=self.template_type
            )
            # will throw exception for invalid expected data struct
            self.validate_template_records(
                records=self.template_records,
                template_type=self.template_type,
                validation_step='generation',
                external_values=self.external_values
            )

        # if template records have attributes from multiple tables
        table_names = set([obj.rdr_field.split('.')[0].lower() for obj in self.template_records])

        # loop in externals not in template
        if 'genomic_informing_loop' not in table_names \
                and any('informing_loop_' in key for key in self.external_values.keys()):
            self.generate_loop_records(
                'genomic_informing_loop'
            )

        for table in table_names:
            current_table_attrs = list(
                filter(lambda x: x.rdr_field.split('.')[0].lower() == table and x.field_source != 'bypass',
                       self.template_records)
            )

            if current_table_attrs:
                # loop in externals and in template
                if table == 'genomic_informing_loop':
                    self.generate_loop_records(table, current_table_attrs)
                    continue

                case, need_update_exisiting_obj = False, False

                if table in self.default_table_map.keys():
                    case, need_update_exisiting_obj = True, True

                attr_dict = self._get_type_attr_dict(current_table_attrs, case=case)

                if need_update_exisiting_obj:
                    dao_name = list(filter(lambda x: x.split('_dao')[0] == table, self.base_daos))[0]
                    dao = getattr(self, dao_name)
                    current_obj = self.default_table_map.get(table, {}).get('obj')
                    current_obj.__dict__.update(**attr_dict)
                    dao.update(current_obj)
                    return

                self.generate_type_records(table, attr_dict)

    def generate_loop_records(self, table, current_table_attrs=None):
        loop_dict = {key: value for key, value in self.external_values.items() if 'informing_loop_' in key}

        for loop_type, loop_value in loop_dict.items():
            module_type = f'{loop_type.split("_", 2)[-1]}'
            non_external_attrs = [obj for obj in current_table_attrs if obj.field_source != 'external'] if \
                current_table_attrs else []

            attr_dict = self._get_type_attr_dict(non_external_attrs, case=False)

            # implicit for now
            if not attr_dict.get('participant_id'):
                participant_id = self.default_table_map['participant']['obj'].participantId
                attr_dict['participant_id'] = participant_id

            attr_dict['module_type'] = module_type
            attr_dict['decision_value'] = loop_value

            self.generate_type_records(table, attr_dict)

    def generate_type_records(self, table, attr_dict):
        generator_method = self._get_generator_method(table)
        try:
            generator_method(**attr_dict)
        except Exception as error:
            raise Exception(f'Error when inserting template type records: {error}')

    def evaluate_value(self, field_source, field_value):
        field_source = field_source.lower()
        if field_source == 'literal':
            return field_value
        if field_source == 'external':
            return self._get_from_external_data(field_value)
        if field_source == 'calculated':
            return self._calc_algo_value(field_value)

    def _get_type_attr_dict(self, table_attrs, case=True):
        attr_dict = {}
        for obj in table_attrs:
            field_name = obj.rdr_field.split('.')[-1].lower()
            value = self.evaluate_value(
                obj.field_source,
                obj.field_value
            )
            attr_dict[self.convert_case(field_name) if case else field_name] = value
        return attr_dict

    def _get_generator_method(self, table_name):
        gen_method_name = f'create_database_{table_name}'
        try:
            generator_method = getattr(self.data_generator, gen_method_name)
        except AttributeError:
            raise Exception(f"Cannot find generator for table: {table_name}")

        return generator_method

    def _calc_algo_value(self, field_value):
        # field_value should be %<table_name>.<snake_case_attr>%
        parsed_list, new_list = self._get_clean_field_list(field_value), []

        for val in parsed_list:
            if len(val.split('.')) == 2 and val.split('.')[0] in self.default_table_map.keys():
                table, attribute = val.split('.')
                # check in default map should contain all calc attrs
                obj = self.default_table_map.get(table, {}).get('obj')
                if not obj:
                    raise Exception(f'Cannot find object for calculated value: {field_value}')
                attr_value = getattr(obj, self.convert_case(attribute))

                if not attr_value or attr_value is None:
                    raise Exception(f'Cannot find attribute {attribute} in obj')

                new_list.append(str(attr_value))
            else:
                new_list.append(val)
        return ''.join(new_list)

    def _get_from_external_data(self, field_name):
        external_value = self.external_values.get(field_name)
        if external_value is None:
            raise Exception(f'Value for external field {field_name} is not found')

        return external_value

    def _set_genomic_set(self):
        return self.data_generator.create_database_genomic_set(
            genomicSetName=f"generator_{clock.CLOCK.now()}",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def run_participant_creation(self, **kwargs):
        self.num_participants = kwargs.get('num_participants')
        self.template_type = kwargs.get('template_type')
        self.external_values = kwargs.get('external_values')
        self.member_ids, self.template_records = [], []

        self.logger.info(f'Running template type {self.template_type} for {self.num_participants} participants.')

        for _ in range(self.num_participants):
            self.build_participant_default()
            self.build_participant_type_records()

        self.datagen_member_run_dao.batch_insert_member_records(
            self.run_obj.id,
            self.template_type,
            self.member_ids
        )

        self.logger.info(f'{self.template_type}: {self.num_participants} created.')


class GeneratorOutputTemplate(GeneratorMixin):
    def __init__(
        self,
        output_template_name,
        output_run_id=None,
        output_sample_ids=None,
        project='cvl',
    ):
        self.output_template_name = output_template_name
        self.output_run_id = output_run_id
        self.output_sample_ids = output_sample_ids
        self.project = project
        self.data_records = None

        self.loop_string = 'informing_loop_'

        self.output_template_dao = GenomicDataGenOutputTemplateDao()
        self.datagen_run_dao = GenomicDataGenRunDao()

    def _get_output_template(self):
        self.output_template_records = self.output_template_dao.get_output_template_records(
            project=self.project,
            template_type=self.output_template_name
        )
        self.validate_template_records(
            records=self.output_template_records,
            template_type=self.output_template_name,
            validation_step='output'
        )

    def build_output_records(self):
        template_records = []
        for data_record in self.data_records:
            row_dict = {}
            for record in self.output_template_records:
                value = None
                if record.source_type == 'literal':
                    value = record.source_value
                elif record.source_type == 'model':
                    attribute = record.source_value.split('.')[-1]
                    value = getattr(data_record, attribute)
                row_dict[record.field_name] = self._parse_enum_strings(value)
            template_records.append(row_dict)
        return template_records

    def _get_template_model_source(self):
        calc_records = list(filter(
            lambda x: self.loop_string not in x.field_name
                      and x.source_type == 'model',
            self.output_template_records)
        )
        attr_records = [obj.source_value for obj in calc_records]
        return attr_records

    def _get_loop_types_from_template_records(self):
        return [obj.field_name.split('_', 2)[-1] for obj in self.output_template_records if self.loop_string in
                obj.field_name]

    @staticmethod
    def _parse_enum_strings(value):
        if not hasattr(value, 'name'):
            return value
        return value.name.lower()

    def run_output_creation(self):
        self._get_output_template()
        loop_types = self._get_loop_types_from_template_records()
        attr_records = self._get_template_model_source()
        if not any('GenomicSetMember' in obj for obj in attr_records):
            raise Exception('Attribute for GenomicSetMember required')

        self.data_records = self.datagen_run_dao.get_output_template_data(
            attr_records=attr_records,
            datagen_run_id=self.output_run_id,
            sample_ids=self.output_sample_ids,
            loop_types=loop_types
        )

        if not self.data_records:
            exception_msg = None
            if self.output_run_id:
                exception_msg = f'No records for run id: {self.output_run_id} were found'
            if self.output_sample_ids:
                exception_msg = f'No records for sample ids: {self.output_sample_ids} were found'
            raise Exception(exception_msg)
        return self.build_output_records()


class ManifestGenerator:
    def __init__(
        self,
        project_name='cvl',
        template_name=None,
        sample_ids=None,
        update_samples=False,
        member_run_id_column=None,
        cvl_site_id=None,
        logger=logging,
        biobank_id_prefix=None
    ):
        # Params
        self.project_name = project_name
        self.template_name = template_name
        self.sample_ids = sample_ids
        self.update_samples = update_samples
        self.member_run_id_column = member_run_id_column  # only used for testing
        self.cvl_site_id = cvl_site_id
        self.logger = logger
        self.biobank_id_prefix = biobank_id_prefix or 'Z'

        # Job vars
        self.job = GenomicJob.DATAGEN_MANIFEST_GENERATION
        self.job_run = None
        self.member_run_id_attribute = None
        self.pipeline_state = None

        # Dao
        self.manifest_datagen_dao = GenomicDataGenManifestSchemaDao()
        self.manifest_compiler = None
        self.manifest_def_provider = None

        # Operational vars
        self.template_fields = None
        self.internal_manifest_type_name = f"{self.project_name.upper()}_{self.template_name}"

        self.run_results = {
            "status": None,
            "message": "",
            "manifest_data": []
        }

    def __enter__(self):
        if self.update_samples:
            # Create job run ID like controller
            self.job_run_dao = GenomicJobRunDao()
            self.job_run = self.job_run_dao.insert_run_record(self.job)
            self.member_dao = GenomicSetMemberDao()
            self.results_state_dao = GenomicResultWorkflowStateDao()

            self._set_member_run_id_attribute()
            self._set_pipeline_state()

        return self

    def __exit__(self, *_, **__):
        if self.update_samples:
            self.job_run_dao.update_run_record(self.job_run.id,
                                               self.run_results['status'],
                                               GenomicSubProcessStatus.COMPLETED)

    def generate_manifest_data(self):
        manifest_query_result = None
        # Lookup in template table
        self.template_fields = self.manifest_datagen_dao.get_template_by_name(
            self.project_name,
            self.template_name)

        if self.template_fields:
            self.logger.info("Manifest name found in template table. Running external manifest.")

            if not self.sample_ids:
                self.run_results['status'] = GenomicSubProcessResult.ERROR
                self.run_results['message'] = f"Sample IDs required for external manifest."
                return self.run_results

            manifest_query_result = self.execute_external_manifest_query()

        else:
            # Get manifest definitions and set site
            site = "rdr"
            if self.cvl_site_id:
                if self.cvl_site_id == 'bi':
                    site = 'co'
                else:
                    site = self.cvl_site_id

            self.manifest_def_provider = ManifestDefinitionProvider(
                cvl_site_id=site, kwargs={}
            )

            # check manifest defs
            rdr_manifest_names = [a.name for a in self.manifest_def_provider.manifest_columns_config.keys()]

            if self.internal_manifest_type_name in rdr_manifest_names:
                self.logger.info("Manifest name found in RDR defs.")
                manifest_query_result = self.execute_internal_manifest_query()

            else:
                self.run_results['status'] = GenomicSubProcessResult.ERROR
                self.run_results['message'] = f"Template name not valid: {self.template_name}."

        if manifest_query_result:
            self.run_results['manifest_data'] = [self.manifest_datagen_dao.to_dict(result)
                                                 if not isinstance(result, dict) else result
                                                 for result in manifest_query_result]
            # Add biobank_id prefix
            for result in self.run_results['manifest_data']:
                bid_field = "biobank_id"
                if "biobankid" in result.keys():
                    bid_field = "biobankid"

                if result.get(bid_field)[0] != self.biobank_id_prefix:
                    result[bid_field] = f"{self.biobank_id_prefix}{result[bid_field]}"

            self.run_results['message'] = f"Completed {self.template_name} Manifest Generation. "
            self.run_results['message'] += f"{self.template_name} Manifest Included" \
                                           f" {len(self.run_results['manifest_data'])} records."

            self.run_results['status'] = GenomicSubProcessResult.SUCCESS

            if not self.run_results.get('output_filename'):
                dt = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
                self.run_results.update(
                    {'output_filename':
                        f"{self.template_name}_manifests/RDR_AoU_CVL_{self.template_name}_{dt}.csv"}
                )

            if self.update_samples:
                self.update_samples_status()

        return self.run_results

    def execute_external_manifest_query(self):
        # Get fields to pull for query
        columns = []

        for field in self.template_fields:
            if field.source_type == 'model':
                columns.append(self._prepare_model_column(field))

            elif field.source_type == 'literal':
                columns.append(self._prepare_literal_column(field))

            else:
                self.run_results['status'] = GenomicSubProcessResult.ERROR
                self.run_results['message'] = f"Invalid source_type: {field.source_type}."

                return

        # Run the manifest query for sample IDs.
        return self.manifest_datagen_dao.execute_manifest_query(columns, self.sample_ids, self.cvl_site_id)

    def execute_internal_manifest_query(self):
        self.manifest_compiler = ManifestCompiler()
        manifest_type = GenomicManifestTypes.lookup_by_name(self.internal_manifest_type_name)

        self.manifest_compiler.manifest_def = self.manifest_def_provider.get_def(manifest_type)

        if self.sample_ids:
            # For executing only for specific sample ids,
            # the query must have the filter set up
            self.manifest_compiler.manifest_def.params.update({'sample_ids': self.sample_ids})

        manifest_data = self.manifest_compiler.pull_source_data()

        if not manifest_data:
            self.run_results['status'] = GenomicSubProcessResult.NO_FILES
            self.run_results['status'] = f"No records found for {self.template_name} manifest"
            return

        # Manifest data found, set filename
        self.run_results.update(
            {'output_filename': self.manifest_compiler.manifest_def.output_filename}
        )

        # Headers are RDR attributes, so need to remap to column names
        headers = self.manifest_compiler.manifest_def.columns
        manifest_data_with_headers = []

        for result in manifest_data:
            remapped_result = {k: v for k, v in zip(headers, result)}
            manifest_data_with_headers.append(remapped_result)

        return manifest_data_with_headers

    def update_samples_status(self):
        if self.run_results['status'] != GenomicSubProcessResult.SUCCESS:
            self.run_results['status'] = GenomicSubProcessResult.ERROR
            self.run_results['messge'] = "Cannot update samples in RDR. Manifest data pull was unsuccessful."

            return self.run_results

        if not self.sample_ids:
            # All Internal manifests have sample_id
            self.sample_ids = [result.get('sample_id') for result in self.run_results['manifest_data']]

        members = self.member_dao.get_members_from_sample_ids(self.sample_ids)
        member_ids = [m.id for m in members]

        # Update job run ID
        self.member_dao.update_member_job_run_id(member_ids, self.job_run.id, self.member_run_id_attribute)

        # Insert results workflow state
        for member in members:
            self.results_state_dao.insert(
                GenomicResultWorkflowState(
                    genomic_set_member_id=member.id,
                    results_workflow_state=self.pipeline_state,
                    results_workflow_state_str=self.pipeline_state.name,
                )
            )

    @staticmethod
    def _prepare_model_column(field):
        field_tuple = (field.source_value.split('.')[0],
                       field.source_value.split('.')[1],
                       field.field_name)

        return getattr(eval(field_tuple[0]), field_tuple[1]).label(field_tuple[2])

    @staticmethod
    def _prepare_literal_column(field):
        return literal(field.source_value).label(field.field_name)

    def _set_member_run_id_attribute(self):
        if not self.member_run_id_column:
            # have to clean b/c of attribute name convention: [proj]_[manifest]_manifest_job_run_id,
            # ex: cvl_w3sr_manifest_job_run_id
            self.member_run_id_column = f"{self.project_name.lower()}_{self.template_name.lower()}_manifest_job_run_id"

        member_attr = inspect(GenomicSetMember).c
        columns = {column[1].name: column[0] for column in member_attr.items()}

        if self.member_run_id_column not in columns:
            message = f"Attempting to update a job run field that doesn't exist: {self.member_run_id_column}"
            raise ValueError(message)

        self.member_run_id_attribute = columns[self.member_run_id_column]

    def _set_pipeline_state(self):
        state_name = f"{self.project_name.upper()}_{self.template_name.upper()}"
        self.pipeline_state = ResultsWorkflowState.lookup_by_name(state_name)
        return self.pipeline_state

