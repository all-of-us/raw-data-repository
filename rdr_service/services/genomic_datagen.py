import logging

import faker
from sqlalchemy import literal
from sqlalchemy.inspection import inspect
from rdr_service import clock
from rdr_service.dao import database_factory
from rdr_service.dao.genomic_datagen_dao import GenomicDateGenCaseTemplateDao, GenomicDataGenRunDao, \
    GenomicDataGenMemberRunDao, GenomicDataGenManifestSchemaDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicJobRunDao, GenomicResultWorkflowStateDao
from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider, ManifestCompiler
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessStatus, GenomicSubProcessResult, \
    ResultsWorkflowState, GenomicManifestTypes
from rdr_service.model.genomics import GenomicSetMember, GenomicGCValidationMetrics, GenomicInformingLoop, \
    GenomicResultWorkflowState
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from tests.helpers.data_generator import DataGenerator


class ParticipantGenerator:
    def __init__(
        self,
        project='cvl',
    ):
        self.project = project
        self.num_participants = None
        self.template_type = None
        self.external_values = None

        self.member_ids = []
        self.default_template_records = []
        self.template_records = []

        self.default_table_map = {
            'participant': {
                'model': Participant,
            },
            'participant_summary': {
                'model': ParticipantSummary,
            },
            'genomic_set_member': {
                'model': GenomicSetMember
            },
            'genomic_gc_validation_metrics': {
                'model': GenomicGCValidationMetrics
            }
        }

    def __enter__(self):
        self.data_generator = self.initialize_data_generator()
        self.genomic_set = self._set_genomic_set()

        # init daos
        self.member_dao = GenomicSetMemberDao()
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

    def build_participant_default(self):
        if not self.default_template_records:
            self.default_template_records = self.datagen_template_dao.get_default_template_records(
                project=self.project
            )
            # will throw exception for invalid expected data struct
            self.validate_template_records(self.default_template_records)

        base_participant = None
        for table, table_items in self.default_table_map.items():
            model = table_items.get('model')
            # make sure it has generator, will throw exception if not
            generator_method = self._get_generator_method(table)

            if table == 'participant':
                base_participant = generator_method()
                self.default_table_map[table]['obj'] = base_participant
                continue

            current_table_defaults = list(
                filter(lambda x: x.rdr_field.split('.')[0].lower() == table, self.default_template_records)
            )

            attr_dict = {}
            for obj in current_table_defaults:
                field_name = obj.rdr_field.split('.')[-1].lower()
                if not hasattr(model, self.convert_case(field_name)):
                    raise Exception(f"Field name {field_name} is not present in {table} table")

                value = self.evaluate_value(
                    field_name,
                    obj.field_source,
                    obj.field_value
                )

                attr_dict[self.convert_case(field_name)] = value

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
            self.validate_template_records(self.template_records)

        # if template records have attributes from multiple tables
        table_names = set([obj.rdr_field.split('.')[0].lower() for obj in self.template_records])
        for table in table_names:
            generator_method = self._get_generator_method(table)

            current_table_attrs = list(
                filter(lambda x: x.rdr_field.split('.')[0].lower() == table, self.template_records)
            )

            attr_dict = {}
            for obj in current_table_attrs:
                field_name = obj.rdr_field.split('.')[-1].lower()
                value = self.evaluate_value(
                    field_name,
                    obj.field_source,
                    obj.field_value
                )

                attr_dict[field_name] = value

            try:
                generator_method(**attr_dict)
            except Exception as error:
                raise Exception(f'Error when inserting default records: {error}')

        print('Darryl')

    @staticmethod
    def convert_case(string_value):
        converted = string_value[0].lower() + string_value.title()[1:].replace("_", "")
        if 'Ror' in converted:
            converted, _ = converted.split('Ror')
            converted = converted + 'ROR'
        return converted

    def validate_template_records(self, records):
        if not records:
            raise Exception(f"{self.template_type} template records were not found for project: {self.project}")

        source_types = ['system', 'external', 'literal', 'calculated']
        for source_type in source_types:
            source_records = list(filter(lambda x: x.field_source == source_type, records))
            for record in source_records:
                value = record.field_value
                source = record.field_source
                name = record.rdr_field.split('.')[-1].lower()

                if value and (source == 'system' or source == 'external'):
                    raise Exception(f'System/External sources cannot have values, Record ID: {record.id}')
                if not value and (source == 'literal' or source == 'calculated'):
                    raise Exception(f'Literal/Calculated sources require value, Record ID: {record.id}')
                if source == 'external' and not self.external_values.get(name):
                    raise Exception(f'External key was not found, Record ID: {record.id}')

    def evaluate_value(self, field_name, field_source, field_value):
        field_source = field_source.lower()
        if field_source == 'literal':
            return field_value
        if field_source == 'external':
            return self._get_from_external_data(field_name)
        if field_source == 'calculated':
            return self._calc_algo_value(field_value)

    def _get_generator_method(self, table_name):
        gen_method_name = f'create_database_{table_name}'
        try:
            generator_method = getattr(self.data_generator, gen_method_name)
        except AttributeError:
            raise Exception(f"Cannot find generator for table: {table_name}")

        return generator_method

    def _calc_algo_value(self, field_value):
        # field_value should be %<table_name>.<snake_case_attr>%
        parsed_list, new_list = [val for val in field_value.split('%') if val != ''], []

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
        if not external_value:
            raise Exception(f'Value for external field {field_name} is not found')

        return external_value

    def _set_genomic_set(self):
        return self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def run_participant_creation(self, num_participants, template_type, external_values):
        self.num_participants = num_participants
        self.template_type = template_type
        self.external_values = external_values
        self.member_ids, self.template_records = [], []

        for _ in range(self.num_participants):
            self.build_participant_default()
            self.build_participant_type_records()

        self.datagen_member_run_dao.batch_insert_member_records(
            self.run_obj.id,
            self.template_type,
            self.member_ids
        )


class ManifestGenerator:
    def __init__(
        self,
        project_name='cvl',
        template_name=None,
        sample_ids=None,
        update_samples=False,
        member_run_id_column=None,
        logger=logging
    ):
        # Params
        self.project_name = project_name
        self.template_name = template_name
        self.sample_ids = sample_ids
        self.update_samples = update_samples
        self.member_run_id_column = member_run_id_column  # only used for testing
        self.logger = logger

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

        self.default_table_map = {
            'participant': {
                'model': Participant,
            },
            'participant_summary': {
                'model': ParticipantSummary,
            },
            'genomic_set_member': {
                'model': GenomicSetMember
            },
            'genomic_gc_validation_metrics': {
                'model': GenomicGCValidationMetrics
            },
            'genomic_informing_loop': {
                'model': GenomicInformingLoop
            }
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
        self.template_fields = self.manifest_datagen_dao.get_template_by_name(self.project_name,
                                                                  self.template_name)
        if self.template_fields:
            self.logger.info("Manifest name found in template table. Running external manifest.")

            if not self.sample_ids:
                self.run_results['status'] = GenomicSubProcessResult.ERROR
                self.run_results['message'] = f"Sample IDs required for external manifest."
                return self.run_results

            manifest_query_result = self.execute_external_manifest_query()

        else:
            # check manifest defs
            self.manifest_def_provider = ManifestDefinitionProvider(kwargs={})
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
        return self.manifest_datagen_dao.execute_manifest_query(columns, self.sample_ids)

    def execute_internal_manifest_query(self):
        # If sample IDs, use sample ID injection
        if self.sample_ids:
            pass

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
