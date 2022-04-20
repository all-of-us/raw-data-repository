import faker
import os

from rdr_service.dao import database_factory
from rdr_service.dao.genomic_datagen_dao import GenomicDateGenCaseTemplateDao, GenomicDataGenRunDao, \
    GenomicDataGenMemberRunDao, GenomicDataGenOutputTemplateDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
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
                    if source == 'external' and not external_values.get(value):
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
    ):
        self.project = project
        self.num_participants = None
        self.template_type = None
        self.external_values = None

        self.member_ids = []
        self.default_template_records = []
        self.template_records = []

        self.default_table_map = {
            'participant': {},
            'participant_summary': {},
            'genomic_set_member': {},
            'genomic_gc_validation_metrics': {}
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
                if os.environ["UNITTEST_FLAG"] == "1":
                    base_participant = generator_method()
                else:
                    participant_id = self.member_dao.get_random_id()
                    biobank_id = self.member_dao.get_random_id()
                    research_id = self.member_dao.get_random_id()
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
                records=self.default_template_records,
                template_type=self.template_type,
                validation_step='generation',
                external_values=self.external_values
            )

        # if template records have attributes from multiple tables
        table_names = set([obj.rdr_field.split('.')[0].lower() for obj in self.template_records])
        for table in table_names:
            current_table_attrs = list(
                filter(lambda x: x.rdr_field.split('.')[0].lower() == table, self.template_records)
            )

            if table == 'genomic_informing_loop':
                self.generate_loop_records(table, current_table_attrs)
                continue

            attr_dict = self._get_type_attr_dict(current_table_attrs, case=False)

            self.generate_type_records(table, attr_dict)

    def generate_loop_records(self, table, current_table_attrs):
        loop_dict = {key: value for key, value in self.external_values.items() if 'informing_loop_' in key}

        for loop_type, loop_value in loop_dict.items():
            module_type = f'{loop_type.split("_", 2)[-1]}'
            non_external_attrs = [obj for obj in current_table_attrs if obj.field_source != 'external']

            attr_dict = self._get_type_attr_dict(non_external_attrs, case=False)
            # implicit for now
            attr_dict['module_type'] = module_type
            attr_dict['decision_value'] = loop_value

            self.generate_type_records(table, attr_dict)

    def generate_type_records(self, table, attr_dict):
        generator_method = self._get_generator_method(table)
        try:
            generator_method(**attr_dict)
        except Exception as error:
            raise Exception(f'Error when inserting default records: {error}')

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
        if not external_value:
            raise Exception(f'Value for external field {field_name} is not found')

        return external_value

    def _set_genomic_set(self):
        return self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def run_participant_creation(self, **kwargs):
        self.num_participants = kwargs.get('num_participants')
        self.template_type = kwargs.get('template_type')
        self.external_values = kwargs.get('external_values')
        self.member_ids, self.template_records = [], []

        for _ in range(self.num_participants):
            self.build_participant_default()
            self.build_participant_type_records()

        self.datagen_member_run_dao.batch_insert_member_records(
            self.run_obj.id,
            self.template_type,
            self.member_ids
        )


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