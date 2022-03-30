import faker

from rdr_service.dao import database_factory
from rdr_service.dao.genomic_datagen_dao import GenomicDateGenCaseTemplateDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicGCValidationMetricsDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from tests.helpers.data_generator import DataGenerator


class ParticipantGenerator:
    def __init__(
        self,
        num_participants,
        template_type='default',
        project='cvl',
        external_values=None
    ):
        self.data_generator = self.initialize_data_generator()
        self.genomic_set = self._set_genomic_set()
        self.num_participants = num_participants
        self.template_type = template_type
        self.project = project
        self.external_values = external_values
        self.base_calc_value_dict = {}
        self.template_dao = GenomicDateGenCaseTemplateDao()
        self.default_table_map = {
            'participant': {
                'dao': ParticipantDao,
            },
            'participant_summary': {
                'dao': ParticipantSummaryDao,
            },
            'genomic_set_member': {
                'dao': GenomicSetMemberDao
            },
            'genomic_gc_validation_metrics': {
                'dao': GenomicGCValidationMetricsDao
            }
        }

    @staticmethod
    def initialize_data_generator():
        session = database_factory.get_database().make_session()
        fake = faker.Faker()
        return DataGenerator(session, fake)

    def build_participant_default(self):
        template_records = self.template_dao.get_default_template_records(
            project=self.project
        )
        # will throw exception for invalid expected data struct
        self.validate_template_records(template_records)

        base_participant = None
        for table, table_items in self.default_table_map.items():
            dao = table_items.get('dao')()
            col_keys = dao.model_type().__table__.columns.keys()
            # make sure it has generator
            gen_method_name = f'create_database_{table}'
            try:
                generator_method = getattr(self.data_generator, gen_method_name)
            except AttributeError:
                raise Exception(f"Cannot find generator for table: {table}")

            if table == 'participant':
                base_participant = generator_method()
                self.default_table_map[table]['obj'] = base_participant
                continue

            current_table_defaults = list(
                filter(lambda x: x.rdr_field.split('.')[0].lower() == table, template_records)
            )

            attr_dict = {}
            for obj in current_table_defaults:
                field_name = obj.rdr_field.split('.')[-1].lower()
                if field_name not in col_keys:
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
                gen_data = generator_method(**attr_dict)
                self.default_table_map[table]['obj'] = gen_data
            except Exception as error:
                raise Exception(f'Error when inserting default records: {error}')

    def build_participant_type_records(self):
        template_records = self.template_dao.get_template_records_template(
            project=self.project,
            template_type=''
        )
        # will throw exception for invalid expected data struct
        self.validate_template_records(template_records)

    @staticmethod
    def convert_case(string_value):
        converted = string_value[0].lower() + string_value.title()[1:].replace("_", "")
        if 'Ror' in converted:
            converted, _ = converted.split('Ror')
            converted = converted + 'ROR'
        return converted

    def validate_template_records(self, records):
        if not records:
            raise Exception(f"Default template records were not found for project:  {self.project}")

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

    def _calc_algo_value(self, field_value):
        # field_value should be %<table_name>.<snake_case_attr>%
        parsed_list, new_list = [val for val in field_value.split('%') if val != ''], []
        for val in parsed_list:
            if len(val.split('.')) == 2 and val.split('.')[0] in self.default_table_map.keys():
                table, attribute = val.split('.')
                # check for now in default map
                obj = self.default_table_map.get(table, {}).get('obj')
                if not obj:
                    raise Exception(f'Cannot find object for calculated value: {field_value}')
                attr_value = getattr(obj, self.convert_case(attribute))
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

    def run_participant_creation(self):
        for _ in range(self.num_participants):
            self.build_participant_default()
            self.build_participant_type_records()
