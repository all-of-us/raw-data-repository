"""Imports entities into the database based on a CSV file.
"""
import csv
import logging

class CsvImporter(object):
  """Importer for database entities from CSV input.

  Subclasses indicate in the constructor the name of the entity (for logging purposes),
  the DAO used to save the entity, the name of the primary key database ID field,
  the name of the external ID (referenced in the CSV file), and columns that must be populated
  with some value in the CSV.

  They then define _entity_from_row(row) to parse an entity out of a row dictionary.
  """

  def __init__(self, entity_name, dao, id_field, external_id_field, required_columns):
    self.entity_name = entity_name
    self.dao = dao
    self.id_field = id_field
    self.external_id_field = external_id_field
    self.required_columns = required_columns

  def run(self, filename, dry_run):
    """Imports entities from the CSV file with the specified name.

    When dry_run flag is true, entities are not updated; instead logging indicates what would be
    updated."""
    skip_count = 0
    new_count = 0
    updated_count = 0
    matched_count = 0
    logging.info('Importing %ss from %r.', self.entity_name, filename)
    with open(filename, 'r') as csv_file:
      reader = csv.DictReader(csv_file)
      existing_map = {getattr(entity, self.external_id_field): entity for entity
                      in self.dao.get_all()}
      with self.dao.session() as session:
        for row in reader:
          # Strip leading and trailing whitespace
          row = {k.strip(): v.strip() for k, v in row.iteritems()}
          missing_fields = []
          for column in self.required_columns:
            value = row.get(column)
            if value is None or value == '':
              missing_fields.append(column)
          if missing_fields:
            logging.info('Skipping %s with missing columns: %s', self.entity_name, missing_fields)
            skip_count += 1
            continue

          entity = self._entity_from_row(row)
          if entity is None:
            skip_count += 1
            continue
          existing_entity = existing_map.get(getattr(entity, self.external_id_field))          
          if existing_entity:
            changed = self._update_entity(entity, existing_entity, session, dry_run)
            if changed:
              updated_count += 1
            else:
              matched_count += 1
          else:
            self._insert_entity(entity, existing_map, session, dry_run)
            new_count += 1

    logging.info('Done importing %ss%s: %d skipped, %d new, % d updated, %d not changed',
                 self.entity_name, ' (dry run)' if dry_run else '', skip_count, new_count,
                 updated_count, matched_count)

  def _entity_from_row(self, row):
    #pylint: disable=unused-argument
    raise Exception('Subclasses must implement _entity_from_row')

  def _update_entity(self, entity, existing_entity, session, dry_run):
    new_dict = entity.asdict()
    new_dict[self.id_field] = None
    existing_dict = existing_entity.asdict()
    existing_dict[self.id_field] = None
    if existing_dict == new_dict:
      logging.info('Not updating %s.', new_dict[self.external_id_field])
      return False
    else:
      logging.info('Updating %s%s: old = %s, new = %s', self.entity_name,
                   ' (dry run)' if dry_run else '', existing_dict, new_dict)
      if not dry_run:
        self._do_update(entity, existing_entity, session)
      return True

  def _do_update(self, entity, existing_entity, session):
    for k, v in entity.asdict().iteritems():
      if k != self.external_id_field and k != self.id_field:
        setattr(existing_entity, k, v)
    self.dao.update_with_session(session, existing_entity)

  def _insert_entity(self, entity, existing_map, session, dry_run):
    #pylint: disable=unused-argument
    logging.info('Inserting %s: %s', self.entity_name, entity.asdict())
    if not dry_run:
      self.dao.insert_with_session(session, entity)
