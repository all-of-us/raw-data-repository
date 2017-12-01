import csv
import logging

class CsvImporter(object):

  def __init__(self, entity_name, dao, id_field, external_id_field):
    self.entity_name = entity_name
    self.dao = dao
    self.id_field = id_field
    self.external_id_field = external_id_field

  def run(self, filename, dry_run):
    skip_count = 0
    new_count = 0
    updated_count = 0
    matched_count = 0
    logging.info('Importing %s from %r.', self.entity_name, filename)
    with open(filename, 'r') as csv_file:
      reader = csv.DictReader(csv_file)
      existing_map = {getattr(entity, self.external_id_field): entity for entity
                      in self.dao.get_all()}
      with self.dao.session() as session:
        for row in reader:
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
            self._insert_entity(entity, dry_run)
            new_count += 1

    logging.info('Done importing %s%s: %d skipped, %d new, % d updated, %d not changed',
                 self.entity_name, ' (dry run)' if dry_run else '', skip_count, new_count,
                 updated_count, matched_count)

  def _update_entity(self, entity, existing_entity, session, dry_run):
    new_dict = entity.asdict()
    new_dict[self.id_field] = None
    existing_dict = existing_entity.asdict()
    existing_dict[self.id_field] = None
    if existing_dict == new_dict:
      logging.info('Not updating %s.', new_dict[self.external_id_field])
      return False
    else:

      existing_site.siteName = site.siteName
      existing_site.mayolinkClientNumber = site.mayolinkClientNumber
      existing_site.hpoId = site.hpoId
      if not dry_run:
        site_dao.update_with_session(session, existing_site)
      logging.info(
          'Updating site: old = %s, new = %s', existing_site_dict, existing_site.asdict())
      return True
  else:
    logging.info('Inserting site: %s', site_dict)
    if not dry_run:
      site_dao.insert_with_session(session, site)
    return True