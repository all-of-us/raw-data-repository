from main_util import get_parser, configure_logging
from unicode_csv import UnicodeDictReader
import csv

def strip(val):
  if val is None:
    return None
  stripped_val = val.strip()
  if stripped_val == '' or stripped_val == 'NULL' or 'UNSET' in stripped_val:
    return None
  if stripped_val.startswith('Organization/'):
    stripped_val = stripped_val[13:]
  if stripped_val.startswith('Site/'):
    stripped_val = stripped_val[5:]
  return stripped_val

def check_prev_entry(p_map, prefix, participant_id, obj_id):
  prev_entry = p_map.get(participant_id)
  new_entry = prefix + obj_id
  if prev_entry is not None:
    if prev_entry != new_entry:
      raise "Prev entry = %s; new entry = %s" % (prev_entry, new_entry)
    return False
  else:
    p_map[participant_id] = new_entry
    return True

def main(args):
  with open(args.file, 'r') as input_file, \
    open('hpos.csv', 'w') as hpos_file, \
    open('orgs.csv', 'w') as orgs_file, \
    open('sites.csv', 'w') as sites_file:
    reader = UnicodeDictReader(input_file)
    hpo_writer = csv.writer(hpos_file)
    orgs_writer = csv.writer(orgs_file)
    sites_writer = csv.writer(sites_file)
    p_map = {}
    for row in reader:
      participant_id = strip(row.get('external_id'))
      if participant_id is None:
        print "Skipping line with no participant_id, continuing."
        continue
      awardee_id = strip(row.get('awardee_code'))
      org_id = strip(row.get('org_code'))
      site_val = row.get('donation_site_code')
      if site_val and not site_val.strip().startswith('Site/hpo-site'):
        # Ignore site values that don't start with 'Site/hpo-site'
        site_val = None
      site_id = strip(site_val)
      if awardee_id is None and org_id is None and site_id is None:
        print "Skipping participant with no awardee, id = %s" % participant_id
        continue
      if site_id is not None:        
        if check_prev_entry(p_map, 'site:', participant_id, site_id):
          sites_writer.writerow([participant_id, site_id])
      elif org_id is not None:
        if check_prev_entry(p_map, 'org:', participant_id, org_id):
          orgs_writer.writerow([participant_id, org_id])
      else:
        if check_prev_entry(p_map, 'hpo:', participant_id, awardee_id):
          hpo_writer.writerow([participant_id, awardee_id])

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='Path to the CSV file containing the participant data.',
                      required=True)
  main(parser.parse_args())
