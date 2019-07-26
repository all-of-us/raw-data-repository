import os
""" Downloads csv files from Google buckets and places in data/ dir (gitignored).
"""

#all-of-us-organization-sheets-stable is also available if needed for testing.
SOURCE_BUCKET = 'all-of-us-organization-sheets-prod/'

AWARDEES = SOURCE_BUCKET + 'awardees.csv'
ORGS = SOURCE_BUCKET + 'organizations.csv'
SITES = SOURCE_BUCKET + 'sites.csv'
LIST_SERV = SOURCE_BUCKET + 'list-serv.csv'
ALL_HPO_SITES = SOURCE_BUCKET + 'all-hpo-sites.csv'
ROLLOUT = SOURCE_BUCKET + 'rollout-plan.csv'

ALL_FILES = (AWARDEES, ORGS, SITES, LIST_SERV, ALL_HPO_SITES, ROLLOUT)

def import_data_files():
  for f in ALL_FILES:
    command = 'gsutil -m cp -r gs://'+f+' '+ 'data/' + f.split('/')[-1]
    os.system(command)


if __name__ == '__main__':
  import_data_files()
