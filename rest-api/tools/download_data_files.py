import os

# @todo: change this to the new proper bucket once it's made
SOURCE_BUCKET = 'all-of-us-rdr-sequestered-config-test/'

AWARDEES = SOURCE_BUCKET + 'awardees.csv'
ORGS = SOURCE_BUCKET + 'organizations.csv'
SITES = SOURCE_BUCKET + 'sites.csv'


def import_data_files():
  for f in (AWARDEES, ORGS, SITES):
    command = 'gsutil -m cp -r gs://'+f+' '+ 'data/' + f.split('/')[-1]
    os.system(command)


if __name__ == '__main__':
  import_data_files()
