import logging
import re
import sys

import requests


# Configure logger to print out to CircleCI interface
logger = logging.getLogger()
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)

# Get GAE project and version number from CLI input
project_id = sys.argv[1]
version_id = sys.argv[2]

# Convert the version number if needed
# (our deploy replaces periods with hyphens during a release)
release_version_pattern = re.compile('^\d+\.\d+\.\d+$')
if release_version_pattern.match(version_id):
    # Version is 3 numbers separated by periods, so is a release version and would be deploy to GAE with hyphens
    version_id = version_id.replace('.', '-')

# Check to see if an instance responds
app_engine_url = f'https://{version_id}-dot-{project_id}.appspot.com/'
logger.info(f'Checking "{app_engine_url}" for a running instance')
response = requests.get(app_engine_url)

if response.status_code != 200:
    logger.error(f'ERROR: server responded with {response.status_code} status code')
    exit(1)
else:
    logger.info('Instance successfully responded!')
