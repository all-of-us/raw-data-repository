import os
import sys

ENDPOINTS_PROJECT_DIR = os.path.join(os.path.dirname(__file__),
                                     'endpoints-proto-datastore')
sys.path.append(ENDPOINTS_PROJECT_DIR)

# from google.appengine.ext import vendor

# Add any libraries installed in the "lib" folder.

# Uncomment when we actually want to load libraries.
# vendor.add('lib')
