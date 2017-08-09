#!/bin/bash -e

# Sets environment variables used to run Python with the AppEngine SDK.

GCLOUD_PATH=$(which gcloud)
CLOUDSDK_ROOT_DIR=${GCLOUD_PATH%/bin/gcloud}
APPENGINE_HOME="${CLOUDSDK_ROOT_DIR}/platform/appengine-java-sdk"
GAE_SDK_ROOT="${CLOUDSDK_ROOT_DIR}/platform/google_appengine"

# The next line enables Python libraries for Google Cloud SDK
GAEPATH=${GAE_SDK_ROOT}

# * OPTIONAL STEP *
# If you wish to import all Python modules, you may iterate in the directory
# tree and import each module.
#
# * WARNING *
# Some modules have two or more versions available (Ex. django), so the loop
# will import always its latest version.
for module in ${GAE_SDK_ROOT}/lib/*; do
  if [ -r ${module} ];
  then
    GAEPATH=${module}:${GAEPATH}
  fi
done
unset module

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
ROOT_REPO_DIR="$( cd "${BASE_DIR}" && cd .. && pwd )"
# RDR libs should appear first in PYTHONPATH so we can override versions from
# the GAE SDK. (Specifically, we need oauth2client >= 4.0.0 and GAE uses 1.x.)
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib:${ROOT_REPO_DIR}/rdr_client:${ROOT_REPO_DIR}/rdr_common:${GAEPATH}

