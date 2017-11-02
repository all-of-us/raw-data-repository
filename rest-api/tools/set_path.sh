#!/bin/bash -e

export BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
ROOT_REPO_DIR="$( cd "${BASE_DIR}" && cd .. && pwd )"
# RDR libs should appear first in PYTHONPATH so we can override versions from
# the GAE SDK. (Specifically, we need oauth2client >= 4.0.0 and GAE uses 1.x.)
export PYTHONPATH=$PYTHONPATH:${BASE_DIR}:${BASE_DIR}/lib:${ROOT_REPO_DIR}/rdr_client:${ROOT_REPO_DIR}/rdr_common
