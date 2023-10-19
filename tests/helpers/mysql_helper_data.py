import contextlib
from mock import patch
#
# Data for unittests
#

UNSET_HPO_ID = 0
PITT_HPO_ID = 2
PITT_ORG_ID = 3
PITT_SITE_ID = 1
AZ_HPO_ID = 4
AZ_ORG_ID = 4
OBSOLETE_ID = 5
ILLINOIS_HPO_ID = 6
ILLINOIS_ORG_ID = 5
ILLINOIS_SITE_ID = 4

@contextlib.contextmanager
def random_ids(ids):
    with patch("random.randint", side_effect=ids):
        yield
