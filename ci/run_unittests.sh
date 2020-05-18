#
# Run unittests
#
source venv/bin/activate
export PYTHONPATH=$PYTHONPATH:`pwd`
PYTHONUNBUFFERED=1 UNITTEST_FLAG=1 python -m unittest discover -v -s tests
