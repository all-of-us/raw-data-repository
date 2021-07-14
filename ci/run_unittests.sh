#
# Run unittests
#
source venv/bin/activate
export PYTHONPATH=$PYTHONPATH:`pwd`
UNITTEST_FLAG=1 coverage run -m unittest discover -v -s tests
