#
# Run unittests
#
source venv/bin/activate
export PYTHONPATH=$PYTHONPATH:`pwd`
UNITTEST_FLAG=1 python -m unittest discover -v -s tests