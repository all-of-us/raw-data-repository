#
# Run unittests
#
source venv/bin/activate
export PYTHONPATH=$PYTHONPATH:`pwd`
UNITTEST_FLAG=1 coverage run -m unittest -v $(circleci tests glob "tests/**/*.py" | circleci tests split --split-by=name)
