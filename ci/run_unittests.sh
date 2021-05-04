#
# Run unittests
#
source venv/bin/activate
export PYTHONPATH=$PYTHONPATH:`pwd`
UNITTEST_FLAG=1 coverage run -m unittest
coverage xml -o report.xml
bash <(curl -Ls https://coverage.codacy.com/com.get.sh) report -r report.xml
