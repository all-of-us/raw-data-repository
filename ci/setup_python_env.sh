#
# Install the python library requirements
#

python3 -m venv venv
source venv/bin/activate
export PYTHONPATH=`pwd`
echo "PYTHONPATH=${PYTHONPATH}"
pip install -q -r requirements.txt
pip install safety