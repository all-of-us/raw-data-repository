#
# Install the python library requirements
#

python3 -m venv venv
source venv/bin/activate
export PYTHONPATH=`pwd`
echo "PYTHONPATH=${PYTHONPATH}"
pip install --exists-action=w --no-cache-dir -r requirements.txt
pip install safety
