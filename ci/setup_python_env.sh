#
# Install the python library requirements
#

python3 -m venv venv
source venv/bin/activate
export PYTHONPATH=`pwd`
echo "PYTHONPATH=${PYTHONPATH}"
pip install --upgrade pip
pip install safety
pip install -r requirements.txt

safety check
