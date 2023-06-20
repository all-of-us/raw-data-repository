#
# Run Flask Unittest Server
#
source venv/bin/activate
source /tmp/cloud-sdk/google-cloud-sdk/path.bash.inc
export PYTHONPATH=$PYTHONPATH:`pwd`

python3 main.py --flask --unittests
