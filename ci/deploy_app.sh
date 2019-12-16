#
# Deploy application to GAE
#
source venv/bin/activate
source /tmp/cloud-sdk/google-cloud-sdk/path.bash.inc
export PYTHONPATH=$PYTHONPATH:`pwd`
export GOOGLE_APPLICATION_CREDENTIALS=/home/circleci/gcloud-credentials.key
./ci/activate_creds.sh ~/gcloud-credentials.key
cd rdr_service
python -m tools app-engine --project $1 deploy --git-target $2 --quiet