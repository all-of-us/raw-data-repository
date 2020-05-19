
#!/bin/bash -e
# Exports ETL results to GCS.

USAGE="etl/export_etl_results.sh [--project <PROJECT> --account <ACCOUNT> --instance_name <INSTANCE_NAME>] --directory <DIRECTORY>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
    --instance_name) INSTANCE_NAME=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${DIRECTORY}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi
#PROJECT_AND_ACCOUNT=
#if [ "${PROJECT}" ]
#then
#  PROJECT_AND_ACCOUNT="--project ${PROJECT} --account ${ACCOUNT} --service_account exporter@${PROJECT}.iam.gserviceaccount.com"
#  if [ -n "${INSTANCE_NAME}" ]; then
#    PROJECT_AND_ACCOUNT="${PROJECT_AND_ACCOUNT} --instance_name ${INSTANCE_NAME}"
#  fi
#fi

if [ -z "${INSTANCE_NAME}"]; then
  INSTANCE_NAME="rdrmaindb"
fi

#pushd ../rdr_client
echo "Exporting tables from the cdm database..."
#./rdr_client/run_client.sh ${PROJECT_AND_ACCOUNT} export_tables.py \
#   --directory ${DIRECTORY} --database cdm \
#   --tables care_site,condition_era,condition_occurrence,cost,death,device_exposure,dose_era,drug_era,drug_exposure,fact_relationship,location,measurement,observation,observation_period,payer_plan_period,person,procedure_occurrence,provider,visit_occurrence
##popd

gcloud config set ${PROJECT}

for TABLE in care_site condition_era condition_occurrence cost death device_exposure dose_era drug_era drug_exposure fact_relationship location measurement observation observation_period payer_plan_period person procedure_occurrence provider visit_occurrence
do
  echo -n "  exporting: ${TABLE}..."
  SQL="SELECT * FROM cdm.${TABLE}"
  gcloud sql export csv ${INSTANCE_NAME} gs://all-of-us-rdr-prod-cdm/${DIRECTORY}/${TABLE}.csv --database="cdm" --query="${SQL}"
  echo " done."
done