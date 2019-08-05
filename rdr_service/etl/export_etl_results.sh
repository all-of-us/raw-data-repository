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
PROJECT_AND_ACCOUNT=
if [ "${PROJECT}" ]
then
  PROJECT_AND_ACCOUNT="--project ${PROJECT} --account ${ACCOUNT} --service_account exporter@${PROJECT}.iam.gserviceaccount.com"
  if [ -n "${INSTANCE_NAME}" ]; then
    PROJECT_AND_ACCOUNT="${PROJECT_AND_ACCOUNT} --instance_name ${INSTANCE_NAME}"
  fi
fi

pushd ../rdr_client
echo "Exporting tables from the cdm database..."
./run_client.sh ${PROJECT_AND_ACCOUNT} export_tables.py \
   --directory ${DIRECTORY} --database cdm \
   --tables care_site,condition_era,condition_occurrence,cost,death,device_exposure,dose_era,drug_era,drug_exposure,fact_relationship,location,measurement,observation,observation_period,payer_plan_period,person,procedure_occurrence,provider,visit_occurrence
popd
