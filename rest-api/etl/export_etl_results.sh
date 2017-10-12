#!/bin/bash -e

# Exports ETL results to GCS.

USAGE="etl/export_etl_results.sh [--project <PROJECT> --account <ACCOUNT>] --directory <DIRECTORY>"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --directory) DIRECTORY=$2; shift 2;;
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
  PROJECT_AND_ACCOUNT="--project ${PROJECT} --account ${ACCOUNT}"
fi

pushd ../rdr_client
echo "Exporting tables from the voc database..."

./run_client.sh ${PROJECT_AND_ACCOUNT} export_tables.py \
   --directory ${DIRECTORY} --database voc \
   --tables concept,concept_ancestor,concept_class,concept_relationship,concept_synonym,domain,drug_strength,relationship,vocabulary

echo "Exporting tables from the cdm database..."
./run_client.sh ${PROJECT_AND_ACCOUNT} export_tables.py \
   --directory ${DIRECTORY} --database cdm \
   --tables care_site,condition_era,condition_occurrence,cost,death,device_exposure,dose_era,drug_era,drug_exposure,fact_relationship,location,measurement,observation,observation_period,payer_plan_period,person,procedure_occurrence,provider,visit_occurrence
popd