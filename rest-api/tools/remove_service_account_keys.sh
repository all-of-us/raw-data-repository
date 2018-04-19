#!/usr/bin/env bash

# remove keys for service accounts that start with 'awardee-' that are older than 3 days.
# PARAM: --PROJECT
# There is an endpoint for this same functionality as an offline cron job.
# The difference here is it only rotates service account keys for SA's beginning with 'awardee-' such as an HPO service account.

while true; do
  case "$1" in
    --project) PROJECT=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

EXPIRE_DATE=$(date -v"-3d"  "+%Y-%m-%d %H:%M:%S")
EXPIRE_DATE_EPOCH=$(date -j -f"%Y-%m-%d %H:%M:%S" "$EXPIRE_DATE" +"%s")

   echo "Project: $PROJECT"
   for account in $(gcloud iam service-accounts list --project $PROJECT --format="value(email)")
   do
        # match any service account starting with awardee-
        if [[ $account == awardee-* ]]
        then
            echo " -> Account: $account"
            for key in $(gcloud iam service-accounts keys list --project $PROJECT --iam-account $account --format="value(name.basename())")
            do
                for createdon in $(gcloud iam service-accounts keys list --iam-account $account --project $PROJECT --format="value(CREATED_AT)" )
                   do
                       echo "        Key created on $createdon"
                       KEY_EPOCH=$(date -j -f"%Y-%m-%dT%H:%M:%SZ" "$createdon" +"%s")
                       if [ "$KEY_EPOCH" -gt "$EXPIRE_DATE_EPOCH" ]
                       then
                           echo "       Deleting key older than 3 days."
                           echo "       key = $key"
                           $(gcloud iam service-accounts keys delete $key --iam-account $account)
                       fi
                   done
            done
        fi
   done
