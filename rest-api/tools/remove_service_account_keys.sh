#!/usr/bin/env bash

DATE=`date '+%Y-%m-%dT%H:%M:%SZ'`
EXPIRE_DATE=`date -v"-3d"  "+%Y-%m-%d %H:%M:%S"`
EXPIRE_DATE_EPOCH=`date -j -f"%Y-%m-%d %H:%M:%S" "$EXPIRE_DATE" +"%s"`
PROJECTS="pmi-drc-api-test"

for project in $PROJECTS
do
   echo "Project: $project"
   for account in $(gcloud iam service-accounts list --project $project --format="value(email)")
   do
        # match any service account starting with awardee-
        if [[ $account == awardee-* ]]
        then
            echo " -> Account: $account"
            for key in $(gcloud iam service-accounts keys list --project $project --iam-account $account --format="value(name.basename())")
            do
                for createdon in $(gcloud iam service-accounts keys list --iam-account $account --project $project --format="value(CREATED_AT)" )
                   do
                       echo "        Key created on $createdon"
                       KEY_EPOCH=`date -j -f"%Y-%m-%dT%H:%M:%SZ" "$createdon" +"%s"`
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
done
