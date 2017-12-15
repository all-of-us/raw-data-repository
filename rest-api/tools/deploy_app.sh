#!/bin/bash -e
# Checks out RDR code from git in the current directory; by default, uses the same version of the
# app that is currently running in the staging environment.
# After a Y/N confirmation, upgrades the database, installs the latest config, deploys the code, or
# (by default) does all three.

# Run this in the rest-api dir of the git repo with no uncommitted changes. You will need to
# check out whatever branch you want to work in after it's done.

. tools/set_path.sh

TARGET="all"

while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --project) PROJECT=$2; shift 2;;
    --version) VERSION=$2; shift 2;;
    --deploy_as_version) DEPLOY_AS_VERSION=$2; shift 2;;
    --target) TARGET=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

function usage {
  echo "Usage: deploy_app.sh --project all-of-us-rdr-stable --account $USER@pmi-ops.org \\"
  echo "    [--target app|db|config|all] [--version GIT_REF] [--deploy_as_version APPENGINE_VERSION]"
  exit 1
}

if [ -z "${PROJECT}" ]
then
  echo "Missing required --project flag."
  usage
fi

UPDATE_TRACKER=tools/update_release_tracker.py
if [ "${PROJECT}" == "all-of-us-rdr-prod" ]
then
  CONFIG="config/config_prod.json"
elif [ "${PROJECT}" == "all-of-us-rdr-stable" ]
then
  CONFIG="config/config_stable.json"
elif [ "${PROJECT}" == "all-of-us-rdr-dryrun" ]
then
  CONFIG="config/config_dryrun.json"
elif [ "${PROJECT}" == "all-of-us-rdr-sandbox" ]
then
  CONFIG="config/config_sandbox.json"
  echo "Skipping JIRA tracker updates for Sandbox."
  UPDATE_TRACKER=echo
else
  echo "Unsupported project: ${PROJECT}; exiting."
  usage
fi

if [ -z "${ACCOUNT}" ]
then
  echo "Missing required --account flag."
  usage
fi

if [ "$TARGET" != "all" ] && [ "$TARGET" != "app" ] && [ $TARGET != "db" ] && [ $TARGET != "config" ]
then
  echo "Target must be one of: all, app, db, config. Exiting."
  usage
fi

gcloud auth login $ACCOUNT
if [ -z "${VERSION}" ]
then
  VERSION=`gcloud app versions --project all-of-us-rdr-staging list | grep default | grep " 1.00" | tr -s ' ' | cut -f2 -d" "`
  if [ -z "${VERSION}" ]
  then
    echo "App version for $PROJECT could not be determined; exiting."
    usage
  fi
fi
if [ -z ${DEPLOY_AS_VERSION} ]
then
  DEPLOY_AS_VERSION="$VERSION"
fi

BOLD=$(tput bold)
NONE=$(tput sgr0)

echo "Project: ${BOLD}$PROJECT${NONE}"
echo "Source Version: ${BOLD}$VERSION${NONE}"
echo "Target Version: ${BOLD}$DEPLOY_AS_VERSION${NONE}"
echo "Target: ${BOLD}$TARGET${NONE}"
read -p "Are you sure? (Y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  echo "Exiting."
  exit 1
fi

echo "${BOLD}Checking out code...${NONE}"
git checkout $VERSION

if [ "$TARGET" == "all" ] || [ "$TARGET" == "db" ]
then
  echo "${BOLD}Upgrading database...${NONE}"
  $UPDATE_TRACKER --version $VERSION --comment "Upgrading database for ${PROJECT}."
  tools/upgrade_database.sh --project $PROJECT --account $ACCOUNT
  $UPDATE_TRACKER --version $VERSION --comment "Database for ${PROJECT} upgraded."
fi

if [ "$TARGET" == "all" ] || [ "$TARGET" == "config" ]
then
  echo "${BOLD}Updating configuration...${NONE}"
  $UPDATE_TRACKER --version $VERSION --comment "Updating config for ${PROJECT}."
  tools/install_config.sh --project $PROJECT --account $ACCOUNT --config $CONFIG --update
  $UPDATE_TRACKER --version $VERSION --comment "Config for ${PROJECT} updated."
fi

if [ "$TARGET" == "all" ] || [ "$TARGET" == "app" ]
then
  if [ "${PROJECT}" = "all-of-us-rdr-prod" ]
  then
    echo "Using ${BOLD}prod${NONE} app.yaml for project $PROJECT."
    APP_YAML=app_prod.yaml
  else
    APP_YAML=app_nonprod.yaml
  fi
  cat app_base.yaml $APP_YAML > app.yaml

  CRON_YAML=cron_default.yaml
  if [ "${PROJECT}" = "all-of-us-rdr-sandbox" ]
  then
    CRON_YAML=cron_sandbox.yaml
  fi
  cp "${CRON_YAML}" cron.yaml

  echo "${BOLD}Deploying application...${NONE}"
  $UPDATE_TRACKER --version $VERSION --comment "Deploying app to ${PROJECT}."
  gcloud app deploy app.yaml cron.yaml index.yaml queue.yaml offline.yaml \
      --quiet --project "$PROJECT" --version "$DEPLOY_AS_VERSION"
  $UPDATE_TRACKER --version $VERSION --comment "App deployed to ${PROJECT}."
  rm app.yaml cron.yaml
fi

echo "${BOLD}Done!${NONE}"
