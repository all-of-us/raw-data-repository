# Checks out RDR code from git in the current directory; by default, uses the same version of the
# app that is currently running in the staging environment.
# After a Y/N confirmation, deploys the code, upgrades the database, or
# (by default) upgrades the database and then deploys the code.

# Run this in the rest-api dir of the git repo with no uncommitted changes. You will need to
# check out whatever branch you want to work in after it's done.

TARGET="app_and_db"

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

if [ -z "${PROJECT}" ]
then
  echo "Project not specified; exiting."
  exit 1
fi

if [ -z "${ACCOUNT}" ]
then
  echo "Account not specified; exiting."
  exit 1
fi

if [ "$TARGET" != "app_and_db" ] && [ "$TARGET" != "app" ] && [ $TARGET != "db" ]
then
  echo "Target must be one of: app_and_db, app, db. Exiting."
  exit 1
fi

gcloud auth login $ACCOUNT
if [ -z "${VERSION}" ]
then
  VERSION=`gcloud app versions --project all-of-us-rdr-staging list | grep default | grep " 1.00" | tr -s ' ' | cut -f2 -d" "`
  if [ -z "${VERSION}" ]
  then
    echo "App version for $PROJECT could not be determined; exiting."
    exit 1
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

set -e
echo "${BOLD}Checking out code...${NONE}"
git checkout $VERSION

if [ "$TARGET" == "app_and_db" ] || [ "$TARGET" == "db" ]
then
  echo "${BOLD}Upgrading database...${NONE}"
  tools/upgrade_database.sh --project $PROJECT --account $ACCOUNT
fi

if [ "$TARGET" == "app_and_db" ] || [ "$TARGET" == "app" ]
then
  if [ "${PROJECT}" = "all-of-us-rdr-prod" ]
  then
    echo "Using ${BOLD}prod${NONE} app.yaml for project $PROJECT."
    APP_YAML=app_prod.yaml
  else
    APP_YAML=app_nonprod.yaml
  fi
  echo "${BOLD}Deploying application...${NONE}"
  cp $APP_YAML app.yaml
  gcloud app deploy app.yaml app_base.yaml cron.yaml index.yaml queue.yaml offline.yaml \
      --project "$PROJECT" --version "$DEPLOY_AS_VERSION"
  rm app.yaml
fi

echo "${BOLD}Done!${NONE}"
