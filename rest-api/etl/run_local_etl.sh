# Runs the ETL SQL on the local database.

source tools/setup_local_vars.sh

ROOT_PASSWORD_ARGS="-p${ROOT_PASSWORD}"
while true; do
  case "$1" in
    --account) ACCOUNT=$2; shift 2;;
    --nopassword) ROOT_PASSWORD=; ROOT_PASSWORD_ARGS=; shift 1;;
    --db_user) ROOT_DB_USER=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "${MYSQL_ROOT_PASSWORD}" ]
then
  ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}"
  ROOT_PASSWORD_ARGS='-p"${ROOT_PASSWORD}"'
else
  echo "Using a default root mysql password. Set MYSQL_ROOT_PASSWORD to override."
fi

mysql --verbose -h 127.0.0.1 -u "$ROOT_DB_USER" $ROOT_PASSWORD_ARGS < etl/etl.sql

echo "Done."