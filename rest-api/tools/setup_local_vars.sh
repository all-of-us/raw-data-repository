source tools/setup_vars.sh
ROOT_PASSWORD=root
RDR_PASSWORD=rdr!pwd

function set_local_db_connection_string {
  DB_USER=$RDR_DB_USER  
  if [ "$1" ]
  then
    DB_USER=$1
  fi
  export DB_CONNECTION_STRING="mysql+mysqldb://${DB_USER}:${RDR_PASSWORD}@localhost/${DB_NAME}?charset=utf8"
}