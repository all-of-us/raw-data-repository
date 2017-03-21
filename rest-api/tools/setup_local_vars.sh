source tools/setup_vars.sh
PASSWORD=root

function set_local_db_connection_string {
  if [ $PASSWORD ]  
  then
    PASSWORD_STRING=":${PASSWORD}"
  else
    PASSWORD_STRING=""
  fi
  export DB_CONNECTION_STRING="mysql+mysqldb://${DB_USER}${PASSWORD_STRING}@localhost/${DB_NAME}?charset=utf8"
}