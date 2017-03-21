export DB_CONNECTION_STRING=`tools/install_config.sh --key db_config | grep db_connection_string | cut -d\" -f4`
 