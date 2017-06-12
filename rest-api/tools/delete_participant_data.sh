#!/bin/bash

# Deletes all participant data from the database of the specified project. (Don't use this
# post-launch in prod!)

if [ "${PROJECT}" ]
then
  tools/connect_to_database.sh $@ < tools/delete_participant_data.sql
else
  mysql -u root -proot < tools/delete_participant_data.sql
fi
