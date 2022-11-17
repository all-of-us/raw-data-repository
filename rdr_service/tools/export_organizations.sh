#!/bin/bash -e

SHEET_EXPORT_BASE="https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/export?format=csv&gid="

curl -L "${SHEET_EXPORT_BASE}1076878570" > data/awardees.csv
curl -L "${SHEET_EXPORT_BASE}1098779958" > data/organizations.csv
curl -L "${SHEET_EXPORT_BASE}0" > data/sites.csv

echo "Done."