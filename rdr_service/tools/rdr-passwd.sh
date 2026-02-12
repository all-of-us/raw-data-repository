#! /bin/bash
#Ensure that the gcloud account configuration is set to a pmi-ops account
bold=$(tput bold)
normal=$(tput sgr0)

case "$1" in
      prod) PW_PROJECT=all-of-us-rdr-prod;;
      stable) PW_PROJECT=all-of-us-rdr-stable;;
      staging) PW_PROJECT=all-of-us-rdr-staging;;
      sandbox) PW_PROJECT=all-of-us-rdr-sandbox;;
      test) PW_PROJECT=pmi-drc-api-test;;
      * ) PW_PROJECT=pmi-drc-api-test;;
esac

echo "Readonly:"
gcloud secrets versions access latest --secret='rdr-cloud-sql-readonly-password' --project=${PW_PROJECT}
echo
echo "RDR:"
gcloud secrets versions access latest --secret='rdr-cloud-sql-rdr-password' --project=${PW_PROJECT}
echo
echo "Root:"
gcloud secrets versions access latest --secret='rdr-cloud-sql-root-password' --project=${PW_PROJECT}
