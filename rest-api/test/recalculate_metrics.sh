if [ -z "$instance" ];
then
    instance=http://localhost:8080
    echo "Testing ${instance}"
fi

wget --header='X-Appengine-Cron: true' ${instance}/rdr/v1/MetricsRecalculate
