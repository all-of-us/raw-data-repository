source venv/bin/activate
coverage xml -o report.xml
bash <(curl -Ls https://coverage.codacy.com/get.sh) report -r report.xml
