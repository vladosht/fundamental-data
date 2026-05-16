#!/bin/bash
# This is a sample script which could be used to run the companyfacts extractor in a container or in an automation pipeline
# All command line arguments are forwarded verbatim to the underlying python script.
# Please note that some container environments do not tolerate multiprocessing well. In such a case you can specify the
# --max-jobs=1 option to disable multiprocessing, which is on by default.
set -o pipefail

for cmd in curl gzip gcloud; do
   if ! command -v "$cmd" &> /dev/null; then
      echo "Error: Required command '$cmd' not available." >&2
      exit 1  #Dependency not found
   fi
done

if [ ! -x "./companyfacts_extractor.py" ]
then
   echo "Error: companyfacts_extractor.py not found in $(pwd) or not executable." >&2
   exit 2  #Main python script not found
fi

# If --help or -h is found in the CLI arguments, show the python script help message and exit.
case "$*" in
  *--help*|*-h*)
    ./companyfacts_extractor.py --help
    exit 0
    ;;
esac

if [ -z "$SNAPSHOTS_SEC_UA" ]
then
   echo "Error: Environment variable SNAPSHOTS_SEC_UA must contain a User-Agent string for the SEC like this:" >&2
   echo "Your Name your.email.here@example.org" >&2
   exit 3  #Required parameter missing
fi

if [ -z "$SNAPSHOTS_TARGET_FILE" ]
then
   echo "Error: Environment variable SNAPSHOTS_TARGET_FILE must contain a Google Cloud Storage bucket and file name like this:" >&2
   echo "bucket/snapshots.csv" >&2
   echo "A .gz extension will be appended automatically." >&2
   exit 3  #Required parameter missing
fi

# This following json file is about half a megabyte in size and
# provides mapping between SEC cik numbers and stock exchange ticker symbols.
if [ ! -f company_tickers_exchange.json ] &&\
     ! curl --fail --no-progress-meter --remote-name --user-agent "$SNAPSHOTS_SEC_UA" 'https://www.sec.gov/files/company_tickers_exchange.json'
then
   echo "Downloading the tickers JSON file failed!" >&2
   rm -f company_tickers_exchange.json  #The python script will fail if the json file is present but corrupted
   exit 4
fi

# The zip file is at least 1.3 GiB in size and slowly growing each night.
# You can opt to manually download it from
# https://www.sec.gov/search-filings/edgar-application-programming-interfaces
# and put it in the working directory of this script to avoid repeated downloads.
zipname="companyfacts.zip"
{ if [ -f $zipname ]
then
   echo "$zipname found locally in $(pwd), skipping download." >&2
   cat $zipname
else
   echo "Starting snapshots generation." >&2
   curl --fail --no-progress-meter --user-agent "$SNAPSHOTS_SEC_UA" 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
fi | ./companyfacts_extractor.py "$@" | gzip | gcloud storage cp - gs://${SNAPSHOTS_TARGET_FILE}.gz --quiet --no-user-output-enabled; } || { echo "Processing failure!" >&2; exit 10; }

echo "All done successfully." >&2
