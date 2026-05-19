#!/bin/bash
# This is a sample script which could be used to run the companyfacts extractor in a container or in an automation pipeline.
# All command line arguments are forwarded verbatim to the underlying python script.
# Please note that some container environments do not tolerate multiprocessing well. In such a case you can specify the
# --max-jobs=1 option to disable multiprocessing, which is on by default.
# Running this script with the --help option will check all necessary prerequisites before printing a usage message.
set -o pipefail
echo "Companyfacts batch script $0 $*: Running as $(whoami) in $(pwd)" >&2
df -h $(pwd) >&2  #Print the available free space

for cmd in curl gzip; do
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

# If --help or -h is found in the CLI arguments, show the python script help message and exit.
case "$*" in
  *--help*|*-h*)
    ./companyfacts_extractor.py --help
    exit 0
    ;;
esac

# The zip file is of substantial size and growing incrementally each night.
# You can opt to manually download it from
# https://www.sec.gov/search-filings/edgar-application-programming-interfaces
# and put it in the working directory of this script to avoid repeated downloads.

# Download the two files needed to assemble the result dataset
# If a file already exists, skip downloading.
url_json='https://www.sec.gov/files/company_tickers_exchange.json'  #About half a megabyte
url_zip='https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'  #At least 1.3 GiB
for a_url in "$url_json" "$url_zip"; do
   sec_fname=$(basename $a_url)
   if [ ! -f "$sec_fname" ] && ! curl -v --http2-prior-knowledge --fail --user-agent "$SNAPSHOTS_SEC_UA" --remote-name "$a_url"
   then
      echo "Downloading $sec_fname failed!" >&2
      rm -f $sec_fname  #The python script will fail if the file is present but corrupted
      exit 4
   else
      if [ -f "$sec_fname" ]; then
         # A successful download will print here, too. This is intentional.
         echo "$sec_fname found locally in $(pwd)." >&2
      fi
   fi
done

echo "Starting snapshots generation with the following memory layout:" >&2
free -m >&2
{ ./companyfacts_extractor.py "$@" <companyfacts.zip | gzip | ./upload_results.py; } || { echo "Processing failure!" >&2; exit 10; }
echo "Upload of results was successful." >&2
