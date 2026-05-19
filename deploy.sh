#!/bin/bash
# This script builds and deploys the extractor scripts from source as a Google Cloud Run batch job.
# More info on how this works: https://docs.cloud.google.com/docs/buildpacks/overview
# This script needs a properly configured gcloud CLI. Specifically, the following two settings are needed:
#   gcloud config set project [PROJECT_ID]
#   gcloud config set run/region [REGION]
# You can see your current settings with:
#   gcloud config list

JOB_NAME="job-companyfacts"

# Use the current shell environment to set the job environment
if [[ -z "$SNAPSHOTS_SEC_UA" || -z "$SNAPSHOTS_TARGET_FILE" ]]; then
  echo "Error: Required environment variables are not set." >&2
  echo "Please set SNAPSHOTS_SEC_UA and SNAPSHOTS_TARGET_FILE before running this script." >&2
  echo "Look inside run.sh for insights how to set them." >&2
  exit 1
fi

# "deploy" handles building, creating and updating a job in one go.
# The --source flag uses a Dockerfile, if available. Otherwise, it uses buildpacks
gcloud run jobs deploy "$JOB_NAME" --source=. \
  --set-env-vars="SNAPSHOTS_SEC_UA=$SNAPSHOTS_SEC_UA" \
  --set-env-vars="SNAPSHOTS_TARGET_FILE=$SNAPSHOTS_TARGET_FILE" \
  --tasks=1 --cpu=2 --memory=8Gi --max-retries=0 --task-timeout=60m

cat >&2 << EOF
Regardless of how you start the job, you can observe its logs in real time with a command like this:
watch -n 10 'gcloud run jobs logs read $JOB_NAME --freshness=10m | tail -n 30'
EOF
