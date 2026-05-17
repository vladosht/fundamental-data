#!/usr/bin/env python3
import os, sys, warnings
from google.cloud import storage
warnings.filterwarnings("error")

# Upload standard input to a Google Cloud Storage bucket
client = storage.Client()
bucket_name, file_name = os.environ.get("SNAPSHOTS_TARGET_FILE").split('/', 1)
assert file_name
assert bucket_name in [i.name for i in list(client.list_buckets())]
file_name += ".gz"
print('Uploading to:', client.project, bucket_name, file_name, file=sys.stderr)
client.bucket(bucket_name).blob(file_name).upload_from_string(sys.stdin.buffer.read(),timeout=5)
