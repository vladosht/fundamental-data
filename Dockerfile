# This file is intended for Google Cloud Build, but you can adapt it as you see fit.

# Available container images are explained here:
# https://docs.cloud.google.com/docs/buildpacks/base-images
FROM europe-west9-docker.pkg.dev/serverless-runtimes/google-24/runtimes/python314

# The above container image starts with user www-data and current working directory /workspace
# Unfortunately it is owned by root and the python scripts need to write inside it,
# so here we change ownership. Furthermore, the default $HOME for this user is set to /root
# which prevents installing and running python libraries as non-root. Here we fix this as well. 
USER root
RUN usermod --home $(pwd) www-data &&\
    chown www-data:www-data .
USER www-data

# Double check that the pre-requisites are met
RUN echo "I am $(whoami), currently running in $(pwd) and my home is $HOME" &&\
    grep $(whoami) /etc/passwd &&\
    curl --version &&\
    gzip --version &&\
    python3 --version --version &&\
    which python3

# We are deploying from source, so this will transfer the uploaded source files to the container
# Look at .gcloudignore to see what was omitted.
COPY . .

# Install and test python dependencies.
RUN df -h &&\
    ls -alhF &&\
    pip install --no-cache-dir --user -r requirements.txt &&\
    python3 -c 'import pandas; pandas.show_versions()'

# Multiprocessing seems to cause problems in Google Cloud Run, so we turn it off.
ENTRYPOINT ["./run.sh", "--max-jobs=1"]
