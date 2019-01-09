
# ISSUES:
#
# Find a way to allow access to a local mysql server that is external to the container.
# https://dev.to/bufferings/access-host-from-a-docker-container-4099
# https://github.com/docker/for-linux/issues/264
# https://developer.ibm.com/recipes/tutorials/bridge-the-docker-containers-to-external-network/
# http://blog.oddbit.com/2018/03/12/using-docker-macvlan-networks/
# https://docs.docker.com/network/
#

# USAGE:
#
# To build cd to same directory as "Dockerfile" and run: docker build --tag=rdr-server .
#
# After build to run: docker run -p localhost:8080:8080 rdr-server
#

# Use an official Python runtime as a parent image
FROM python:2.7-slim

RUN apt-get -qq update
RUN apt-get -qq -y install git curl gcc default-libmysqlclient-dev

# Downloading google cloud sdk package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

ENV SDK_ROOT /usr/local/gcloud/google-cloud-sdk
ENV GCP_SDK_ROOT $SDK_ROOT/platform/google_appengine

# Adding the package path to local
ENV PATH $PATH:$SDK_ROOT/bin

RUN gcloud --quiet components install app-engine-python-extras app-engine-python cloud-datastore-emulator

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

WORKDIR /app/rest-api

RUN tools/setup_env.sh

ENV PYTHONPATH $PYTHONPATH:$GCP_SDK_ROOT:$GCP_SDK_ROOT/lib
ENV sdk_dir $SDK_ROOT

ENTRYPOINT ["dev_appserver.py", "test.yaml", "--require_indexes"]
