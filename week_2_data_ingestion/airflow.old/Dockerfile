# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################
# Own additions

# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# # Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

# # ARG BASE_AIRFLOW_IMAGE
# # FROM ${BASE_AIRFLOW_IMAGE}

# SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# USER 0

# ARG CLOUD_SDK_VERSION=322.0.0
# ENV GCLOUD_HOME=/opt/google-cloud-sdk

# ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

# RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
#     && TMP_DIR="$(mktemp -d)" \
#     && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
#     && mkdir -p "${GCLOUD_HOME}" \
#     && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
#     && "${GCLOUD_HOME}/install.sh" \
#        --bash-completion=false \
#        --path-update=false \
#        --usage-reporting=false \
#        --additional-components alpha beta kubectl \
#        --quiet \
#     && rm -rf "${TMP_DIR}" \
#     && gcloud --version

WORKDIR $AIRFLOW_HOME

USER ${AIRFLOW_UID}