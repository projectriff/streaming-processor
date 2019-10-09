#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

function deploy() {
  ./mvnw -B com.google.cloud.tools:jib-maven-plugin:1.3.0:build \
    -Djib.to.image="${1}" \
    -Djib.to.auth.username="${DOCKER_USERNAME}" \
    -Djib.to.auth.password="$(echo ${DOCKER_PASSWORD} | base64 --decode)"
}

function main() {
  ./mvnw -q -B compile -Dmaven.test.skip=true

  base_image="projectriff/streaming-processor"
  version=$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout | tail -n1)
  commit=$(git rev-parse HEAD)

  echo "Deploying ${base_image} (latest and ${version})"
  deploy "${base_image}"
  deploy "${base_image}:${version}"
  deploy "${base_image}:${version}-${commit:0:16}"
}

main
