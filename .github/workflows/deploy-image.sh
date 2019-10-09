#!/usr/bin/env bash

set -Eeuxo pipefail

function get_maven_project_version() {
  ./mvnw -q org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate -Dexpression=project.version -DforceStdout
}

function deploy() {
  ./mvnw -B com.google.cloud.tools:jib-maven-plugin:1.3.0:build \
    -Djib.to.image="${1}" \
    -Djib.to.auth.username="${DOCKER_USERNAME}" \
    -Djib.to.auth.password="$(echo ${DOCKER_PASSWORD} | base64 --decode)"
}

function main() {
  ./mvnw -q -B compile -Dmaven.test.skip=true

  base_image="projectriff/streaming-processor"
  version=$(get_maven_project_version)
  commit=$(git rev-parse HEAD)

  echo "Deploying ${base_image} (latest and ${version})"
  deploy "${base_image}"
  deploy "${base_image}:${version}"
  deploy "${base_image}:${version}-${commit}"
}

main
