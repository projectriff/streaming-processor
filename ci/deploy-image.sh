#!/usr/bin/env bash

set -Eeuxo pipefail

function deploy() {
  mvn -B com.google.cloud.tools:jib-maven-plugin:1.3.0:build \
    -Djib.to.image="${1}" \
    -Djib.to.auth.username="${DOCKER_USER}" \
    -Djib.to.auth.username="${DOCKER_PASS}"
}

function main() {
  base_image="projectriff/streaming-processor"
  version=$(mvn -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version)

  echo "Deploying ${base_image} (latest and ${version})"
  deploy "${base_image}"
  deploy "${base_image}:${version}"
}

main
