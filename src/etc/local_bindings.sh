#!/usr/bin/env bash

mvn --quiet test-compile exec:java \
  -Dexec.classpathScope="test" \
  -Dexec.args="${*}" \
  -Dstart-class="io.projectriff.bindings.LocalStreamBindingsGenerator" # not exec.mainClass b/c of Spring Boot parent POM
