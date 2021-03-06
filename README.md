# java-control-plane

[![CircleCI](https://circleci.com/gh/joeyb/java-control-plane.svg?style=svg)](https://circleci.com/gh/joeyb/java-control-plane) [![codecov](https://codecov.io/gh/joeyb/java-control-plane/branch/master/graph/badge.svg)](https://codecov.io/gh/joeyb/java-control-plane)

This repository contains a Java-based implementation of an API server that implements the discovery service APIs defined
in [data-plane-api](https://github.com/envoyproxy/data-plane-api). It is a port of the
[go-control-plane](https://github.com/envoyproxy/go-control-plane).

### Requirements

1. Java 8+
2. Maven

### Build & Test

```bash
mvn clean package
```

More thorough usage examples are still TODO, but there is a basic test implementation in
[TestMain](server/src/test/java/io/envoyproxy/controlplane/server/TestMain.java).
