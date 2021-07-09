#!/bin/sh
docker run  \
  --rm \
  --network=wikipedia-changes_default \
  --entrypoint '/bin/bash' hazelcast/hazelcast:5.0-BETA-1 \
  -c '/opt/hazelcast/bin/hz-cli cancel -t hazelcast:5701 wikipedia'