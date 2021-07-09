#!/bin/sh
docker run  \
  --rm \
  --network=wikipedia-changes_default \
  -v /Users/nico/projects/hazelcast/wikipedia-changes/target:/jobs \
  --entrypoint '/bin/bash' hazelcast/hazelcast:5.0-BETA-1 \
  -c '/opt/hazelcast/bin/hz-cli submit -t hazelcast:5701 -n wikipedia /jobs/wikipedia-changes-1.0-SNAPSHOT.jar'