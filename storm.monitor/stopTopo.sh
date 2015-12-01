#!/bin/bash
echo "the topology is stoping."
echo "Usage: ./stopTopo.sh"
for var in $(cat topoName);do
   storm kill $var &
done
