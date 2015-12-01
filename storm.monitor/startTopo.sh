#!/bin/bash
if [ ! -n "$1" ] ;then  
    echo "you have not input a jar name!"
    echo "missing 1 args:xxx.jar."  
    echo "Usage: ./startTopo.sh xxx.jar"
else  
    echo "the topology is running..."  
    for var in $(cat topoName);do
       storm jar $1 main.$var &
    done
fi
