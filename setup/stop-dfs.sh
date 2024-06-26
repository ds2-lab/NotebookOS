#!/bin/bash 
ssh -i ~/.ssh/hadoop.key hadoop@localhost 'env JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 bash -c /home/hadoop/hadoop/sbin/stop-dfs.sh'