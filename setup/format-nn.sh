#!/bin/bash 
ssh -i ~/.ssh/hadoop.key hadoop@localhost 'env JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 ~/hadoop/bin/hdfs namenode -format'