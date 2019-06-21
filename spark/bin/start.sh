#!/bin/bash
. ./DMIC-env.sh

$SPARK_HOME/bin/spark-submit \
    --master $URL \
    --jars $JAR_PATH\
    --name $JOBNAME \
    --class DMIC.edu.Job.Job \
    $*
