#!/bin/bash

SPARK_HOME=/usr/lib/spark-current
queue=root.rec.algomr

date1=$1
date2=$2

$SPARK_HOME/bin/spark-submit --master yarn --queue $queue \
                            --conf 'spark.port.maxRetries=16' \
                            --executor-memory 10G \
                            --driver-memory 4g \
                            --executor-cores 10 \
                            --num-executors 20  \
                            --conf spark.pyspark.python=/usr/bin/python2.7 \
                            --conf spark.pyspark.driver.python=/usr/bin/python \
                            --conf 'spark.yarn.executor.memoryOverhead=4g'\
                            --conf 'spark.dynamicAllocation.maxExecutors=50'\
                            update_interest.py $date1 $date2