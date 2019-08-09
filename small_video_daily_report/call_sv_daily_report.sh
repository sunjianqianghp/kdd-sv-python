#!/bin/bash

source ~/.bash_profile
source /etc/profile
SPARK_HOME=/usr/lib/spark-current
queue=root.rec.algomr

date=`date -d"-1 days" +"%Y-%m-%d"`

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
                            ~/sunjianqiang/kdd-smallvideo-sjq-py/small_video_daily_report/sv_daily_report.py $date
