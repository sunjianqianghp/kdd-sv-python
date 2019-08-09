# coding:utf-8

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("PCOC MONITOR").enableHiveSupport().getOrCreate()

def updateData(spark, date):
    sqlRequest = """select
                      hour,
                      ctr_model_name,
                      t1.shown,
                      t1.clickn,
                      t1.clickn / t1.shown as ctr,
                      avg_exp_ctr
                    from
                      (
                        select
                          hour,
                          ctr_model_name,
                          sum(isshow) as shown,
                          sum(isclick) as clickn,
                          avg(exp_ctr) as avg_exp_ctr
                        from
                          dl_cpc.cpc_basedata_union_events
                        where
                          day = '{}'
                          and ctr_model_name in ("qtt-ext-video-dnn-rawid-v16", "qtt-ext-video-dnn-rawid-v3")
                          and adsrc = 1
                          and media_appsid = '80002819'
                          and isshow = 1
                          and ideaid > 0
                          and userid > 0
                        group by hour, 
                          ctr_model_name
                      ) t1
    """.format(date)
    print(sqlRequest)
    df0 = spark.sql(sqlRequest)
    df1 = df0.withColumn("pcoc", col("avg_exp_ctr")/col("ctr")/1000000.0 ).withColumn("day", lit(date)).select( "ctr_model_name", "shown", "clickn", "ctr", "avg_exp_ctr", "pcoc", "day", "hour")
    df1.toPandas().to_csv("/home/lechuan/sunjianqiang/pcoc/pcoc_"+date+".csv")



if __name__ == '__main__':
    date = sys.argv[1]
    updateData(spark, date)
    print("$"*20 + "SUCCESS" + "$"*20)



