from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

def getBaseData(spark):
    dir_crtdate_device = "/home/lechuan/sunjianqiang/data/actdate_device.csv"
    dir_revisit = "/home/lechuan/sunjianqiang/data/revisit_device.csv"
    crt_date_device = []
    revisit_device = []
    with open(dir_crtdate_device) as f:
        for line1 in f.readlines():
            crt_date_device.append(line1.strip().split(","))
    with open(dir_revisit) as f2:
        for line2 in f2.readlines():
            revisit_device.append(line2.strip().split(","))
    df_ctrdate_device = spark.createDataFrame(
        [[ele[0], time.strptime(ele[0], "%Y-%m-%d")[6] + 1, ele[1]] for ele in crt_date_device[1:]]).toDF("date", "wday", "device1")
    df_revisit_device = spark.createDataFrame(revisit_device[1:]).withColumn("tag", lit(1)).toDF("device2", "tag")
    comb = df_ctrdate_device.join(df_revisit_device, df_ctrdate_device.device1 == df_revisit_device.device2,"left").fillna({"tag": 0})\
        .selectExpr("device1 as device", "tag", "date as create_date", "wday")
    return comb

if __name__ == "__main__":
    spark = SparkSession.builder.appName("baseData").enableHiveSupport().getOrCreate()
    baseData = getBaseData(spark)
    baseData.write.mode("overwrite").saveAsTable("test.baseData_sjq")

    # zfb_device_sex = spark.read.parquet("hdfs://emr-cluster/user/cpc/qtt-zfb/{1,5,10,15}").select("did", "sex")
    # comb2 = comb.join( zfb_device_sex, comb.device == zfb_device_sex.did, "left" ).select("device", "tag", "create_date", "wday", "sex")
    # comb2.show(20)
    # time.sleep(5)







