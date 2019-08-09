from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("baseData2").enableHiveSupport().getOrCreate()
    sql = "select uid, max(tag) as tag from test.match_to_drop_sjq group by uid"
    df = spark.sql(sql)
    df.write.mode("overwrite").saveAsTable("test.wsBaseData_sjq") # 8978135
