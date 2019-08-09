import redis
from pyspark.sql import SparkSession


def getRedisPipe():
    host = "r-2ze554aee853e8a4.redis.rds.aliyuncs.com"
    port = 6379
    passwd = "6X42p7uTvqLCpmC7"
    db = 24
    conn = redis.Redis(host=host,
                       port=port,
                       password=passwd,
                       db=db)
    pipe = conn.pipeline(transaction=False)
    return pipe


def addClass2Redis():
    sql1 = """
        select
         video_id,
         class
        from test.video_class_sjq where if_online = 1
      """
    df = spark.sql(sql1)
    rdd1 = df.rdd.map(lambda x: [str(x[1]), str(x[0])])
    rdd2 = rdd1.mapValues(lambda x: [x]).reduceByKey(lambda x, y: x + y).mapValues(lambda x: ','.join(x))
    pipe = getRedisPipe()
    for (key, value) in rdd2.collect():
        pipe.set("qq_class_" + key, value)
    pipe.execute()



if __name__ == '__main__':
    spark = SparkSession.builder.appName("Save class to redis").enableHiveSupport().getOrCreate()
    addClass2Redis()
