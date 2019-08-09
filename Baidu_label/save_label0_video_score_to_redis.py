from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import redis
import time

def getVideoLabel0(spark):
    sql1 = """
            select
              t1.video_id,
              t2.label0_id
            from
              (select
                  video_id,
                  label
                from ( select
                        video_id,
                        case when type = "figure" then "figure" else label  end as label,
                        row_number() over ( partition by video_id order by cast(confidence as float) desc ) as rk
                       from dl_cpc.small_video_info tt
                      where type not in ("keyword", "logo")
                      ) tt
                where rk = 1
              ) t1
              join ( select
                       label,
                       label0_id
                       from bdm_kanduoduo.video_parsed_by_baidu_labels_relation
                    ) t2 on t1.label = t2.label
            group by t1.video_id, t2.label0_id
            """
    print(sql1)
    df = spark.sql(sql1)
    return df

def getVideoScore(spark):
    sql1 = "select video_id as video_id2, combScore from test.video_summary_info_sjq"
    df = spark.sql(sql1)
    return df

def getRedisPipe():
    host = "r-2ze554aee853e8a4.redis.rds.aliyuncs.com"
    port = 6379
    passwd = "6X42p7uTvqLCpmC7"
    db = 24
    conn = redis.Redis(host=host,
                       port=port,
                       password=passwd,
                       db=db)
    return conn

def saveToRedis(df):
    rdd1 = df.rdd.map(lambda x: [str(x[1]), [str(x[0])]]).reduceByKey(lambda x,y: x+y).mapValues(lambda x: ",".join(x))
    conn = getRedisPipe()
    count = 0
    for ( key, value ) in rdd1.collect():
        if key != '17':
            key1 = "label_" + key
            conn.set(key1, value)
            conn.expire(key1, 3600*24*30)  # expired in 30 days
            count += 1
    print("count %d "% count)
    time.sleep(3)

def saveBaiduLabelToRedis(df):
    rdd1 = df.rdd.map(lambda x: [str(x[0]), str(x[1])])
    conn = getRedisPipe()
    count = 0
    for (video_id, label0_id) in rdd1.collect():
        if label0_id != '17':
            key1 = "baidu_" + video_id
            conn.set(key1, "label_"+label0_id)
            conn.expire(key1, 3600*24*30)
            count += 1
    print("count %d"%count)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("video label0 match").enableHiveSupport().getOrCreate()
    video_label0 = getVideoLabel0(spark) # video_id, label0_id
    # video_score  = getVideoScore(spark)  # video_id2, combScore
    # video_label0_score = video_label0.join(video_score, video_label0.video_id == video_score.video_id2 ).select("video_id", "label0_id", "combScore")
    # saveToRedis(video_label0)
    saveBaiduLabelToRedis(video_label0)
    print("FINISHED")

