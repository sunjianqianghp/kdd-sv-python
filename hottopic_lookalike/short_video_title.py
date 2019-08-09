from pyspark.sql import SparkSession
import jieba
import time
import datetime

def getBaseData(spark, sdate, edate):
    sqlRequest = """
                  select device, title
                  from (select
                          device,
                          video_id
                          from bdm_kanduoduo.kanduoduo_log_info_p
                         where day between '%s' and '%s'
                           and cmd = 110
                           and from_source in (1,4)
                           and is_dubious = 0
                           and device is not null
                           and trim(device) <> ''
                         group by device, video_id 
                           ) t1
                  join ( select id, title from src_kanduoduo.video where status = 4 and ( category < 100 or category > 500) group by id, title ) t2
                    on t1.video_id = t2.id
                 group by device, title
                 """ % (sdate, edate)
    print(sqlRequest)
    temp = spark.sql(sqlRequest)
    return temp


if __name__ == "__main__":
    spark = SparkSession.builder.appName("short_video_title").enableHiveSupport().getOrCreate()
    enddate = str( datetime.datetime.now() + datetime.timedelta(-1) )[0:10]
    startdate = str( datetime.datetime.now() + datetime.timedelta(-(1+1)) )[0:10]
    baseData = getBaseData(spark, startdate, enddate)
    result = baseData.rdd.map(lambda x: (str(x[0]), ",".join( jieba.cut(x[1], False) ) ) )
    kw = result.flatMap(lambda x: x[1].split(",") ).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False).keys().take(200)
    print(kw)





