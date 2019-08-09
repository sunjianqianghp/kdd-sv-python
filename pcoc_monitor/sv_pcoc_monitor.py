# coding:utf-8
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def parse_json(x):
    try:
        return [(tp[0].encode('utf-8'), tp[1]["RankValue"]) for tp in json.loads(x).items()]
    except Exception:
        return [('', -1.0)]

def getExpCtr(spark, date, hour):
    sqlRequest = """select pv_id, device, groupid, props 
                        from (
                          select
                          field ["pv_id"].string_type as pv_id,
                          field ["device"].string_type as device,
                          substr(md5(field ['device'].string_type), 7, 1) as groupid,
                          case when field['app_package'].string_type between '1348' and '1447'
                                or  field['app_package'].string_type between '1448' and '1547'
                                or  field['app_package'].string_type between '1548' and '1647'
                                or  field['app_package'].string_type between '2348' and '2447'
                                or  field['app_package'].string_type between '2448' and '2547'
                                or  field['app_package'].string_type between '2598' and '2647'
                                or  field['app_package'].string_type between '2648' and '2667'
                                or  field['app_package'].string_type between '2768' and '2797'
                                or  field['app_package'].string_type between '2868' and '2887'
                                or  field['app_package'].string_type between '2888' and '2897'
                                or  field['app_package'].string_type between '2993' and '2994'
                                or  field['app_package'].string_type between '2995' and '3014'
                                or  field['app_package'].string_type between '3015' and '3032'
                                or  field['app_package'].string_type between '3233' and '3532'
                                or  field['app_package'].string_type between '3533' and '3582'
                                or  field['app_package'].string_type between '3723' and '3742'
                                or  field['app_package'].string_type between '3873' and '3892'
                                or  field['app_package'].string_type between '4025' and '4044'
                                or  field['app_package'].string_type between '4985' and '5004'
                                or  field['app_package'].string_type between '5105' and '5154'
                                or  field['app_package'].string_type between '5155' and '5204'
                                or  field['app_package'].string_type between '5375' and '5474'
                                or  field['app_package'].string_type between '5475' and '5494'
                                or  field['app_package'].string_type between '3723' and '3742'
                                or  field['app_package'].string_type between '3873' and '3892'
                                or  field['app_package'].string_type between '4025' and '4044'
                                or  field['app_package'].string_type between '4985' and '5004'
                                or  field['app_package'].string_type between '5105' and '5154'
                                or  field['app_package'].string_type between '5155' and '5204'
                                or  field['app_package'].string_type between '5375' and '5474'
                                or  field['app_package'].string_type between '5475' and '5494'
                                or  field['strategy'].string_type = 'health'
                            then 'health' else 'unhealth' end as strategy,
                          field ["props"].string_type as props
                         from src_kanduoduo.kanduoduo_log
                        where thedate = '{0}'
                          and thehour = '{1}'
                          and field ["cmd"].string_type = '100'
                          and field ['ct'].string_type = 'smallVideo'
                          --and substr(md5(field ['device'].string_type), 7, 1)  in ('0', '1', 'a', 'b')
                          ) tt 
                        where tt.strategy = 'unhealth'
                    """.format(date, hour)
    df = spark.sql(sqlRequest)
    df.show(5)
    RDD1 = df.rdd.map(lambda x: ((x[0], x[1], x[2]), parse_json(x[3]))) \
        .map(lambda x: [x[0] + tp for tp in x[1]]) \
        .flatMap(lambda x: x) \
        .filter(lambda x: x[4] > 0)
    df1 = spark.createDataFrame(RDD1).toDF("pv_id", "device", "groupid", "video_id", "score")  # pv_id, device, groupid, video_id, score















if __name__ == '__main__':
    spark = SparkSession.builder.appName("PCOC MONITOR").enableHiveSupport().getOrCreate()
    df_expctr = getExpCtr(spark, date, hour)