# coding:utf-8

import sys
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
import json

def parse(x):
    """
    写该函数的主要目的是考虑到有的样本中没有“Rank_score”这个键
    :param x: (video_id, map )
    :return:
    """
    try:
        return (x[0].encode('utf-8'), x[1]["RankValue"])
    except Exception:
        return (x[0].encode('utf-8'), 0)

def parse_json(x):
    try:
        return [(tp[0].encode('utf-8'), tp[1]["RankValue"]) for tp in json.loads(x).items()]
    except Exception:
        return [('', -1.0)]

if __name__ == '__main__':
    date = sys.argv[1]
    hour = sys.argv[2]
    spark = SparkSession.builder.appName("lab").enableHiveSupport().getOrCreate()


    sqlRequest = """select pv_id, device, groupid, props 
                    from (
                      select
                      field ["pv_id"].string_type as pv_id,
                      field ["device"].string_type as device,
                      substr(md5(field ['device'].string_type), 7, 1) as groupid,
                      case when field['strategy'].string_type = 'health'
                         then 'health' else 'unhealth' end as strategy,
                      field ["props"].string_type as props
                     from src_kanduoduo.kanduoduo_log
                    where thedate = '2019-07-05'
                      and thehour = '10'
                      and field ["cmd"].string_type = '100'
                      and field ['ct'].string_type = 'smallVideo'
                      and field ['client_version'].string_type >= '13600'
                      --and substr(md5(field ['device'].string_type), 7, 1)  in ('0', '1', 'a', 'b')
                      ) tt 
                    where tt.strategy = 'unhealth'
                      """
    print(sqlRequest)

    df = spark.sql(sqlRequest) #  pv_id, device, groupid, props

    df.show(5)
    RDD1 = df.rdd.map(lambda x: ( (x[0], x[1], x[2]), parse_json(x[3]) ) )\
        .map(lambda x: [ x[0] + tp  for tp in  x[1]])\
        .flatMap(lambda x: x)\
        .filter(lambda x: x[4] > 0 )
    df1 = spark.createDataFrame(RDD1).toDF( "pv_id", "device", "groupid", "video_id", "score") # pv_id, device, video_id, score
    df1.show(10)
    df1.write.mode("overwrite").saveAsTable("test.basedata_for_auc_sjq")

    sqlRequest2 = """select
                       a.pv_id,
                       a.device,
                       a.video_id,
                       cast(COALESCE(b.v_play,0) as int) as label
                    from
                    (
                         select
                          field ['pv_id'].string_type    as pv_id,
                          field ['device'].string_type   as device,
                          field ['video_id'].string_type as video_id,
                          1 as v_show
                        from
                          src_kanduoduo.kanduoduo_client_log
                        where
                              thedate = '{0}'
                          and thehour = '09'
                          and field['video_id'].string_type>0
                          and field ['cmd'].string_type = '137' --and field ['action'].string_type != 2
                          and field ['from'].string_type = '3'
                          and field ['device'].string_type is not null
                          and trim(field ['device'].string_type) <> ''
                          and field ['pv_id'].string_type is not null
                          and field ['client_version'].string_type >= '13600'
                          --and substr(md5(field ['device'].string_type), 7, 1)  in ('0', '1', 'a', 'b')
                        group by field ['pv_id'].string_type,
                                 field ['device'].string_type,
                                 field ['video_id'].string_type
                          )a
                    left join
                    (
                           select
                             field ['pv_id'].string_type    as pv_id,
                             field ['device'].string_type   as device,
                             field ['video_id'].string_type as video_id,
                             1 as v_play
                         from
                           src_kanduoduo.kanduoduo_client_log
                         where
                           thedate = '{0}'
                           and thehour = '09'
                           and field ['video_id'].string_type > 0
                           and field ['cmd'].string_type = '110'
                           and field ['from'].string_type = '3'
                           and field ['client_version'].string_type >= '13600'
                           ---and substr(md5(field ['device'].string_type), 7, 1)  in ('0', '1', 'a', 'b')
                         group by field ['pv_id'].string_type,
                                  field ['device'].string_type,
                                  field ['video_id'].string_type
                         )b
                      on a.video_id = b.video_id
                     and a.pv_id    = b.pv_id
                     and a.device   = b.device""".format( '2019-07-05' )
    print(sqlRequest2)

    df2 = spark.sql(sqlRequest2) ## pv_id, device, video_id, label
    df2.persist()
    df2.write.mode("overwrite").saveAsTable("test.show_play_sjq")

    df3 = df1.join(df2, ["pv_id", "device", "video_id"] ).filter("label is not null").select("pv_id", "device", "groupid", "video_id", "score", "label")
    # df3.show()
    df3.write.mode("overwrite").saveAsTable("test.score_label_sjq")
    # expCtr = df3.groupBy("groupid").agg(  )




