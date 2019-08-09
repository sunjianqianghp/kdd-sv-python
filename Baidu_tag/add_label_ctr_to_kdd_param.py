
import sys
from pyspark.sql import SparkSession
import pymysql
from pyspark.sql.functions import *
import time

def connectMySQL():
    host = "rm-2zem5lmi9vj0vm9e5.mysql.rds.aliyuncs.com"
    port = 3306
    user = "kdd_rec_w"
    passwd = "7cwK0EZxNi0wyrEb"
    db = "kdd_rec"
    conn = pymysql.connect(host = host,
                           port = port,
                           user = user,
                           passwd = passwd,
                           db = db)
    return conn

def getLabelCtr( spark ):

    conn = connectMySQL()
    cursor = conn.cursor()
    sql1 = """
         select
           t1.video_id,
           t2.rec_validshow,
           t2.rec_validread
        from ( select distinct id as video_id from kdd_video_report where type between 100 and 1000 and type != 200 and status = 2 ) t1
        join ( select          id as video_id, rec_validshow, rec_validread from kdd_doc_pv_detail where rec_validshow >= 100 and rec_validshow >= rec_validread ) t2 
          on t1.video_id = t2.video_id
          """
    print("======= get video id from mysql =======")
    print(sql1)
    time.sleep(3)
    cursor.execute(sql1)
    videoInfos = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    videoInfos2 = [[vi[0], vi[1], vi[2]] for vi in videoInfos]
    print("NOTE1")
    print(videoInfos2[0:10])
    time.sleep(3)
    df = spark.createDataFrame(videoInfos2).toDF( "video_id", "valid_show", "valid_read" )
    df.createOrReplaceTempView("onlineVideoInfo")

    sql2 = """select
                  label0_id,
                  sum(valid_read) / sum(valid_show) as ctr
                from
                  (
                    select
                      label0_id,
                      label
                    from
                      bdm_kanduoduo.video_parsed_by_baidu_labels_relation
                  ) t1
                  join (
                    select
                      video_id,
                      case
                        when type = "figure" then "figure"
                        else label
                      end as label
                    from
                      dl_cpc.small_video_info tt
                    where
                      type not in ("keyword", "logo")
                    group by
                      video_id,
                      case
                        when type = "figure" then "figure"
                        else label
                      end
                  ) t2 on t1.label = t2.label
                  join (
                    select
                      video_id,
                      valid_show,
                      valid_read
                    from
                      onlineVideoInfo
                  ) t3 on t2.video_id = t3.video_id
                group by
                  label0_id """
    print("NOTE2")
    print(sql2)
    time.sleep(3)
    lst = spark.sql(sql2).rdd.map(lambda x: [ str(x[0]),str(0), str(x[1]) ]).collect()
    return lst

def insertIntoKddParam(value):
    name = "'labe0_ctr'"
    sql = "replace into kdd_param values (%s, '%s', 1, now())" % (name, value)
    print(sql)
    time.sleep(3)
    conn = connectMySQL()
    cursor = conn.cursor()
    ok = cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()



if __name__ == '__main__':
    spark = SparkSession.builder.appName("add label ctr to kdd_param").enableHiveSupport().getOrCreate()
    df0 = spark.createDataFrame([[0],[2]]).toDF('t').createOrReplaceTempView
    label0ctr = getLabelCtr(spark)
    value = "|" + "|".join([":".join(ele) for ele in label0ctr]) + "|"
    print(value)
    time.sleep(3)
    insertIntoKddParam(value)
    print("$"*30 + "SUCCESS")



