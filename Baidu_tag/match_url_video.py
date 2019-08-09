# coding:utf-8

import pymysql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd


def getVideoUrl(spark):
    sql1 = """select 
               t1.video_id,
               t2.category_id,
               t1.url 
             from (
                 select 
                  video_id, url 
                 from src_kanduoduo.video_address 
                group by video_id, url) t1
              join ( 
                 select 
                  id as video_id, category as category_id
            	 from src_kanduoduo.video 
            	 group by id, category
            	 ) t2
            	 on t1.video_id = t2.video_id"""
    df = spark.sql(sql1)
    return df


def getClassUrl(dir, spark):
    data = []
    with open(dir) as f:
        for line in f.readlines():
            data.append(line.strip().split('\t'))
    data1 = [ele[0:2] for ele in data if len(ele) == 3]
    df = spark.createDataFrame(data1).toDF("class", "url")
    return df

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

def VideoOnline(spark):
    conn = connectMySQL()
    sql = "select distinct id from kdd_video_report where status = 2"
    print(sql)
    conn = connectMySQL()
    cursor = conn.cursor()
    ok = cursor.execute(sql)
    videos_online = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    videos = [[ele[0]]  for ele in videos_online]
    return videos


if __name__ == '__main__':
    spark = SparkSession.builder.appName("match_url_video").enableHiveSupport().getOrCreate()
    df1 = getVideoUrl(spark)  # video_id, category_id, url
    dir = "/home/lechuan/sunjianqiang/hottopic_sv_cluster.txt"
    df2 = getClassUrl(dir, spark)  # class, url
    df3 = df2.join(df1, df2.url == df1.url).selectExpr("cast(video_id as int) as video_id1", "category_id", "class")
    # df3.show()
    df3.write.mode("overwrite").saveAsTable("test.video_cate_class_sjq")
    data = VideoOnline( spark )
    df4 = spark.createDataFrame(data).toDF("video_id").withColumn("if_online", lit(1)).selectExpr("cast(video_id as int) as video_id2", "if_online")
    df4.write.mode("overwrite").saveAsTable("test.video_online_sjq")
    # df4.show()
    df5 = df3.join(df4, df3.video_id1 == df4.video_id2, "left").fillna({"if_online": 0}).selectExpr("video_id1 as video_id", "category_id", "class", "if_online")

    df5.write.mode("overwrite").saveAsTable("test.video_class_sjq")

