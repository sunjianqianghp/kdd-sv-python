from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import datetime
import time
import sys

def VvAndDuration(spark, date, days):
    endday = datetime.datetime.strptime(date, "%Y-%m-%d")
    startday = endday + datetime.timedelta(days = -days)
    date0 = startday.strftime("%Y-%m-%d")
    sql1 = """ select
                  day,
                  count( case when cmd = 110 and from_source in ('3', '5') then device else null               end ) as small_video_play,
                  sum(   case when cmd = 112 and page = 3                  then cast(duration as float) else 0 end )/60000.0 as small_video_duration,
                  count(distinct case when cmd = 119 and menu_key = 2              then device else null       end ) as small_tab_uv
                from  bdm_kanduoduo.kanduoduo_log_info_p
                where day between '{date0}' and '{date1}'  
                  and is_dubious = 0
                  and device is not null  
                  and trim(device) <> '' 
                group by day
                order by day desc
           """.format(date0 = date0, date1 = date)
    print(sql1)
    time.sleep(3)
    df1 = spark.sql(sql1)
    df2 = df1.withColumn("sv_tab_vv", col("small_video_play")/col("small_tab_uv")).withColumn("sv_tab_avg_duration", col("small_video_duration")/col("small_tab_uv"))\
        .selectExpr("day", "small_video_play", "small_video_duration", "small_tab_uv", "round(sv_tab_vv,2) as sv_tab_vv", "round(sv_tab_avg_duration,2) as sv_tab_avg_duration")
    df2.toPandas().to_csv("~/sunjianqiang/sm_daily_report/uv_avg_vv_duration_"+date+".csv")

def appVersionVvAndAccount(spark, date, days):
    endday = datetime.datetime.strptime(date, "%Y-%m-%d")
    startday = endday + datetime.timedelta(days=-days)
    date0 = startday.strftime("%Y-%m-%d")
    sql1 = """select
                day,
                case when cast(client_version as int) >= 13600 then 1 else 0 end as if_136,
                count(         case when cmd = 110 and from_source in ('3', '5') then device else null end ) as small_video_play,
                count(distinct case when cmd = 119 and menu_key = 2              then device else null end ) as small_tab_uv
              from  bdm_kanduoduo.kanduoduo_log_info_p
              where day between '{date0}' and '{date1}'  
                and is_dubious = 0
                and device is not null  
                and trim(device) <> '' 
              group by  
                day,
                case when cast(client_version as int) >= 13600 then 1 else 0 end
             order by day desc, if_136
           """.format(date0 = date0, date1 = date)
    print(sql1)
    time.sleep(3)
    df1 = spark.sql(sql1)
    df0 = df1.groupBy("day").agg(sum("small_tab_uv").alias("small_tab_uv_total")).selectExpr("day as day2", "small_tab_uv_total")
    df2 = df1.withColumn("sv_tab_vv", col("small_video_play")/col("small_tab_uv")).select("day", "if_136", "small_video_play", "small_tab_uv", "sv_tab_vv")
    df3 = df2.join(df0, df2.day == df0.day2).withColumn("small_tab_uv_account", col("small_tab_uv")/col("small_tab_uv_total"))\
        .selectExpr("day", "if_136", "small_tab_uv", "small_tab_uv_total", "round(small_tab_uv_account, 4) as small_tab_uv_account", "small_video_play", "round(sv_tab_vv, 2) as sv_tab_vv").orderBy(["day", "if_136"], ascending = [0, 1])
    df3.toPandas().to_csv("~/sunjianqiang/sm_daily_report/uv_avg_vv_by_version_"+date+".csv")

def groupedVvDurationComment(spark, date):
    sql1 = """select
                 substr(md5( device ), 13, 1) as groupid,
                 count(         case when cmd = 110 and from_source in ('3', '5') then device else null               end ) as small_video_play,
                 sum(           case when cmd = 112 and page = 3 then cast(duration as float) else 0 end )/60000.0          as small_video_duration,
                 count(distinct case when cmd = 119 and menu_key = 2              then device else null               end ) as small_tab_uv
               from  bdm_kanduoduo.kanduoduo_log_info_p
               where day = '{}' 
                 and is_dubious = 0
                 and device is not null  
                 and trim(device) <> '' 
               group by  
                 substr(md5( device ), 13, 1)
           """.format(date)
    sql2 = """select
                 substr(md5( device ), 13, 1) as groupid1,
                 count( case when cmd = 400 then device else null end ) as small_video_comment
                from  bdm_kanduoduo.kanduoduo_server_log_info_p
               where day = '{}'  
                 and is_dubious = 0
                 and device is not null  and trim(device) <> '' 
               group by substr(md5( device ), 13, 1)
           """.format(date)
    print(sql1)
    time.sleep(3)
    print(sql2)
    time.sleep(3)
    df1 = spark.sql(sql1)
    df2 = spark.sql(sql2)
    df = df1.join(df2, df1.groupid == df2.groupid1, "left").withColumn("sv_tab_vv", col("small_video_play")/col("small_tab_uv"))\
        .withColumn("sv_tab_avg_duration", col("small_video_duration")/col("small_tab_uv"))\
        .withColumn("sv_tab_avg_comment", col("small_video_comment")/col("small_tab_uv"))\
        .selectExpr("groupid", "small_video_play", "small_video_duration", "small_video_comment", "small_tab_uv", "round(sv_tab_vv, 2) as sv_tab_vv",
                    "round(sv_tab_avg_duration, 2) as sv_tab_avg_duration", "round(sv_tab_avg_comment, 4) as sv_tab_avg_comment").orderBy("groupid")
    df.toPandas().to_csv("~/sunjianqiang/sm_daily_report/grouped_vv_duration_Comment_"+date+".csv")

def nonTencentVv(spark, date):
    sql = """ select
                 day,
                 --case when cast(client_version as int) >= 13600 then 1 else 0 end as if_136,
                 case 
                   when app_package between  '1348' and '1447' 
                     or app_package between  '2348' and '2447'
                     or app_package between  '2448' and '2547'
                     or app_package between  '2598' and '2647'
                     or app_package between  '2868' and '2887'
                     or app_package between  '2888' and '2897'
                     or app_package between  '1448' and '1547'
                     or app_package between  '1548' and '1647'
                     or app_package between  '2648' and '2667'
                     or app_package between  '2768' and '2797'
                     or app_package between  '3233' and '3532'
                     or app_package between  '2993' and '2994'
                     or app_package between  '3015' and '3032'
                     or app_package between  '3533' and '3582'
                     or app_package between  '2995' and '3014'
                     or app_package between  '3723' and '3742'
                     or app_package between  '3873' and '3892'
                     or app_package between  '4025' and '4044'
                     or app_package between  '4985' and '5004'
                     or app_package between  '5105' and '5154'
                     or app_package between  '5155' and '5204'
                     or app_package between  '5375' and '5474'
                     or app_package between  '5475' and '5494'
                	 then 'tencent' else 'nontencent'
                    end as if_tencent,
                    count( case when cmd = 110 and from_source in ('3', '5') then device else null end ) as small_video_play,
                    sum(   case when cmd = 112 and page = 3 then cast(duration as float) else 0 end )/60000.0 as small_video_duration,
                    count(distinct case when cmd = 119 and menu_key = 2 then device else null       end ) as small_tab_uv
                  from  bdm_kanduoduo.kanduoduo_log_info_p
                  where day = '{0}' and is_dubious = 0 and device is not null and trim(device) <> '' 
                  group by day,
                 --case when cast(client_version as int) >= 13600 then 1 else 0 end as if_136,
                 case 
                   when app_package between  '1348' and '1447' 
                     or app_package between  '2348' and '2447'
                     or app_package between  '2448' and '2547'
                     or app_package between  '2598' and '2647'
                     or app_package between  '2868' and '2887'
                     or app_package between  '2888' and '2897'
                     or app_package between  '1448' and '1547'
                     or app_package between  '1548' and '1647'
                     or app_package between  '2648' and '2667'
                     or app_package between  '2768' and '2797'
                     or app_package between  '3233' and '3532'
                     or app_package between  '2993' and '2994'
                     or app_package between  '3015' and '3032'
                     or app_package between  '3533' and '3582'
                     or app_package between  '2995' and '3014'
                     or app_package between  '3723' and '3742'
                     or app_package between  '3873' and '3892'
                     or app_package between  '4025' and '4044'
                     or app_package between  '4985' and '5004'
                     or app_package between  '5105' and '5154'
                     or app_package between  '5155' and '5204'
                     or app_package between  '5375' and '5474'
                     or app_package between  '5475' and '5494'
                 	then 'tencent' else 'nontencent' end
                   """.format(date)
    print(sql)
    df = spark.sql(sql)
    result = df.selectExpr("day", "if_tencent", "small_video_play", "small_video_duration", "small_tab_uv", "small_video_play/small_tab_uv as small_tab_vv", "small_video_duration/small_tab_uv as avg_small_tab_duration")
    result.toPandas().to_csv("~/sunjianqiang/sm_daily_report/if_tencent_small_tab_vv_"+date+".csv")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Small video daily report").enableHiveSupport().getOrCreate()
    yesterday = sys.argv[1]
    VvAndDuration(spark, yesterday, 2)
    appVersionVvAndAccount(spark, yesterday, 2)
    groupedVvDurationComment(spark, yesterday)
    nonTencentVv(spark, yesterday)









