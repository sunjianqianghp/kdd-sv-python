from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymysql


def connectMySQL():
    host = "rm-2zem5lmi9vj0vm9e5.mysql.rds.aliyuncs.com"
    port = 3306
    user = "kdd_rec_w"
    passwd = "7cwK0EZxNi0wyrEb"
    db = "kdd_rec"
    conn = pymysql.connect(host=host,
                           port=port,
                           user=user,
                           passwd=passwd,
                           db=db)
    return conn


def getSummaryData(spark):
    sql1 = """select
                video_id,
                valid_click/valid_show  as v_ctr,
                vote_cnt/click          as vote_rate,
                collect_cnt/click       as collect_rate
              from
                (
                  select
                    video_id,
                    sum(vote_cnt) as vote_cnt,
                    sum(collect_cnt) as collect_cnt,
                    sum(valid_show)  as valid_show,
                    sum(valid_click) as valid_click,
					sum(click_vv) as click 
                  from
                    bdm_kanduoduo.kanduoduo_algorithm_basedata t1
                  where
                    category between 100
                    and 500
                    and category != 200
                  group by
                    video_id
                ) t1
              where
                valid_show >= 100
            """
    print(sql1)
    df = spark.sql(sql1).fillna({"v_ctr":0.0, "vote_rate":0.0, "collect_rate":0.0}).withColumn("combScore", col("v_ctr") + col("vote_rate")*2.0 + col("collect_rate")*3.0)
    df.write.mode("overwrite").saveAsTable("test.video_summary_info_sjq")

if __name__ == '__main__':
    spark = SparkSession.builder.appName("baidu label new score").enableHiveSupport().getOrCreate()
    getSummaryData(spark)
    print("FINISHED")
