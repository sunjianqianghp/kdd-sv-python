# coding:utf-8

from pyspark.sql import SparkSession
import pymysql

spark = SparkSession.builder.appName("SvExploreInfoDaily").enableHiveSupport().getOrCreate()

def getMySQLData():
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
    cursor = conn.cursor()
    sql = """
        SELECT type, a.id, valid_read_pv, validshow_pv, valid_read_pv/validshow_pv as ctr
        FROM (
        	SELECT id, type
        	FROM kdd_video_report
        	WHERE type BETWEEN 100 AND 500
        		AND type != 200
        		AND status = 2
            ) a
        	LEFT JOIN (
        		SELECT id
        			, SUM(ifnull(rec_validshow, 0) + ifnull(other_validshow, 0)) AS validshow_pv
        			, SUM(ifnull(rec_validread, 0) + ifnull(other_validread, 0)) AS valid_read_pv
        		FROM kdd_doc_pv_detail
        		GROUP BY id
        	) b
        	ON a.id = b.id
        WHERE b.validshow_pv >= 500
        	AND b.valid_read_pv < b.validshow_pv
    """
    print("======= get video id from mysql =======")
    print(sql)
    cursor.execute(sql)
    mysqlDataRows = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    mysqlData = [ row for row in mysqlDataRows]
    mysqlDataDF = spark.createDataFrame(mysqlData).toDF("category", "video_id", "valid_read", "valid_show", "ctr")
    # mysqlDataDF.write.mode("overwrite").saveAsTable("test.show_read_sjq")
    mysqlDataDF.createOrReplaceTempView("temp")
    sql2 = """
    select 
    *
    from (
    select
    *,
    row_number()over(partition by category order by ctr desc) as rk
    from temp tt
    ) t1 where rk <= 30
    """
    df = spark.sql(sql2)
    df.toPandas().to_csv("/home/lechuan/sunjianqiang/data/result.csv")
    df.write.mode("overwrite").saveAsTable("test.result_sjq")

if __name__ == '__main__':
    getMySQLData()