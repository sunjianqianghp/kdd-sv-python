from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("prepareBaseData").enableHiveSupport().getOrCreate()
    sql = """select 
               t1.day, t1.uid, case when t2.uid is not null then 1 else 0 end as tag
              from ( select dt as day,searchid,uid
                       from dl_cpc.slim_union_log
                      where dt between '2019-07-09' and '2019-07-18'
                        and adsrc  = 1
                        and isshow = 1
                        and media_appsid = "80002819"
                        and ( charge_type is NULL or charge_type = 1 )
                        and ideaid > 0
                        and userid = 1558645
                        and uid not like "%.%"
                        and uid not like "%000000%"
                        and length(uid) in (14, 15, 36)
                  	  group by dt,searchid,uid ) t1
               left join (SELECT day, searchid, uid
                          FROM dl_cpc.dm_conversions_for_model
                         WHERE day between '2019-07-09' and '2019-07-18'
                           AND array_contains(conversion_target, 'api') 
                           AND userid = 1558645
                         GROUP BY day, searchid, uid) t2
                    on t1.day = t2.day
                   and t1.searchid = t2.searchid
                   and t1.uid = t2.uid 
              group by t1.day, t1.uid, case when t2.uid is not null then 1 else 0 end
            """
    df = spark.sql(sql)
    df.write.mode("overwrite").saveAsTable("test.match_to_drop_sjq")

