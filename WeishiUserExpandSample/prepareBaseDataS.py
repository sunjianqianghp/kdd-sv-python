from pyspark.sql import SparkSession
import time

def getHottopicDevice(spark):
    sqlRequest = """select
                      c.device
                    from bdm_kanduoduo.kanduoduo_log_info_p c
                   where c.day between '2019-07-17' and '2019-07-23'
                     and c.cmd = 137
                     and c.from_source in (1,3,4,5)
                     and c.is_dubious = 0
                     and c.device is not null
                     and trim(c.device) <> ''
                   group by c.device
                 """
    df0 = spark.sql(sqlRequest)
    df0.createOrReplaceTempView("hottopicDevice")

    sql2 = """select
                a.load_date,
                a.uid,
                concat_ws(',', a.app_name) as pkgs1
               from dl_cpc.cpc_user_installed_apps a
               join ( select device from hottopicDevice ) b
                 on a.uid = b.device
              where a.load_date between '2019-07-17' and '2019-07-23'
           """
    df1 = spark.sql(sql2)
    df1.createOrReplaceTempView("uid_date_pkgs")

    sql3 = """select 
                a.uid, 
                a.pkgs1
              from (select load_date, uid, pkgs1 from uid_date_pkgs) a
              join (select uid, max(load_date) as date1 from uid_date_pkgs group by uid ) b
                on a.uid = b.uid and a.load_date = b.date1
              group by a.uid, 
                       a.pkgs1
           """
    df2 = spark.sql(sql3)
    return df2

def judge(appList):
    if "com.qukandian.video" in appList :
        if "com.tencent.weishi" not in appList:
            return 1
    return 0



if __name__ == "__main__":
    spark = SparkSession.builder.appName("baseData").enableHiveSupport().getOrCreate()
    hottopicDevicePkgs = getHottopicDevice(spark) # uid, pkgs1
    resultRdd = hottopicDevicePkgs.rdd.map(lambda x: [x[0], x[1].split(",")]).mapValues(lambda x: [ele.split("-")[0] for ele in x]).map(lambda x: [x[0], judge(x[1])]).filter(lambda x: x[1] == 1)
    result = spark.createDataFrame(resultRdd).toDF("device", "flag").select("device")
    # result.show(20)
    # time.sleep(5)
    result.write.mode("overwrite").saveAsTable("test.hottopic_device_last7d_without_ws_sjq") #7297633
