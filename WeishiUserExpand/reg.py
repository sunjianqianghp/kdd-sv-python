from pyspark.sql import SparkSession

def getNetworkDistr(spark):
    sqlRequest = """select
                      c.device,
                      case when member_id is not null then 1 else 0 end as if_reg,
                      count(1) as ct
                    from bdm_kanduoduo.kanduoduo_log_info_p c
                    join ( select uid as device from test.wsBaseData_sjq ) b
                      on c.device = b.device
                   where c.day between '2019-07-09' and '2019-07-18'
                     and c.cmd = 137
                     and c.from_source in (1,3,4,5)
                     and c.is_dubious = 0
                     and c.device is not null
                     and trim(c.device) <> ''
                  group by c.device,
                      case when member_id is not null then 1 else 0 end
                 """
    df = spark.sql(sqlRequest)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("net_work").enableHiveSupport().getOrCreate()
    network_distr = getNetworkDistr(spark)
    network_distr.write.mode("overwrite").saveAsTable("test.ws_if_reg_sjq") #8389366