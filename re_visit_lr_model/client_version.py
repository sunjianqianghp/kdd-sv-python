from pyspark.sql import SparkSession

def getNetworkDistr(spark):
    sqlRequest = """select
                      c.device,
                      client_version,
                      count(1) as ct
                    from bdm_kanduoduo.kanduoduo_log_info_p c
                    join ( select device from test.baseData_sjq ) b
                      on c.device = b.device
                   where c.day between '2019-06-13' and '2019-07-12'
                     and c.cmd = 137
                     and c.from_source in (1,3,4,5)
                     and c.is_dubious = 0
                     and c.device is not null
                     and trim(c.device) <> ''
                  group by c.device,
                     client_version
                 """
    df = spark.sql(sqlRequest)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("client_version").enableHiveSupport().getOrCreate()
    network_distr = getNetworkDistr(spark)
    network_distr.write.mode("overwrite").saveAsTable("test.client_version_sjq") #135140
