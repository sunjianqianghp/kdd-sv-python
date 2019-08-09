from pyspark.sql import SparkSession

def getNetworkDistr(spark):
    sqlRequest = """select
                      c.device,
                      network,
                      count(1) as ct
                    from ( select 
                             device, network 
                             from bdm_kanduoduo.kanduoduo_log_info_p 
                            where day between '2019-06-22' and '2019-07-21'
                              and cmd = 137
                              and from_source in (1,3,4,5)
                              and is_dubious = 0
                              and device is not null
                              and trim(device) <> ''
                          ) c
                    join ( select device from test.hottopic_device_last3d_without_qtt_sjq ) b
                      on c.device = b.device
                    group by c.device, network
                 """
    df = spark.sql(sqlRequest)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("net_workS").enableHiveSupport().getOrCreate()
    network_distr = getNetworkDistr(spark)
    network_distr.write.mode("overwrite").saveAsTable("test.network_distrS_sjq") # 7698590