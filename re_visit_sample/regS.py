from pyspark.sql import SparkSession

def getNetworkDistr(spark):
    sqlRequest = """select
                      c.device,
                      case when member_id is not null then 1 else 0 end as if_reg,
                      count(1) as ct
                    from ( select 
                             device, member_id 
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
                  group by c.device,
                      case when member_id is not null then 1 else 0 end
                 """
    df = spark.sql(sqlRequest)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("regS").enableHiveSupport().getOrCreate()
    network_distr = getNetworkDistr(spark)
    network_distr.write.mode("overwrite").saveAsTable("test.if_regS_sjq") # 7698590