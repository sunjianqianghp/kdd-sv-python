from pyspark.sql import SparkSession

def getBrandLevelDistr(spark):
    sqlRequest = """select
                      c.device,
                      c.brand,
	                  c.level,
                      count(1) as ct
                    from ( select 
                             device, brand, level 
                            from bdm_kanduoduo.kanduoduo_log_info_p 
                           where day between '2019-07-14' and '2019-07-23'
                             and cmd = 137
                             and from_source in (1,3,4,5)
                             and is_dubious = 0
                             and device is not null
                             and trim(device) <> ''
                            )c
                    join ( select device from test.hottopic_device_last7d_without_ws_sjq ) b
                      on c.device = b.device
                    group by 
                         c.device,
                         c.brand, 
	                     c.level
                 """
    df = spark.sql(sqlRequest)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("brand_level_distrS").enableHiveSupport().getOrCreate()
    brand_level_distr = getBrandLevelDistr(spark)
    brand_level_distr.write.mode("overwrite").saveAsTable("test.ws_brand_level_distrS_sjq") # 7297633