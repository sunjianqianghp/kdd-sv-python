from pyspark.sql import SparkSession

def getBrandLevelDistr(spark):
    sqlRequest = """select
       c.device,
       brand,
	   level,
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
      brand, 
	  level
    """
    df = spark.sql(sqlRequest)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("brand_level_distr").enableHiveSupport().getOrCreate()
    brand_level_distr = getBrandLevelDistr(spark)
    brand_level_distr.write.mode("overwrite").saveAsTable("test.brand_level_distr_sjq") #135140
