from pyspark.sql import SparkSession

def getTimeInterval(spark):
    sqlRequest = """select
                      c.device,
                      case when hour between '09' and '11' then 1
                           when hour between '13' and '17' then 2
                           else 0
                        end as time_intvl,
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
                  case
                    when hour between '09' and '11' then 1
                    when hour between '13' and '17' then 2
                    else 0
                   end"""
    df = spark.sql(sqlRequest)
    return df

if __name__ == '__main__':
    spark = SparkSession.builder.appName("add_time_interval").enableHiveSupport().getOrCreate()
    visit_time_interval = getTimeInterval(spark)
    visit_time_interval.write.mode("overwrite").saveAsTable("test.visit_time_interval_sjq") #135140