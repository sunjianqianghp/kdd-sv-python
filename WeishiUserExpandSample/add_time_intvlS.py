from pyspark.sql import SparkSession

def getTimeInterval(spark):
    sqlRequest = """select
                      c.device,
                      case when hour between '09' and '11' then 1
                           when hour between '13' and '17' then 2
                           else 0
                        end as time_intvl,
                      count(1) as ct
                    from ( select 
                             device, hour 
                             from bdm_kanduoduo.kanduoduo_log_info_p 
                            where day between '2019-07-14' and '2019-07-23' 
                              and cmd = 137
                              and from_source in (1,3,4,5)
                              and is_dubious = 0
                              and device is not null
                              and trim(device) <> ''
                              ) c
                    join ( select device from test.hottopic_device_last7d_without_ws_sjq ) b
                      on c.device = b.device
                group by c.device,
                  case
                    when hour between '09' and '11' then 1
                    when hour between '13' and '17' then 2
                    else 0
                   end"""
    df = spark.sql(sqlRequest)
    return df

if __name__ == '__main__':
    spark = SparkSession.builder.appName("add_time_intervalS").enableHiveSupport().getOrCreate()
    visit_time_interval = getTimeInterval(spark)
    visit_time_interval.write.mode("overwrite").saveAsTable("test.ws_visit_time_intervalS_sjq") #7297633 