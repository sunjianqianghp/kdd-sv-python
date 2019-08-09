from pyspark.sql import SparkSession

def getComb(spark):
    sqlRequest = """select 
                     t1.device,
                     t1.tag,
                     t1.wday,
                     t2.free_ratio,
                     t2.visit_hours,
                     t3.wifi_ratio,
                     t4.if_reg,
                     t5.info_app_num
                    from (      select device, tag, wday from test.baseData_sjq ) t1
                    left join ( select device, sum(case when time_intvl = 0              then ct else 0 end)/sum(ct) as free_ratio, sum(ct) as visit_hours from test.visit_time_interval_sjq group by device ) t2 on t1.device = t2.device
                    left join ( select device, sum(case when network in ("wifi", "WiFi") then ct else 0 end)/sum(ct) as wifi_ratio   from test.network_distr_sjq       group by device ) t3 on t1.device = t3.device
                    left join ( select device, round(sum(case when if_reg = 1 then ct else 0 end)/sum(ct), 0)        as if_reg       from test.if_reg_sjq              group by device ) t4 on t1.device = t4.device
                    left join ( select device, max(num)                                                              as info_app_num from test.uid_infoApp_num_sjq     group by device ) t5 on t1.device = t5.device
                 """
    df1 = spark.sql(sqlRequest)
    device_brand_sql = """select 
                            device, brand, row_number()over(partition by device order by ct desc) as rk
                          from (select 
                                  device, brand, sum(ct) as ct 
                                from test.brand_level_distr_sjq 
                               group by device, brand ) a 
                       """
    device_brand = spark.sql(device_brand_sql).where("rk = 1").selectExpr("device as device1", "brand")
    device_level_sql = """select 
                            device, level, row_number()over(partition by device order by ct desc) as rk
                          from (select 
                                  device, level, sum(ct) as ct 
                                from test.brand_level_distr_sjq 
                               group by device, level ) a 
                       """
    device_level = spark.sql(device_level_sql).where("rk = 1").selectExpr("device as device2", "level")
    device_client_version_sql = """select 
                                 device, client_version, row_number()over(partition by device order by ct desc) as rk
                                from (select device, client_version, ct from test.client_version_sjq ) a 
                            """
    device_client_version = spark.sql(device_client_version_sql).where("rk = 1").selectExpr("device as device3", "client_version")

    device_brand_level = device_brand.join(device_level, device_brand.device1 == device_level.device2).selectExpr("device1", "brand", "level")
    device_brand_level_appVersion = device_brand_level.join(device_client_version, device_brand_level.device1 == device_client_version.device3).selectExpr("device1", "brand", "level", "client_version")
    comb = df1.join(device_brand_level_appVersion, df1.device == device_brand_level_appVersion.device1, "left").select("device", "tag", "wday", "free_ratio", "visit_hours", "wifi_ratio", "client_version", "if_reg", "info_app_num", "brand", "level")
    return comb

if __name__ == "__main__":
    spark = SparkSession.builder.appName("combination").enableHiveSupport().getOrCreate()
    comb = getComb(spark)
    comb.write.mode("overwrite").saveAsTable("test.comb_sjq")
