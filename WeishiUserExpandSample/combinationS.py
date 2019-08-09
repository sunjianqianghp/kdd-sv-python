from pyspark.sql import SparkSession

def getComb(spark):
    sqlRequest = """select 
                     t1.device,
                     t2.free_ratio,
                     t2.visit_hours,
                     t3.wifi_ratio,
                     t4.if_reg,
                     t5.info_app_num
                    from (      select device from test.hottopic_device_last7d_without_ws_sjq ) t1
                    join ( select device, sum(case when time_intvl = 0              then ct else 0 end)/sum(ct) as free_ratio, sum(ct) as visit_hours from test.ws_visit_time_intervalS_sjq group by device ) t2 on t1.device = t2.device
                    join ( select device, sum(case when network in ("wifi", "WiFi") then ct else 0 end)/sum(ct) as wifi_ratio   from test.ws_network_distrS_sjq       group by device ) t3 on t1.device = t3.device
                    join ( select device, round(sum(case when if_reg = 1 then ct else 0 end)/sum(ct), 0)        as if_reg       from test.ws_if_regS_sjq              group by device ) t4 on t1.device = t4.device
                    join ( select device, max(num)                                                              as info_app_num from test.ws_uid_infoApp_numS_sjq     group by device ) t5 on t1.device = t5.device
                 """
    df1 = spark.sql(sqlRequest)
    device_brand_sql = """select 
                            device, brand, row_number()over(partition by device order by ct desc) as rk
                          from (select 
                                  device, brand, sum(ct) as ct 
                                from test.ws_brand_level_distrS_sjq 
                               group by device, brand ) a 
                       """
    device_brand = spark.sql(device_brand_sql).where("rk = 1").selectExpr("device as device1", "brand")
    device_level_sql = """select 
                            device, level, row_number()over(partition by device order by ct desc) as rk
                          from (select 
                                  device, level, sum(ct) as ct 
                                from test.ws_brand_level_distrS_sjq 
                               group by device, level ) a 
                       """
    device_level = spark.sql(device_level_sql).where("rk = 1").selectExpr("device as device2", "level")

    device_brand_level = device_brand.join(device_level, device_brand.device1 == device_level.device2).selectExpr("device1", "brand", "level")
    comb = df1.join(device_brand_level, df1.device == device_brand_level.device1).select("device", "free_ratio", "visit_hours", "wifi_ratio", "if_reg", "info_app_num", "brand", "level")
    return comb

if __name__ == "__main__":
    spark = SparkSession.builder.appName("combinationS").enableHiveSupport().getOrCreate()
    comb = getComb(spark)
    comb.write.mode("overwrite").saveAsTable("test.ws_combS_sjq") #