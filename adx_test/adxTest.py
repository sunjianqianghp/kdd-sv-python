# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import datetime

def getTimeIntval(start_date, start_hour, end_date, end_hour):
    """
    :param start_date: string, format: yyyy-mm-dd
    :param start_hour: string format: hh
    :param end_date:
    :param end_hour:
    :return: string of time Interval
    """
    startStr = start_date + " " + start_hour
    endStr = end_date + " " + end_hour
    startTime = datetime.datetime.strptime(startStr, "%Y-%m-%d %H")
    endTime = datetime.datetime.strptime(endStr, "%Y-%m-%d %H")
    if startTime >= endTime:
        raise Exception("End time is before start time!!")
    if start_date == end_date:
        return "day = '{0}' and hour between '{1}' and '{2}'".format(start_date, start_hour, end_hour)
    startDate = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    endDate = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    delta = endDate - startDate
    if delta.days == 1:
        return "((day = '{0}' and hour >= '{1}') or (day = '{2}' and hour <= '{3}'))".format( start_date, start_hour, end_date, end_hour )
    elif delta.days > 1:
        start_date2 = datetime.datetime.strftime(startDate + datetime.timedelta(days=1), "%Y-%m-%d")
        end_date2 = datetime.datetime.strftime(endDate + datetime.timedelta(days=-1), "%Y-%m-%d")
        return "((day = '{0}' and hour >= '{1}') or (day = '{2}' and hour <= {3}) or (day between '{4}' and '{5}'))".format( start_date, start_hour, end_date, end_hour, start_date2, end_date2 )

def getBaseData(spark, app_versions, timeInterval, testDual, test_groups):
    sql1 = """ select
               case when t2.groupid in ('{2}') then 'test' else 'dual' end as ab_test1,
               t2.adslot_id,
               case when t2.adsrc = 22 then 0 else 1 end as if_cpc,
               sum( case when adsrc in (1, 28) and charge_type <> 2 then case when price>10000    or price<0 then 0 else isclick*price end else 0 end )*0.82/100 as cost_cpc,
               sum( case when adsrc in (1, 28) and charge_type =  2 then case when price>10000000 or price<0 then 0 else isshow*price  end else 0 end )*0.82/100000 +
               sum( case when adsrc not in (1, 28) then case when price>10000000 or price<0 then 0 else isshow*price end else 0 end )/100000 as cost_cpm,
               sum(t2.isshow)  as shown,
               sum(t2.isclick) as clickn,
               sum(t2.exp_ctr)/1000000 as exp_ctr_total,
               count(1) as ct
             from (
                 select
             	  day, adsrc, adslot_id, substr(md5(uid), 2, 1) as groupid, charge_type, isshow, isclick, price, exp_ctr
                 from
                   dl_cpc.cpc_basedata_union_events
                 where {0}
                  and adsrc in (1, 22, 28)
             	  and media_appsid = '80002819'
             	  and isfill = 1
             	  and {1}
             	  and media_app_versioncode in ( '{3}' )
             	  and substring(os_version, 1, 1) between '1' and '8'
                   ) t2
             group by
               case when t2.groupid in ('{2}') then 'test' else 'dual' end,
               t2.adslot_id,
               case when t2.adsrc = 22 then 0 else 1 end""".format( timeInterval, testDual, test_groups, app_versions )
    print(sql1)
    time.sleep(3)
    base = spark.sql(sql1)
    return base

def getAdslotName(spark, adslotIdList, date1, date2):
    date0 = datetime.datetime.strftime( datetime.datetime.strptime(date1, "%Y-%m-%d") + datetime.timedelta(days=-1) , "%Y-%m-%d")
    sql = """select
              adslot_id as adslot_id2,
              adslot_name
            from
              qttdm.dm_comm_media_cate_adslot_kpi_di
            where
              dt between '{1}' and '{2}'
             and adslot_id in ({0})
            group by
              adslot_id,
              adslot_name
           """.format( ",".join([str(x) for x in adslotIdList]), date0, date2 )
    print(sql)
    time.sleep(3)
    idName = spark.sql(sql)
    return idName

def getDau(spark, app_versions, timeInterval, test_dual_groups, test_groups ):
    sql = """select
                case when substr(md5(device), 2, 1) in ('{3}') then 'test' else 'dual' end as ab_test2,
                count(distinct device ) as dau
              from bdm_kanduoduo.kanduoduo_log_info_p_byhour a
              where {1} 
                and is_dubious = 0
                and device is not null  
                and trim(device) <> '' 
             	and cmd in ('136','112','111','110','137','102','162','163','121')
             	and device not in ('0','00000000','00000000000000','000000000000000','111111111111111','111111111111119','11223344','123456789012345','123456789012347','222222222222222','33333333333334')
             	and substr(md5(device), 2, 1) in ( '{2}' )
                and client_version in ( '{0}' )
                and substring(osversion, 1, 1) between '1' and '8'
              group by 
                case when substr(md5(device), 2, 1) in ('{3}') then 'test' else 'dual' end
          """.format(app_versions, timeInterval, test_dual_groups, test_groups )
    print(sql)
    time.sleep(3)
    dau = spark.sql(sql)
    return dau

if __name__ == "__main__":
    spark = SparkSession.builder.appName("adx monitor").enableHiveSupport().getOrCreate()
    ## 时间参数
    d1 = "2019-07-23"
    h1 = "00"
    d2 = "2019-07-23"
    h2 = "23"
    ## 实验对照组参数
    app_versions = "','".join( ['13800', '13801', '13803', '13804', '13805', '13806'])
    test_adslots, test_groups = [7025441], ['7']
    dual_adslots, dual_groups = [7180707], ['6']
    adslotList = test_adslots + dual_adslots
    ## 时间区间
    timeInterval = getTimeIntval(d1, h1, d2, h2)
    ## 实验设计
    testDual_param = """(( adslot_id in ( {0} ) and substr(md5(uid), 2, 1) in ( '{1}' )) or (adslot_id in ( {2} ) and substr(md5(uid), 2, 1) in ( '{3}' ) ))"""\
        .format( ",".join( [str(x) for x in test_adslots] ), "','".join(test_groups),  ",".join([str(x) for x in dual_adslots]), "','".join(dual_groups)  )
    ## 实验组
    test_group_param = "','".join(test_groups)
    ## 实验组合对照组
    test_dual_group_param = "','".join(test_groups + dual_groups)

    base = getBaseData(spark, app_versions, timeInterval, testDual_param, test_group_param) ## ab_test1, adslot_id, if_cpc, cost_cpc, cost_cpm, shown, clickn, exp_ctr_total, ct

    base.persist()

    adslotIdName = getAdslotName( spark, adslotList, d1, d2 ) ## adslot_id2, adslot_name

    dau = getDau(spark, app_versions, timeInterval, test_dual_group_param, test_group_param) ## ab_test2, dau

    result0 = base.join( adslotIdName, base.adslot_id == adslotIdName.adslot_id2, "left" )\
        .selectExpr("ab_test1", "adslot_id", "adslot_name", "if_cpc", "cost_cpc", "cost_cpm", "case when if_cpc=1 then cost_cpc+cost_cpm else cost_cpm end as fee",
                    "shown", "clickn", "exp_ctr_total", "ct", "exp_ctr_total/ct as avg_exp_ctr")

    result1 = result0.join(dau, result0.ab_test1 == dau.ab_test2, "left")\
        .selectExpr("ab_test1", "adslot_id", "adslot_name", "if_cpc", "cost_cpc", "cost_cpm", "fee", "shown", "clickn", "fee*1000.0/shown as cpm", "dau", "fee/dau as arpu", "avg_exp_ctr")

    result2 = result0.groupBy("adslot_name", "adslot_id", "ab_test1").agg(sum("fee").alias("fee"), sum("shown").alias("shown"), sum("clickn").alias("clickn"), sum("exp_ctr_total").alias("exp_ctr_total"), sum("ct").alias("ct") )\
        .selectExpr("adslot_name", "adslot_id", "ab_test1", "fee", "shown", "clickn", "fee*1000.0/shown as cpm", "exp_ctr_total/ct as avg_exp_ctr")

    result3 = result2.join(dau, result2.ab_test1 == dau.ab_test2, "left").selectExpr( "adslot_name", "adslot_id", "ab_test1", "fee", "shown", "clickn", "cpm", "dau", "fee/dau as arpu", "avg_exp_ctr" )

    result1.orderBy( "adslot_name", desc("ab_test1"), "if_cpc").write.mode("overwrite").saveAsTable("test.detail_sjq")
    result3.orderBy( "adslot_name", desc("ab_test1") ).write.mode("overwrite").saveAsTable("test.summary_sjq")

    # result1.write.mode("overwrite").saveAsTable("test.detail_sjq")
    # result3.write.mode("overwrite").saveAsTable("test.summary_sjq")




