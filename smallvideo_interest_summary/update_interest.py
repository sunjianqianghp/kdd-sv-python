# coding:utf-8

import sys
from pyspark.sql import SparkSession

def updateData(spark, date1, date2):
    sqlRequest1 = """select
               a.device,member_id, a.video_id,b.category,
               case 
            	  when cmd = '114' then 'vote'
            	  when cmd = '107' then 'collect'
            	  else 'other'
            	 end as action,
                case when cmd = '114' and action = '2' then -1 else 1 end as operate,
                a.day as day
            from 
                (select 
                    video_id,device, member_id, cmd, action, day
                from 
                    bdm_kanduoduo.kanduoduo_log_info_p
                where day between '{}' and '{}'
                  --and trim(device) <> ''
            	  --and device is not null
            	  and is_dubious = 0
                  and trim(video_id) <> ''
            	  and ((cmd = '114' and type = '2' ) 
            		or (cmd = '107' and from_source in ('9', '10')))
            	group by video_id,device, member_id, cmd, action, day
            		) a 
            join 
                (select 
                    id, category
                from src_kanduoduo.video
                where category >= 100
                  and category <= 1000
                  and category != 200
                  and status = 4
                group by 
                    id, category) b 
              on a.video_id = b.id
              """.format( date1, date2 )

    sqlRequest2 = """select
                       a.device,member_id, a.content_id as video_id, b.category, 'comment' as action,
                       1 as operate, a.day
                     from
                       (
                         select
                           device,member_id, content_id, day
                         from
                           bdm_kanduoduo.kanduoduo_server_log_info_p
                         where
                           day between '{}' and '{}'
                           and cmd = '400'
                         group by device,member_id, content_id, day
                       ) a
                       join (
                         select
                           id, category
                         from
                           src_kanduoduo.video
                         where
                           category >= 100
                           and category <= 1000
                           and category != 200
                           and status = 4
                         group by
                           id, category
                       ) b on a.content_id = b.id
                       """.format(date1, date2)
    df1 = spark.sql(sqlRequest1)
    df2 = spark.sql(sqlRequest2)
    df1.write.mode("overwrite").insertInto("bdm_kanduoduo.sv_device_operation_log")
    df2.write.mode("overwrite").insertInto("bdm_kanduoduo.sv_device_operation_log")
    print(sqlRequest1)
    print(sqlRequest2)

# create table if not exists bdm_kanduoduo.sv_device_operation_log
# (
#  device   string comment '设备号',
#  member_id string comment '用户id',
#  video_id string comment '视频id',
#  category string comment '类别',
#  action   string comment '行为，vote：点赞，comment：评论，collect：收藏, share: 分享',
#  operate  int    comment '操作， 1：正向操作，-1：负向操作'
# ) comment '设备视频行为操作表'
# partitioned by (day string comment '日期');

if __name__ == '__main__':
    spark = SparkSession.builder.appName("update_interest_daily").enableHiveSupport().getOrCreate()
    date1 = sys.argv[1]
    date2 = sys.argv[2]
    print([date1, date2])
    updateData(spark, date1, date2)
    print("&&&&&&&&&&&&&&&&&&&&&&&&& SUCCESS &&&&&&&&&&&&&&&&&&&&&&&&&&")

