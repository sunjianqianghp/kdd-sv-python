from pyspark.sql import SparkSession
import sys
import timeit

spark = SparkSession.builder.appName("hive").enableHiveSupport().getOrCreate()


def update_data(date):
    date = date
    all_sql = """
    insert overwrite table bdm_kanduoduo.kdd_sv_ctr_info partition(thedate='%s')
    select
        case when b.v_play is not null then 1 else 0 end as label1,
        case when b.v_play is not null then 0 else 1 end as label2,
        a.video_id,
        a.device,
        c.category
        from
        (select
            thedate,
            field['pv_id'].string_type as pv_id,
            field['video_id'].string_type as video_id,
            field['device'].string_type as device,
            1 as v_show
            from src_kanduoduo.kanduoduo_client_log
            where thedate='%s'
            and field['video_id'].string_type > 0
            and field['cmd'].string_type='137' --有效曝光
            and field['from'].string_type ='3' --小视频列表
            and field['device'].string_type is not null
            and trim(field['device'].string_type) <> '')a
        left
        join
        (select
            thedate,
            field['pv_id'].string_type as pv_id,
            field['video_id'].string_type as video_id,
            field['device'].string_type as device,
            1 as v_play
            from src_kanduoduo.kanduoduo_client_log
            where thedate='%s'
            and field['video_id'].string_type > 0
            and field['cmd'].string_type = '110' --视频播放
            and field['from'].string_type='3'    --小视频列表
            and field['device'].string_type is not null
            and trim(field['device'].string_type) <> '' )b
        on
        a.thedate = b.thedate and a.video_id = b.video_id
        and a.pv_id = b.pv_id and a.device = b.device
        join
        (select
            id,
            category
            from src_kanduoduo.video
            where category >= 100
            and category <= 500
            group by id, category)
        c
        on
        a.video_id = c.id
                """ % (date,date,date)
    spark.sql(all_sql)

    rank_base_sql = """
       insert overwrite table bdm_kanduoduo.kdd_sv_ctr_rank_basedata partition (thedate='%s')
       select
       a.device,
       b.category,
       count(1) as all_cnt --用户device观看各类视频category的数量
       from
       (select
        thedate,
       field['device'].string_type as device,
       field['video_id'].string_type as video_id
       from src_kanduoduo.kanduoduo_client_log
       where thedate='%s'
       and field['video_id'].string_type>0
       and field['cmd'].string_type = '110' --视频播放
       and field['from'].string_type='3'    --小视频列表
       and field['device'].string_type is not null
       and trim(field['device'].string_type) <> '')a
       join
       (select
           id,
           category,
           duration
       from src_kanduoduo.video
       where category>=100
       and category<=500
       group by id,category,duration)b on a.video_id=b.id
       group by a.device,b.category
       """ % (date, date)
    spark.sql(rank_base_sql)

    rank_sql_1 = """
          select
           '%s' as day,
           a.device,
           a.category,
           count(a.all_cnt) as all_cnt
           from
           (SELECT *
           from bdm_kanduoduo.kdd_sv_ctr_rank_basedata
           where thedate<='%s'
           and thedate>=date_sub('%s',10))a
           join
           (select device
           from bdm_kanduoduo.kdd_sv_ctr_info
           where thedate='%s'
           group by device)b on a.device=b.device
           group by '%s',a.device,a.category
           """ % (date, date, date, date,date)
    rs = spark.sql(rank_sql_1)
    rs.createOrReplaceTempView("data")

    rank_sql_2 = """
    insert overwrite table bdm_kanduoduo.kdd_sv_ctr_rank partition (day='%s')
select *
from
(
    select
        a.device
        ,a.category
        ,0 as tag1
        ,0 as tag2
        ,a.all_rank-1 as s_index
        ,0 as flag
    from
    (
        select
            device
            ,category
            ,all_cnt
            ,row_number() over (partition by device order by all_cnt desc) as all_rank
        from data
    ) a
    where a.all_rank<=5
    UNION ALL
    select
        b.device
        ,b.category
        ,0 as tag1
        ,1 as tag2
        ,b.all_rank-1 as s_index
        ,1 as flag
    from
    (
        select
            device
            ,category
            ,all_cnt
            ,row_number() over (partition by device order by all_cnt desc) as all_rank
        from data
        where category not in (101,107,108,110,156,180,192,329,371,372,376,397,435,440,442,459)
    ) b
    where b.all_rank<=5
) p
    """ % date
    spark.sql(rank_sql_2)

    rank_sql_3 ="""
          insert overwrite table bdm_kanduoduo.kdd_sv_ctr_rank_info_0 partition(day='%s')
          select
          device,
          concat_ws(' ', collect_list(s)) as cate_list
          from
          (select 
          device,
          concat(tag1,' ',tag2,' ',s_index,' ',category) as s
          from bdm_kanduoduo.kdd_sv_ctr_rank 
          where day='%s'
          and flag=0)a 
          group by device
          """% (date,date)
    spark.sql(rank_sql_3)

    rank_sql_4 = """
            insert overwrite table bdm_kanduoduo.kdd_sv_ctr_rank_info_1 partition(day='%s')
            select
            device,
            concat_ws(' ', collect_list(s)) as cate_list
            from
            (select 
            device,
            concat(tag1,' ',tag2,' ',s_index,' ',category) as s
            from bdm_kanduoduo.kdd_sv_ctr_rank 
            where day='%s'
            and flag=1)a 
            group by device
            """ % (date, date)
    spark.sql(rank_sql_4)
    return


if __name__ == "__main__":
    start_time = timeit.default_timer()
    print("start")
    date = sys.argv[1]
    update_data(date)
    end_time = timeit.default_timer()
    print("\nThe pretraining process ran for {0} minutes\n".format((end_time - start_time) / 60))
    print("successful")