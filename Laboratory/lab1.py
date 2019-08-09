# coding:utf-8

from pyspark.sql import SparkSession
import pymysql
import redis
import sys
import datetime

spark = SparkSession.builder.appName("SvExploreInfoDaily").enableHiveSupport().getOrCreate()
resultMap = {}

def getMySQLData():
    host = "rm-2zem5lmi9vj0vm9e5.mysql.rds.aliyuncs.com"
    port = 3306
    user = "kdd_rec_w"
    passwd = "7cwK0EZxNi0wyrEb"
    db = "kdd_rec"
    conn = pymysql.connect(host = host,
                           port = port,
                           user = user,
                           passwd = passwd,
                           db = db)
    cursor = conn.cursor()
    sql = """
        select
            id as video_id,
            type as category_id,
            video_play_time as `time`
        from
            kdd_video_report
        where
            type between 100 and 500
        and
            type not in (101, 107, 108, 110, 156, 180, 191, 192, 200)
        and
            status = 2
    """
    print("======= get video id from mysql =======")
    print(sql)
    cursor.execute(sql)
    mysqlDataRows = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    mysqlData = [[str(x) for x in row] for row in mysqlDataRows]
    mysqlDataDF = spark.createDataFrame(mysqlData).toDF("video_id", "category_id", "time")
    return mysqlDataDF

def getCollectInfo(date, mysqlDataDF):
    mysqlDataDF.createOrReplaceTempView("mysql_table")
    sql1 = """
        select
            a.video_id,
            b.category_id,
            a.device,
            a.vote_cnt,
            a.collect_cnt,
            a.share_cnt
        from 
            (select 
                video_id,
                device,
                count(case when cmd = '114' then 1 else null end) as vote_cnt,
                count(case when cmd = '107' then 1 else null end) as collect_cnt,
                count(case when cmd = '116' then 1 else null end) as share_cnt
            from 
                bdm_kanduoduo.kanduoduo_log_info_p
            where 
                day >= '%s'
            and 
                trim(device) <> ''
            and 
                trim(video_id) <> ''
            group by 
                video_id,
                device) a 
        inner join 
            mysql_table b 
        on 
            a.video_id = b.video_id
        where
            a.collect_cnt > 0
    """ % date
    print "======= get collect info sql1 ========"
    print sql1
    spark.sql(sql1).createOrReplaceTempView("hive_table")

    sql2 = """
        select
            device,
            category_id,
            sum(collect_cnt) as cate_collect_cnt
        from
            hive_table
        group by
            device,
            category_id
    """
    print "======= get category_id info sql2 ========"
    print sql2
    spark.sql(sql2).createOrReplaceTempView("temp_table")

    sql3 = """
        select
            device,
            category_id,
            cate_collect_cnt
        from
            (select
                device,
                category_id,
                cate_collect_cnt,
                row_number() over (partition by device order by cate_collect_cnt desc) as rk
            from
                temp_table) temp
        where
            rk <= 2
    """
    print "======= get category_id info sql3 ========"
    print sql3
    collectInfoDF = spark.sql(sql3)
    collectInfoRows = collectInfoDF.collect()

    print "======= collect info num %d ======" % len(collectInfoRows)
    for row in collectInfoRows:
        device = str(row[0])
        category_id = str(row[1])
        cate_collect_cnt = int(row[2])
        # 首先判断当前用户是否在resultMap中，如果没有则直接添加到resultMap中
        if device not in resultMap:
            resultMap[device] = [(category_id, cate_collect_cnt, "收藏")]
        else:
            cateInfo = resultMap[device]
            # 然后判断是否给当前device推荐了两个视频类别，如果推荐数量不足两个，则直接添加
            # 添加的时候需要保证两个视频类别是不同的
            if len(cateInfo) < 2 and category_id != cateInfo[0][0]:
                resultMap[device].append((category_id, cate_collect_cnt, "收藏"))
            elif category_id != cateInfo[0][0]:
                # 如果已经添加了两个，那么先按照收藏数量对两个类别逆序排序
                # 接着比较当前类别视频的收藏数是否比已经添加的第二个类别的收藏数少，
                # 如果少则替换第二个，否则不进行任何操作
                cateInfo.sort(key = lambda x: x[1], reverse = True)
                if cate_collect_cnt > cateInfo[1][1]:
                    del cateInfo[1]
                    cateInfo.append((category_id, cate_collect_cnt, "收藏"))
                    resultMap[device] = cateInfo

def getOutStreamInfo(date, mysqlDataDF):
    mysqlDataDF.createOrReplaceTempView("mysql_table")
    sql1 = """
        select
            a.device,
            b.category_id,
            count(a.video_id) as out_cnt
        from
            (select 
                field["device"].string_type as device,
                field["video_id"].string_type as video_id
            from 
                src_kanduoduo.kanduoduo_client_log
            where 
                thedate >= '%s'
            and 
                field["cmd"].string_type = '110'
            and 
                field["from"].string_type = '3') a
        inner join
            mysql_table b
        on 
            a.video_id = b.video_id
        where 
            trim(a.device) <> ''
        and 
            trim(a.video_id) <> ''
        group by
            a.device, 
            b.category_id
    """ % date
    print "======= get out info sql1 ========"
    print sql1
    spark.sql(sql1).createOrReplaceTempView("out_info_table")

    sql2 = """
        select
            device,
            category_id,
            out_cnt
        from
            (select
                device,
                category_id,
                out_cnt,
                row_number() over (partition by device order by out_cnt desc) as rk
            from
                out_info_table) a
        where
            rk <= 2
    """
    print "======= get out info sql2 ========"
    print sql2
    outInfoDF = spark.sql(sql2)
    outInfoRows = outInfoDF.collect()

    print "======= out stream info num %d ======" % len(outInfoRows)
    for row in outInfoRows:
        device = str(row[0])
        category_id = str(row[1])
        out_cnt = int(row[2])
        if device not in resultMap:
            resultMap[device] = [(category_id, out_cnt, "外流点击")]
        elif len(resultMap[device]) < 2:
            cateInfo = resultMap[device]
            if category_id != cateInfo[0][0]:
                resultMap[device].append((category_id, out_cnt, "外流点击"))

def saveRedis():
    host = "r-2ze554aee853e8a4.redis.rds.aliyuncs.com"
    port = 6379
    passwd = "6X42p7uTvqLCpmC7"
    db = 24
    conn = redis.Redis(host = host,
                       port = port,
                       password = passwd,
                       db = db)
    pipe = conn.pipeline(transaction = False)
    count = 0
    for device in resultMap:
        count += 1
        key = "ins_" + device
        cates = []
        for category in resultMap[device]:
            cates.append(str(category[0]))
        value = ",".join(cates)
        # print "===== %s  %s =====" % (key, value)
        # if count == 100:
        #     break
        pipe.set(key, value)
        if count % 5000 == 0 or count == len(resultMap):
            print "==== insert into redis %d ====" % count
            pipe.execute()

def saveHive(date):
    allDataList = []
    for device in resultMap:
        data = [device]
        categoryInfoes = resultMap[device]
        for categoryInfo in categoryInfoes:
            data.append(str(categoryInfo[0]))
            data.append(str(categoryInfo[1]))
            data.append(str(categoryInfo[2]))
        gap = 7 - len(data)
        for i in range(gap):
            data.append("")
        allDataList.append(data)
    table = "test.wt_small_video_user_interest_" + str(date)
    resultDF = spark.createDataFrame(allDataList).toDF("device", "category_id1", "num1", "source1",
                                                       "category_id2", "num2", "source2")
    resultDF.repartition(10).write.mode("overwrite").saveAsTable(table)

if __name__ == "__main__":
    cur = sys.argv[1]
    now = datetime.datetime.strptime(cur, "%Y-%m-%d")
    date1 = (now + datetime.timedelta(days = -30)).strftime("%Y-%m-%d")
    date2 = (now + datetime.timedelta(days = -7)).strftime("%Y-%m-%d")
    mysqlDataDF = getMySQLData()  # video_id, category_id, `time`
    getCollectInfo(date1, mysqlDataDF)
    getOutStreamInfo(date2, mysqlDataDF)
    # count = 0
    # for key in resultMap:
    #     count += 1
    #     print key, resultMap[key]
    #     if count == 100:
    #         break
    saveRedis()
    print "===== save into redis success ===="
    date3 = now.strftime("%Y%m%d")
    saveHive(date3)
    print "===== save into hive success ===="
    print "==== user num = %d ====" % len(resultMap)
    # print u"文涛："
    # print resultMap["867550037801889"]
    # print u"建强："
    # print resultMap["866334031506864"]
    # print u"家麒："
    # print resultMap["868233032224014"]
    # print u"宇麟："
    # print resultMap["866538048310090"]



