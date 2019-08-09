# coding:utf-8

import pymysql
import redis
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BaiduLabel").enableHiveSupport().getOrCreate()
resultMap = {}

def connectMySQL():
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
    return conn

# 从kdd_video_report表中获取视频的类别、时长等信息
def getVideoInfo():
    conn = connectMySQL()
    cursor = conn.cursor()
    sql1 = """
        select
            id as video_id
        from
            kdd_video_report
        where
            type between 100 and 500
        and
            type not in (101, 107, 108, 110, 156, 180, 191, 192, 200)
        and
            status = 2
    """
    print( "======= get video id from mysql =======")
    print( sql1 )
    cursor.execute(sql1)
    videoInfoRows = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    # 将videoInfoRows转成二维列表
    videoInfoList = [[str(x) for x in row] for row in videoInfoRows]
    return videoInfoList

def getBaiduLabel(videoInfoList):
    # 首先获得每个视频置信度最高的标签
    sql1 = """
        select
            video_id,
            label,
            confidence
        from
            (select
                video_id,
                label,
                cast(confidence as float) as confidence,
                row_number() over (partition by video_id order by cast(confidence as float) desc) as rk
            from
                dl_cpc.small_video_info 
            where
                type not in ('keyword', 'logo')
            and
                trim(label) <> '') temp
        where
            rk = 1
    """
    print("====== get data from hive =====")
    print(sql1)
    spark.sql(sql1).createOrReplaceTempView("base_table")

    # 获取每个视频的所属类别
    sql2 = """
        select
            video_id,
            (case when label like "%男%" then 'label_1'
                  when (label like '%美女%' or
                        label like '%美腿%' or
                        label like '%婚纱%' or
                        label like '%泳装%' or
                        label like '%姑娘%' or
                        label like '%舞蹈%' or
                        label like '%女人%' or
                        label like '%比基尼%' or
                        label like '%豹纹%' or
                        label like '%女生%' or
                        label like '%瑜伽%' or
                        label like '%旗袍%' or
                        label like '%化妆%' or
                        label like '%短裤%' or
                        label like '%高颜值%' or
                        label like '%低胸装%' ) then 'label_2'
                  when label like "%人物%" then 'label_3'
                  when label = "figure" then 'label_4'
                  when (label like "%自然%" or 
                        label like "%植物%" or 
                        label like "%风景%") then 'label_5'
                  when label like "%才艺%" then 'label_6'
                  when label like "%车%" then 'label_7'
                  when label like "%交通%" then 'label_8'
                  when (label like "%建筑%" or 
                        label like "%场所%") then 'label_9'
                  when (label like "%动物%" or 
                        label like "%猫%" or 
                        label like "%狗%" or 
                        label like "%鸟%" or 
                        label like "%萌宠%") then 'label_10'
                  when (label like "%美食%" or 
                        label like "%食物%" or 
                        label like "%蛋糕%") then 'label_11'
                  when label like "%家居%" then 'label_12'
                  when label like "%服装%" then 'label_13'
                  when (label like "%活动%" or 
                        label like "%体育%" or 
                        label like "%运动%" or 
                        label like "%旅行%") then 'label_14'
                  when (label like "%物品%" or 
                        label like "%公共设施%" or 
                        label like "%乐器%" or 
                        label like "机械设备" or 
                        label like "%娱乐%") then 'label_15'
                  when (label like "%动漫%" or 
                        label like "%动画%") then 'label_16'
            else 'label_17' end) as tag
        from
            base_table
    """
    print("====== get video's label =====")
    print(sql2)
    spark.sql(sql2).createOrReplaceTempView("hive_table")  #videoid, tag(label)
    spark.createDataFrame(videoInfoList).toDF("video_id").createOrReplaceTempView("mysql_table")

    sql3 = """
        select
            a.video_id,
            b.tag
        from
            mysql_table a
        inner join
            hive_table b
        on
            a.video_id = b.video_id
    """
    print( "====== get video's label =====")
    print(sql3)
    dataRows = spark.sql(sql3).collect()
    # valueList = []
    # count = 0
    # for row in dataRows:
    #     count += 1
    #     if count % 5000 == 0 or count == len(dataRows):
    #         print "===== %d =====" % count
    #     temp = str(row[0]) + "+" + str(row[1])
    #     if count < 100:
    #         print temp
    #     valueList.append(temp)
    # value = ",".join(valueList)
    # print value

    # sql4 = """
    #         select
    #             count(a.video_id) as num,
    #             b.tag
    #         from
    #             mysql_table a
    #         inner join
    #             hive_table b
    #         on
    #             a.video_id = b.video_id
    #         group by
    #             b.tag
    #         order by
    #             count(a.video_id)
    #     """
    # print "====== get video's label ====="
    # print sql4
    # dataRows = spark.sql(sql4).collect()
    # for row in dataRows:
    #     print row

    return dataRows

def saveRedis1(dataRows):
    host = "r-2ze554aee853e8a4.redis.rds.aliyuncs.com"
    port = 6379
    passwd = "6X42p7uTvqLCpmC7"
    db = 24
    conn = redis.Redis(host = host,
                       port = port,
                       password = passwd,
                       db = db)
    pipe = conn.pipeline(transaction=False)
    count = 0
    for row in dataRows:
        count += 1
        # 存单个video_id对应的label
        key = "baidu_" + str(row[0])
        value = str(row[1])
        pipe.set(key, value)
        if count % 5000 == 0 or count == len(dataRows):
            print "===== %d =====" % count
            pipe.execute()

def saveRedis2(dataRows):
    host = "r-2ze554aee853e8a4.redis.rds.aliyuncs.com"
    port = 6379
    passwd = "6X42p7uTvqLCpmC7"
    db = 24
    conn = redis.Redis(host = host,
                       port = port,
                       password = passwd,
                       db = db)
    dict = {}
    for row in dataRows:
        videoId = str(row[0])
        label = str(row[1])
        if label not in dict:
            list = [videoId]
            dict[label] = list
        else:
            dict[label].append(videoId)
    for key in dict:
        if key == "label_17":
            continue
        list = dict[key]
        value = ",".join(list)
        print "==============="
        print list
        print value
        print "==============="
        conn.set(key, value)


if __name__ == "__main__":
    videoInfoList = getVideoInfo()
    dataRows = getBaiduLabel(videoInfoList)
    saveRedis1(dataRows)
    saveRedis2(dataRows)



