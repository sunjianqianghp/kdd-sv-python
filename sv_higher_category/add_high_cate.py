# coding:utf-8

import pymysql
from pyspark.sql import SparkSession
import json
import re

def getResult( ):
    result = []
    exception = []
    with open('/home/lechuan/sunjianqiang/category.txt') as f:
        for line in f.readlines():
            # print(line)
            ll = line.strip().split("\t")
            if len(ll) != 4 :
                print("***************This line is not compatible*************")
                exception.append(ll)
                continue
            midresult = [ll[0], unicode(ll[1], "utf-8"), ll[2], unicode(ll[3], "utf-8")]
            result.append(midresult)
    return result,exception

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

def save2mysql(lst):
    """
    :param lst:  列表，以"[一级分类， 一级分类名称，二级分类， 二级分类名称]"为元素
    :return: NULL
    """
    map1 = {}
    lst1 = [ [(x[0], x[1]), x[2] ] for x in lst]
    for ele in lst1:
        key = ele[0]
        value = ele[1]
        print(key, key[1], type(key[1]))
        if key in map1.keys():
            map1[key].append(value)
        else:
            map1[key] = [value]
    lst2 = map1.items()
    map2 = {ele[0][0]:{"id": ele[0][0], "name":ele[0][1], "cates":ele[1]}  for ele in lst2}
    value = json.dumps( map2 ) #.decode('raw_unicode_escape')
    print(value )
    name = "'sv_class_1st_2nd'"
    sql = "replace into kdd_param values (%s, '%s', 1, now())" % (name, value )
    print(sql)
    conn = connectMySQL()
    cursor = conn.cursor()
    ok = cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


# create table if not exists bdm_kanduoduo.small_video_category_relation
# (
#  high_category_id    string comment '视频高级分类id',
#  high_category_name  string comment '视频高级分类名称',
#  category_id         string comment '视频分类id',
#  category_name       string comment '视频分类名称'
# ) comment '视频分类从属关系表';

if __name__ == '__main__':
    # spark = SparkSession.builder.appName("add higher category for small video").enableHiveSupport().getOrCreate()
    # sc = spark.sparkContext
    result, exceptions = getResult()
    assert(len(exceptions) == 0, "THERE ARE SOME EXCEPTIONS")
    save2mysql(result)

    # df = spark.createDataFrame(result).toDF("high_category_id", "high_category_name", "category_id", "category_name")
    # df.write.mode("overwrite").insertInto("bdm_kanduoduo.small_video_category_relation")



    print("##############################################  SUCCESS  ####################################################")



