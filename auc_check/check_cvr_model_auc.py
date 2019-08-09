# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import math

def getIntervalDistr(sc, scores_labels):
    """
    :param: sc: sparkcontext
    :param scores: RDD[score]
    :param labels: RDD[label]
    :return: RDD[(threshold, (count_pos_intv, count_neg_intv))]
    """
    scores_labels1 = scores_labels.mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
        .mapValues(lambda x: (x[0], x[1]-x[0])).sortByKey()\
        .zipWithIndex().map(lambda x: (x[1], x[0][1]))
    upper = scores_labels1.keys().max() + 1
    intvDist =scores_labels1.union(sc.parallelize([(upper, (0, 0))]))
    return intvDist

def getAccumulateAccount(intervalDistrRdd, pos, neg):
    """
    :param intervalDistrRdd: RDD[(threshold, (count_pos_intv, count_neg_intv))]
    :param pos: 正样本数量
    :param neg: 负样本数量
    :return: RDD[(threshold, (pos_accumlate_account, neg_accumulate_account))] desc sorted by keys
    """
    key = intervalDistrRdd.keys()
    upper = key.max() + 1
    kp = key.map(lambda x: (x, x)).flatMapValues(lambda x: range(x, upper)).map(lambda x: (x[1], x[0]))
    result0 = kp.join(intervalDistrRdd).values().reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    result = result0.mapValues(lambda x: (float(x[0])/pos, float(x[1])/neg)).sortByKey()
    return result

def getAuc(accumulate_rdd):
    """
    :param accumulate_rdd: 结构：RDD[(threshold, (pos_accumlate_account, neg_accumulate_account))]
    :return: AUC
    """
    heightL = accumulate_rdd.map(lambda x: (x[0], x[1][0]) )
    heightR = heightL.map(lambda x: (x[0] - 1, x[1]))
    cut_off_ptL = accumulate_rdd.map(lambda x: (x[0], x[1][1]))
    cut_off_ptR = cut_off_ptL.map(lambda x: (x[0] - 1, x[1]))
    heightSum = heightL.join(heightR).mapValues(lambda x: x[0] + x[1]).repartition(30)
    width = cut_off_ptL.join(cut_off_ptR).mapValues(lambda x: x[1] - x[0]).repartition(30)
    baseRdd = heightSum.join(width)
    area2 = baseRdd.mapValues(lambda x: x[0] * x[1] ).values().reduce(lambda x, y: x + y)
    auc = area2/2 if area2 > 0 else -area2/2
    return auc

def comb(sc, scores_labels):
    pos=scores_labels.map(lambda x: x[1]).sum()
    neg=scores_labels.map(lambda x: x[1]).count()-pos
    param1=getIntervalDistr(sc, scores_labels )
    param2=getAccumulateAccount(param1, pos, neg)
    auc=getAuc(param2)
    return auc

def getBaseData(spark):
    sqlRequest = """select
              t1.cvr_model_name,
              t1.unitid,
              t1.searchid,
              t1.exp_cvr as score,
              case when t2.label is not null then 1 else 0 end as label
            from
              (
                select
                  length( cvr_model_name) as cvr_model_name,
                  unitid,
                  searchid,
                  exp_cvr
                from
                  dl_cpc.cpc_basedata_union_events
                where
                  day = '2019-06-21'
                  --and hour = '12'
                  and cvr_model_name is not null
                  and length(cvr_model_name) > 12
                  and adsrc in (1, 28)
                  and media_appsid = '80002819'
                  and isclick = 1
                  and isshow = 1
                  and unitid in ( 2171008, 2156594,2184663 )
              ) t1
              left join (
                select
                  searchid,
                  label
                from
                  dl_cpc.ocpc_label_cvr_hourly
                where
                  `date` = '2019-06-21'
                  --and hour = '12'
                  and label = 1
              ) t2 on t1.searchid = t2.searchid
    """
    df = spark.sql(sqlRequest)
    return df



if __name__ == "__main__":
    spark = SparkSession.builder.appName("check cvr model auc").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    # base0 = getBaseData(spark) ## cvr_model_name, unitid, searchid, score, label
    # base0.write.mode("overwrite").saveAsTable("test.baseData_sjq")
    # base = spark.sql("select * from baseData_sjq")
    # model_unit_comb = base.groupBy("cvr_model_name", "unitid").agg(countDistinct("searchid").alias("ct")).select("cvr_model_name", "unitid", "ct").rdd.collect()

    # data = []
    # with open("/home/lechuan/sunjianqiang/data/base_data.csv", "r") as f:
    #     index = 0
    #     for line in f.readlines():
    #         if index == 0:
    #             index += 1
    #             continue
    #         data.append( [int(ele) for ele in line.split(",")] )
    #
    # if len(data) > 0:
    #     print("SUCCESS!!!!!!!!!!!!!")
    # else:
    #     raise Exception("DATA IS NULL")
    #
    # score0 = [ele[0] for ele in data]
    # label0 = [ele[1] for ele in data]
    #
    # score = sc.parallelize(score0)
    # label = sc.parallelize(label0)
    #
    # auc = comb(sc, score, label)
    # print("###########################################################################")
    # print(auc)
    # print("###########################################################################")

    result = []
    for model in [30, 31]:
        for unitid in [2156594, 2171008, 2184663]:
            base = spark.sql("select score, label from test.baseData_sjq where cvr_model_name = %d and unitid = %d"%(model, unitid) )
            scores_labels = base.rdd.map(lambda x: (int(x[0]), int(x[1])))
            auc = comb(sc, scores_labels)
            result.append([model, unitid, auc])
    df = spark.createDataFrame(result).toDF("model", "unitid", "auc")
    df.write.mode("overwrite").saveAsTable("test.checkauc_sjq")







