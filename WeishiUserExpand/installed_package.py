#-*-coding: UTF-8 -*-
from pyspark.sql import SparkSession

def processPkg(pkg):
    pair = pkg.split("-")
    if len(pair) == 2:
        return pair[0]
    return

def countInfoApps(appList):
    infoList = ['com.smile.gifmaker','com.ss.android.ugc.aweme','com.ss.android.article.video','com.ss.android.ugc.live','com.yixia.videoeditor','com.meitu.meipaimv','tv.yixia.bobo','com.mobile.videonews.li.video','com.tudou.android','com.baidu.haokan','com.tencent.qqlive','com.youku.phone','com.qiyi.video','tv.danmaku.bili','com.cmcc.cmvideo','com.sohu.sohuvideo','com.letv.android.client']
    matchInfoList = [int(app in infoList)  for app in appList]
    return sum(matchInfoList)

def getUidInfoAppNum(spark):
    sql = """select
                a.load_date,
                a.uid,
                concat_ws(',', a.app_name) as pkgs1
               from dl_cpc.cpc_user_installed_apps a
               join ( select uid as device from test.wsBaseData_sjq ) b
                 on a.uid = b.device
              where a.load_date between '2019-07-09' and '2019-07-18'
          """
    df = spark.sql(sql)
    day_uid_infoAppNumRdd = df.rdd.map(lambda x: [x[0], x[1], [processPkg(ele) for ele in x[2].split(",")]])\
        .map(lambda x: [x[0], x[1], countInfoApps(x[2])])
    uidInfoAppNum = spark.createDataFrame(day_uid_infoAppNumRdd).toDF("load_date", "device", "num")
    return uidInfoAppNum

if __name__ == "__main__":
    spark = SparkSession.builder.appName("installed_package").enableHiveSupport().getOrCreate()
    uidInfoAppNum = getUidInfoAppNum(spark)
    uidInfoAppNum.write.mode("overwrite").saveAsTable("test.ws_uid_infoApp_num_sjq") #8428251