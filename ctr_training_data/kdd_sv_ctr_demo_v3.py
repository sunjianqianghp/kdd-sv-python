# coding: utf-8

import numpy as np
import pandas as pd
import os.path as op
import subprocess
from pyspark.sql import SparkSession
import timeit
import tensorflow as tf
import os
import mmh3
import sys
import shutil
import threading
import time
import math
from multiprocessing import Pool
import os, time, random

spark = SparkSession.builder.appName("kdd_sv_ctr_data").enableHiveSupport().getOrCreate()


def get_dataset(date):
    date=date
    info_sql = """
             select label1,label2,video_id,device,category 
             from  bdm_kanduoduo.kdd_sv_ctr_info
             where thedate='%s'
             """% date
    all_info = spark.sql(info_sql)
    df_info = all_info.toPandas()
    new_cols1 = ['label1','label2','video_id','device','category']
    df_info.columns = new_cols1

    rank_sql_0 = """
                 select device,cate_list
                 from  bdm_kanduoduo.kdd_sv_ctr_rank_info_0
                 where day='%s'
                 """ % date
    rank_info_0 = spark.sql(rank_sql_0)
    df_rank_0 = rank_info_0.toPandas()
    new_cols2 = ['device', 'cate_list_0']
    df_rank_0.columns=new_cols2

    rank_sql_1 = """
                     select device,cate_list
                     from  bdm_kanduoduo.kdd_sv_ctr_rank_info_1
                     where day='%s'
                     """ % date
    rank_info_1 = spark.sql(rank_sql_1)
    df_rank_1 = rank_info_1.toPandas()
    new_cols3 = ['device', 'cate_list_1']
    df_rank_1.columns = new_cols3

    print('get two dataframe OK')
    return df_info,df_rank_0,df_rank_1


def data_process(df_info, df_rank_0,df_rank_1,date):
    s = date.replace("-", "")
    print(df_info.head(10))
    print(df_info.shape)
    print(df_rank_0.head(10))
    print(df_rank_0.shape)
    print(df_rank_1.head(10))
    print(df_rank_1.shape)

    rs = pd.merge(df_info, df_rank_0, on='device', how='left')
    rs = rs[['label1', 'label2', 'video_id', 'device', 'category', 'cate_list_0']]
    rs = rs.fillna(value='0 0 0 0')
    rs = pd.merge(rs, df_rank_1, on='device', how='left')
    rs = rs[['label1', 'label2', 'video_id', 'device', 'category', 'cate_list_0','cate_list_1']]
    rs = rs.fillna(value='0 1 0 0')

    rs['rs'] = rs['label1'].map(str) + ' ' + rs['label2'].map(str) + ' ' + rs['video_id'].map(str) + ' ' + rs['device'].map(str) + ' ' + rs['category'].map(str) + ' ' + rs['cate_list_0'] + ' ' + rs['cate_list_1']
    rs = rs[['rs']]
    #root = "/home/cpc/liuyulin/kdd/kdd_csv/"
    #rs.to_csv(root+'sv_ctr_data_'+s+'.csv', index=False, header=False)
    print('merge to get csv OK')
    n = rs.shape[0]
    print(n)
    t = math.floor(n / 5)
    print(t)
    rs1 = rs[0:t]
    rs2 = rs[t:2 * t]
    rs3 = rs[2 * t:3 * t]
    rs4 = rs[3 * t:4 * t]
    rs5 = rs[4 * t:]
    return rs1,rs2,rs3,rs4,rs5,n,t


def mkdir(date):
    # 去除首位空格
    date=date
    root = "/home/cpc/liuyulin/kdd/"
    path = root+date
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("\\")
    isExists = os.path.exists(path)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录&创建目录操作函数
        os.makedirs(path)
        print (path + ' 创建成功')
        return
    else:
        # 如果目录存在则不创建，并提示目录已存在
        shutil.rmtree(path)
        os.makedirs(path)
        print(path + '删除&创建')
        return


def generate_tfrecords(rs,date,index,sample_idx):
    print('Run task %s (%s)...' % (index, os.getpid()))
    print("start to run data into tfrecords")
    p = rs.shape[0]
    current_path = "/home/cpc/liuyulin/kdd/"+date+'/'
    s=str(index)
    outfile = current_path + 'part_' + s + ".tfrecords"
    writer = tf.python_io.TFRecordWriter(outfile)
    step=0
    sample_idx = sample_idx
    for _, row in rs.iterrows():
        sample_idx = sample_idx + 1
        dt = "".join(row)
        data = dt.split(" ")
        labels = [int(j) for j in data[:2]]
        data[3] = mmh3.hash("device" + data[3])
        data[2] = mmh3.hash("videoid" + data[2])
        data[4] = mmh3.hash("category" + data[4])
        dense = [int(i) for i in data[2:5]]
        sparse = data[5:]
        idx0 = []
        idx1 = []
        idx2 = []
        id_arr = []
        for i in range(len(sparse)):
            if i % 4 == 0:
                idx0.append(int(sparse[i]))
            if i % 4 == 1:
                idx1.append(int(sparse[i]))
            if i % 4 == 2:
                idx2.append(int(sparse[i]))
            if i % 4 == 3:
                id_arr.append(mmh3.hash("category" + sparse[i].strip()))
        # 将数据转化为原生 bytes
        example = tf.train.Example(features=tf.train.Features(feature={
            "sample_idx":
                tf.train.Feature(int64_list=tf.train.Int64List(value=[sample_idx])),
            "label":
                tf.train.Feature(int64_list=tf.train.Int64List(value=labels)),
            "dense":
                tf.train.Feature(int64_list=tf.train.Int64List(value=dense)),
            "idx0":
                tf.train.Feature(int64_list=tf.train.Int64List(value=idx0)),
            "idx1":
                tf.train.Feature(int64_list=tf.train.Int64List(value=idx1)),
            "idx2":
                tf.train.Feature(int64_list=tf.train.Int64List(value=idx2)),
            "id_arr":
                tf.train.Feature(int64_list=tf.train.Int64List(value=id_arr))
        }))
        step=step+1
        q=math.ceil(p/6)
        if step >q:
            index=index+1
            s = str(index)
            outfile = current_path +  'part_'+s+ ".tfrecords"
            writer = tf.python_io.TFRecordWriter(outfile)
            step =0
        writer.write(example.SerializeToString())  # 序列化为字符串

    writer.close()
    print("Successfully for" + "tfrecords")

if __name__ == "__main__":
    print("start")
    start_time = timeit.default_timer()
    date = sys.argv[1]
    df_info,df_rank_0,df_rank_1=  get_dataset(date)
    rs1,rs2,rs3,rs4,rs5,n,t=data_process(df_info, df_rank_0,df_rank_1,date)
    mkdir(date)
    print('Parent process %s.' % os.getpid())  ## 获取进程id
    p = Pool(5)
    for i in range(5):
        index=i*6
        sample_idx=i*t-1
        if i ==0 : rs=rs1
        if i ==1:  rs=rs2
        if i ==2 : rs=rs3
        if i ==3:  rs=rs4
        if i ==4 :
            rs=rs5
            sample_idx = n-i*t-1
        p.apply_async(generate_tfrecords, args=(rs,date,index,sample_idx))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
    current_path = "/home/cpc/liuyulin/kdd/" + date + '/'
    f = open(current_path + 'count', "w")
    c = str(n)
    f.write(c)
    f.close()
    f = open(current_path + 'success', 'w')
    f.close()
    end_time = timeit.default_timer()
    print("\nThe pretraining process ran for {0} minutes\n".format((end_time - start_time) / 60))
    print("successful")