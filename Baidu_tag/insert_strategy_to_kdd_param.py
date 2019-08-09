# coding:utf-8

import pymysql

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

def insertFunc( ):
    value = """{"doc_source":"union","recdoc_select":[{"doc_source":"union","recdoc_select":[{"doc_source":"sv_90_hottest_handlist_by_cates","candidate_size":2,"percent":1,"param":[{"key":"specifiedCates","value":"107"}]},{"doc_source":"random_by_set_cate_or_one_cate","candidate_size":2,"percent":1,"param":[{"key":"cate","value":"107"}]}],"candidate_size":1,"percent":0.125},{"doc_source":"union","recdoc_select":[{"doc_source":"explore_by_cates_new","candidate_size":1,"percent":0.0125,"param":[{"key":"specifiedCates","value":"101,180,108,156,110,192,329,371,372,376,397,435,440,442,459"}]},{"doc_source":"sv_90_hottest_handlist_by_cates","candidate_size":4,"percent":1,"param":[{"key":"specifiedCates","value":"101,108,180,110,156,192,329,371,372,376,397,435,440,442,459"}]},{"doc_source":"hotest_small_video","candidate_size":4,"percent":1,"param":[{"key":"specifiedCates","value":"101,108,180,110,156,192,329,371,372,376,397,435,440,442,459"}]},{"doc_source":"random_by_cates","candidate_size":3,"percent":1,"param":[{"value":"101,108,180,110,156,192,329,371,372,376,397,435,440,442,459","key":"cates"}]}],"candidate_size":2,"percent":0.25},{"doc_source":"explore_by_cates_new","candidate_size":1,"percent":0.125,"param":[{"key":"specifiedCates","value":"109,104,102,105,103,106,111-130,132-155,157-179,181-191,193-328,330-370,373-375,377-396,398-434,436-439,441,443-458,460-500"}]},{"doc_source":"union","recdoc_select":[{"doc_source":"sv_90_hottest_handlist_by_cates","candidate_size":8,"percent":1,"param":[{"value":"101-500","key":"specifiedCates"},{"value":"101,108,180,110,131,156,107,192,329,371,372,376,397,435,440,442,459","key":"excludeCates"}]},{"doc_source":"hotest_small_video","candidate_size":8,"percent":1,"param":[{"value":"101-500","key":"specifiedCates"},{"value":"101,108,180,110,131,156,107,192,329,371,372,376,397,435,440,442,459","key":"excludeCates"}]}],"candidate_size":8,"percent":1}],"candidate_size":8,"percent":1}"""
    name = "'select_baidu_label'"
    sql = "replace into kdd_param values (%s, '%s', 1, now())" % (name, value)
    conn = connectMySQL()
    cursor = conn.cursor()
    ok = cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    insertFunc()
