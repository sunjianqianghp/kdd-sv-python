# -*- coding: utf-8 -*-
import time
import hashlib
import random
import pandas as pd
from xml.sax.handler import ContentHandler
from xml.sax import parse
import pymysql
import json
import requests
import time
import sys


class FindStrategy:
    def __init__(self, data, url,
                 inner_strategy_map=pd.DataFrame({"if_beauty_cate": [], "group": [], "strategy": []})):
        self.data = data
        self.userGroup = hashlib.md5(data['device_code'].encode("UTF-8")).hexdigest()[6]
        self.url = url
        self.if_inner_flow = True if self.url.find("in") >= 0 else False
        self.strategyName = ''
        self.inner_strategy_map = inner_strategy_map

    def findStrategy(self):
        if not self.if_inner_flow:  ## 外流
            timeSub = time.time() - self.data['register_timestamp']
            if timeSub < 3600 * 48:
                self.strategyName = "exp_test_new" if self.userGroup in ['8', '9'] else "exp_0_new"
            if self.data["client_version"] >= 13600:
                if timeSub < 3600 * 48:
                    if self.userGroup in ['1', '9']:
                        self.strategyName = "exp_0_new"
                    elif self.userGroup in ['3', 'b']:
                        self.strategyName = "exp_sv_136_new"
                    else:
                        self.strategyName = "exp_sv_style"
                else:
                    self.strategyName = "exp_sv_style"

            if self.data["client_version"] < 13600:
                now = time.localtime(time.time()).tm_hour
                if now >= 23 or now < 6:
                    if (self.data['register_timestamp'] == 0 or timeSub < 3600 * 72) and len(
                            self.data["device_code"] > 0):
                        if self.data['register_timestamp'] == 0 or timeSub < 3600 * 48:
                            self.strategyName = "sv_0_deep_night_plan_new"
                        else:
                            self.strategyName = "sv_deep_night_plan_new"
                    else:
                        self.strategyName = "sv_deep_night_plan"
        else:  ## 内流
            if_beauty_cate = True if self.data["cate"] in [101, 110, 180, 108, 156, 192, 329, 371, 372, 376, 397, 435,
                                                           440, 442, 459, 107] else False
            self.strategyName = self.inner_strategy_map.loc[list((self.inner_strategy_map.group == self.userGroup) & (
                        self.inner_strategy_map.if_beauty_cate == if_beauty_cate)), "strategyName"].values[0]
            if self.data["cate"] == 107:
                self.strategyName = "insv_beauty_plan_v1"
        return self.strategyName


class RecommendParse(ContentHandler):
    in_headline = False

    def __init__(self, if_inner_flow, strategy_name):
        ContentHandler.__init__(self)
        self.if_inner_flow = if_inner_flow
        self.strategy_name = strategy_name

        self.if_rec_small_video_flow = False
        self.if_rec_in_small_video_flow = False

        self.if_recdoc_select = False
        self.kdd_param_name = ''

    def startElement(self, name, attrs):
        if not self.if_inner_flow:
            if name == "rec_small_video_flow":
                if attrs["name"] == self.strategy_name:
                    self.if_rec_small_video_flow = True
        else:
            if name == "rec_in_small_video_flow":
                if attrs["name"] == self.strategy_name:
                    self.if_rec_in_small_video_flow = True
        if name == "recdoc_select":
            self.if_recdoc_select = True
        if name == "param" and self.if_recdoc_select and (
                self.if_rec_small_video_flow or self.if_rec_in_small_video_flow):
            self.kdd_param_name = attrs["value"]

    def endElement(self, name):
        if name == "rec_small_video_flow":
            self.if_rec_small_video_flow = False
        if name == "rec_in_small_video_flow":
            self.if_rec_in_small_video_flow = False
        if name == "recdoc_select":
            self.if_recdoc_select = False


class Final:
    def __init__(self, kdd_param_name):
        self.sqlRequest = "select value from kdd_param where name = '%s'" % (kdd_param_name)
        self.strategyJson = ''
        self._strategyDict = {}

    def connectMySQL(self):
        host = "rm-2zem5lmi9vj0vm9e5.mysql.rds.aliyuncs.com"
        port = 3306
        user = "kdd_rec_w"
        passwd = "7cwK0EZxNi0wyrEb"
        db = "kdd_rec"
        conn = pymysql.connect(host=host,
                               port=port,
                               user=user,
                               passwd=passwd,
                               db=db)
        return conn

    def getStrategyJson(self):
        conn = self.connectMySQL()
        cursor = conn.cursor()
        cursor.execute(self.sqlRequest)
        self.strategyJson = cursor.fetchall()[0][0]

    def getVideos(self):
        self.getStrategyJson()
        self._strategyDict = json.loads(self.strategyJson)

    @property
    def strategyDict(self):
        return self._strategyDict


class ParseJson:
    def __init__(self, strategyJson, response, incate=0):
        self.strategyJson = strategyJson
        self._strategyJson_reparsed = {}
        self.response = response
        self.incate = incate

    def handleStringCates(self, strCates):
        result = []
        strCatesSplit = strCates.split(",")
        for x in strCatesSplit:
            if x.find("-") >= 0:
                [start, end] = [int(e) for e in x.split("-")]
                result.extend(range(start, end + 1))
            else:
                result.extend([int(x)])
        return result

    def getSpecifiedCates(self, param):
        specified_cate_set = set()
        excluded_cate_set = set()
        for ele in param:
            if ele["key"] != "excludeCates":
                if ele["value"] == "99999":
                    specified_cate_set.add(int(self.incate))
                else:
                    specified_cate_set = specified_cate_set.union(set(self.handleStringCates(ele["value"])))
            if ele["key"] == "excludeCates":
                excluded_cate_set = excluded_cate_set.union(set(self.handleStringCates(ele["value"])))
        result_set = specified_cate_set - excluded_cate_set
        return list(result_set)

    def reparseJson(self, select_size_outer, sub_json):
        tmpNode = {}
        tmpNode["doc_source"] = sub_json["doc_source"]
        tmpNode["candidate_size"] = sub_json["candidate_size"]
        tmpNode["select_size"] = sub_json["percent"] * select_size_outer
        if "recdoc_select" in sub_json.keys():
            tmpNode["is_leaf"] = 0
            tmpNode["recdoc_select"] = [self.reparseJson(tmpNode["candidate_size"], sub_sub_json) for sub_sub_json in
                                        sub_json["recdoc_select"]]
        else:
            tmpNode["is_leaf"] = 1
            tmpNode["cates"] = self.getSpecifiedCates(sub_json["param"])
        return tmpNode

    def getReparsedJson(self):
        self._strategyJson_reparsed = self.reparseJson(8, self.strategyJson)

    @property
    def strategyJson_reparsed(self):
        return self._strategyJson_reparsed


class Request:
    def __init__(self, data, url):
        self.data = data
        self.url = url
        self._response = []

    def request(self):
        response = requests.post(self.url, self.data)
        if response.status_code == 200:
            # print("SUCCESS")
            result = []
            res = json.loads(response.text)
            props = res['props'][0:-3]
            props = json.loads(props)
            docs = props.keys()
            for doc in docs:
                tmpResult = dict()
                tmp = props[doc]
                tmpResult["from"] = tmp["from"]
                from_desc = tmp["from_desc"].split("_\n_")
                tmpResult["cate"] = int(from_desc[1].split(":")[1])
                tmpResult["strategyName"] = str(from_desc[2].split(":")[1])
                result.append(tmpResult)
            self._response = result

    @property
    def response(self):
        return self._response


def getDeviceCode():
    # f = open("/home/work/sunjianqiang/data/deviceCode.csv")
    f = open("/home/lechuan/sunjianqiang/data/deviceCode.csv")
    lines = f.readlines()
    device_code = {}
    for line in lines:
        ll = line.split(",")
        if len(ll) > 1:
            device_code[ll[0]] = ll[1][0]
    f.close()
    return device_code


def getData(device_code, timestamp, dtu, page):
    data = {
        "uid": "1974024",
        "device_code": device_code,
        "count": 8,
        "page": page,
        "region": 1,
        "content_type": [1],
        "ab_group": "ab_3",
        "register_timestamp": timestamp,
        "op": 2,
        "dtu": dtu,
        "client_version": 13602
    }
    return data


def getUrl(if_inner_flow):
    if if_inner_flow:
        return "http://172.16.107.84:2051/in_small_video"
    return "http://172.16.107.84:2051/small_video"


def getDtu():
    is_tx = 0
    dtu = random.randint(1, 4000)
    if 1348 <= dtu and dtu <= 1647:  # 1348-1647
        is_tx = 1
    elif 2348 <= dtu and dtu <= 2547:  # 2348-2547
        is_tx = 1
    elif 2598 <= dtu and dtu <= 2667:  # 2598-2667
        is_tx = 1
    elif 2768 <= dtu and dtu <= 2797:  # 2768-2797
        is_tx = 1
    elif 2868 <= dtu and dtu <= 2897:  # 2868-2897
        is_tx = 1
    elif 2993 <= dtu and dtu <= 3032:  # 2993-3032
        is_tx = 1
    elif 3233 <= dtu and dtu <= 3582:  # 3233-3582
        is_tx = 1
    elif 3723 <= dtu and dtu <= 3742:  # 3723-3742
        is_tx = 1
    return dtu, is_tx


def getPage():
    return random.randint(1, 3)


def getTimestamp():
    now_timestamp = int(time.time())
    sub_time = random.randint(1, 100)
    timestamp = now_timestamp - sub_time * 100000
    return timestamp

class Match:
    def __init__(self, strategyName, parsedJson, response, fromDict):
        self.parsedJson0 = parsedJson.copy()
        self.strategyName = strategyName
        self.parsedJson = parsedJson
        self.response = response
        self.fromDict = fromDict

    def recursionReduceJson(self, doc_source, cate, tempParsedJson):
        if tempParsedJson["is_leaf"] == 0:
            for i in range(len(tempParsedJson["recdoc_select"])):
                code = self.recursionReduceJson(doc_source, cate, tempParsedJson["recdoc_select"][i])
                if code == 0:
                    tempParsedJson["candidate_size"] = tempParsedJson["candidate_size"] - 1
                    tempParsedJson["select_size"] = tempParsedJson["select_size"] - 1
                    return 0
            return 1
        elif tempParsedJson["is_leaf"] == 1:
            if doc_source == tempParsedJson["doc_source"] and cate in tempParsedJson["cates"]:
                tempParsedJson["candidate_size"] = tempParsedJson["candidate_size"] - 1
                tempParsedJson["select_size"] = tempParsedJson["select_size"] - 1
                return 0
            else:
                return 1

    def match(self):
        tempParsedJson = self.parsedJson.copy()
        for doc in self.response:
            if doc["strategyName"] != self.strategyName:
                print("StrategyName doesn't match: [%s] required, but [%s] got!!!" % (
                self.strategyName, doc["strategyName"]))
                return {}
            cate = doc["cate"]
            doc_source = self.fromDict[doc["from"] - 1000]
            # print(cate, doc_source)
            if isinstance(doc_source, str):
                self.recursionReduceJson(doc_source, cate, tempParsedJson)
            elif isinstance(doc_source, list):
                for sub_doc_source in doc_source:
                    self.recursionReduceJson(sub_doc_source, cate, tempParsedJson)
        return tempParsedJson

    def recursonCheck(self, ResultJson):
        if ResultJson["is_leaf"] == 0:
            if ResultJson["candidate_size"] >= 0 and ResultJson["select_size"] >= 0:
                for subResultJson in ResultJson["recdoc_select"]:
                   return self.recursonCheck(subResultJson)
            else:
                return 1
        elif ResultJson["is_leaf"] == 1:
            if ResultJson["candidate_size"] >= 0 and ResultJson["select_size"] >= 0:
                return 0
            else:
                return 1

    def checkResult(self):
        resultJson = self.match()
        try:
            status_code = self.recursonCheck(resultJson)
            if status_code == 1:
                print(self.strategyName)
                print( resultJson )
                print(self.response)
            return status_code
        except KeyError:
            print(self.strategyName)
            # print(self.parsedJson)
            print(self.response)
            return




if __name__ == '__main__':
    print("START")
    # data0 = {"uid": "1974024", "device_code": "1035", "count": 8, "page": 2, "region": 1, "content_type": [1],
    #         "register_timestamp": time.time( )-random.uniform(0,1)*3600*240 , # 随机产生最近10天内的时间戳
    #         "op": 2,"dtu": 1500, "client_version":13602 }
    # data1 = data0.copy()
    # data1["content_id"] = 6416830
    # data1["cate"] = 108
    # url = "172.0.0.1:2051/in_small_video"

    inner_strategy_list = [
        ['0', True, "insv_beauty_plan_v5"],
        ['1', True, "insv_beauty_plan_v2"],
        ['2', True, "insv_beauty_plan_v5"],
        ['3', True, "insv_beauty_plan_v2"],
        ['4', True, "insv_beauty_plan_v2"],
        ['5', True, "insv_beauty_plan_v2"],
        ['6', True, "insv_beauty_plan_v4"],
        ['7', True, "insv_beauty_plan_v4"],
        ['8', True, "insv_beauty_plan_v2"],
        ['9', True, "insv_beauty_plan_v2"],
        ['a', True, "insv_beauty_plan_v2"],
        ['b', True, "insv_beauty_plan_v2"],
        ['c', True, "insv_beauty_plan_v2"],
        ['d', True, "insv_beauty_plan_v2"],
        ['e', True, "insv_beauty_plan_v2"],
        ['f', True, "insv_beauty_plan_v2"],
        ['0', False, "insv_no_beauty_plan_v5"],
        ['1', False, "insv_no_beauty_plan_v3"],
        ['2', False, "insv_no_beauty_plan_v5"],
        ['3', False, "insv_no_beauty_plan_v3"],
        ['4', False, "insv_no_beauty_plan_v3"],
        ['5', False, "insv_no_beauty_plan_v3"],
        ['6', False, "insv_no_beauty_plan_v4"],
        ['7', False, "insv_no_beauty_plan_v4"],
        ['8', False, "insv_no_beauty_plan_v3"],
        ['9', False, "insv_no_beauty_plan_v3"],
        ['a', False, "insv_no_beauty_plan_v3"],
        ['b', False, "insv_no_beauty_plan_v3"],
        ['c', False, "insv_no_beauty_plan_v3"],
        ['d', False, "insv_no_beauty_plan_v3"],
        ['e', False, "insv_no_beauty_plan_v3"],
        ['f', False, "insv_no_beauty_plan_v3"]
    ]
    inner_strategy_map = pd.DataFrame(inner_strategy_list, columns=["group", "if_beauty_cate", "strategyName"])

    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    # print(name)
    # print("############################################################")
    # js ={"doc_source":"union","recdoc_select":[{"doc_source":"union","recdoc_select":[{"doc_source":"sv_new_hottest_handlist_by_cates","candidate_size":2,"percent":1,"param":[{"value":"107","key":"specifiedCates"}]},{"doc_source":"hotest_small_video","candidate_size":2,"percent":1,"param":[{"value":"107","key":"specifiedCates"}]}],"candidate_size":2,"percent":0.25},{"doc_source":"union","recdoc_select":[{"doc_source":"in_sv_new_hottest_handlist_by_cates","candidate_size":4,"percent":1,"param":[{"value":"99999","key":"specifiedCates"}]},{"doc_source":"in_sv_hotest_small_video","candidate_size":4,"percent":1,"param":[{"value":"99999","key":"specifiedCates"}]},{"doc_source":"in_sv_new_hottest_handlist_by_cates","candidate_size":8,"percent":1,"param":[{"value":"99999","key":"specifiedCates"}]},{"doc_source":"in_sv_explore_by_cates","candidate_size":8,"percent":1,"param":[{"value":"99999","key":"cates"}]},{"doc_source":"sv_new_hottest_handlist_by_cates","candidate_size":4,"percent":1,"param":[{"value":"101,110,180,108,156,192,329,371,372,376,397,435,440,442,459","key":"specifiedCates"}]},{"doc_source":"hotest_small_video","candidate_size":4,"percent":1,"param":[{"value":"101,110,180,108,156,192,329,371,372,376,397,435,440,442,459","key":"specifiedCates"}]}],"candidate_size":4,"percent":0.5},{"doc_source":"union","recdoc_select":[{"doc_source":"sv_test_hottest","candidate_size":8,"percent":1,"param":[{"key":"specifiedCates","value":"101-500"},{"key":"excludeCates","value":"156,101,107,108,110,131,180,192,329,371,372,376,397,435,440,442,459"}]},{"doc_source":"hotest_small_video","candidate_size":8,"percent":1,"param":[{"key":"specifiedCates","value":"101-500"},{"key":"excludeCates","value":"156,101,107,108,110,131,180,192,329,371,372,376,397,435,440,442,459"}]}],"candidate_size":8,"percent":1}],"candidate_size":8,"percent":1}
    # pj = ParseJson(js, "","1234567")
    # pj.getReparsedJson()
    # print(js)
    # print( pj.strategyJson_reparsed )
    # sjr = pj.strategyJson_reparsed
    fromDict = {2: 'qukan_hotest',
                4: 'qukan_hotest_by_one_cate',
                8: 'hotest',
                10: 'hotest_by_one_cate',
                12: 'hotest_v3',
                13: 'cf_feedback',
                15: ['hotest_by_cates', 'hotest_by_one_cate_v3'],
                17: ['hotest_small_video', 'in_sv_hotest_small_video'],
                18: 'user_prefere',
                20: 'ctr_new',
                21: 'ctr_high',
                22: 'ctr_new_by_some_cates',
                23: 'ctr_high_by_some_cates',
                24: 'pct_by_some_cates',
                26: 'user_prefere_second',
                27: 'set_top',
                29: ['in_sv_random_by_set_cate_or_one_cate', 'interesting', 'random_by_cates',
                     'random_by_set_cate_or_one_cate', 'sv_interesting_1st'],
                31: 'qukan_item_cf',
                32: 'recently_read_cate',
                33: 'recently_read_cates',
                34: 'explore',
                35: ['explore_by_cates', 'explore_by_set_cate_or_one_cate', 'in_sv_explore_by_cates', 'interesting',
                     'sv_interesting_1st'],
                36: 'random_album',
                39: 'random_album_ep1',
                40: 'random_album_ep1_by_one_cate',
                41: 'dtu_video',
                42: ['explore_album', 'explore_small_video'],
                43: 'hotest_album_ep1',
                44: ['keyword_prefer_v2', 'user_keywords_recall'],
                45: 'hand_list_by_set',
                50: 'word2vec_item_cf',
                54: 'recently_read_cates_second',
                60: 'vote_rank_recall',
                61: 'user_topic_prefer_recall',
                71: 'cf_feedback_similar',
                72: 'new_recall_model',
                73: 'ffm_recall',
                86: 'explore_by_word2veccate',
                92: 'small_video_icf',
                93: 'small_video_w2c',
                100: ['in_sv_hottest_handlist_by_cates', 'interesting', 'sv_hottest_handlist_by_cates',
                      'sv_interesting_1st'],
                103: 'docselect_ee',
                104: 'pct_cate_hot',
                105: 'pct_sub_cate_hot',
                107: 'pct_sub_cate_hot',
                109: ['in_sv_new_hottest_handlist_by_cates', 'sv_new_hottest_handlist_by_cates'],
                111: ['in_sv_90_hottest_handlist_by_cates', 'sv_90_hottest_handlist_by_cates'],
                113: 'sv_new_by_cates',
                114: 'explore_by_cates_new',
                116: 'sv_136_new_hottest',
                117: 'in_sv_baidu_label_quene',
                123: 'word2vec_item_cf_similar',
                150: 'user_applist_prefer',
                151: 'user_dtu_media_recall',
                154: 'ctr_hottest_v2',
                155: 'ctr_hottest_v2',
                156: 'pct_hottest_v2',
                157: 'pct_hottest_v2',
                158: 'random_by_cates_v3',
                171: ['interesting', 'sv_hottest_by_cates', 'sv_interesting_1st'],
                201: 'sv_community_hottest',
                202: ['in_sv_duration_hottest', 'sv_duration_hottest'],
                203: 'sv_136_list',
                221: 'sv_by_cluster_label',
                241: 'recently_read_cluster'}
    device_code_list = getDeviceCode()
    bl = True if int(sys.argv[1]) == 1 else False
    url = getUrl(bl)
    for device_code in device_code_list:
        print("**************************************** another turn *********************************************")
        dtu, is_tx = getDtu()
        page = getPage()
        timestamp = int(time.time() - random.uniform(0, 1) * 3600 * 120)  # getTimestamp()
        data = getData(device_code, timestamp, dtu, page)
        data["cate"] = 108
        data["content_id"] = 6416830
        fs = FindStrategy(data, url, inner_strategy_map)
        strategyName = fs.findStrategy()
        rp = RecommendParse(fs.if_inner_flow, strategyName)
        parse("recommend.xml", rp)
        name = rp.kdd_param_name
        fn = Final(name)
        fn.getStrategyJson()
        strategyJson = json.loads(fn.strategyJson)
        pj = ParseJson(strategyJson, "", data["cate"])
        pj.getReparsedJson()
        parsedJson = pj.strategyJson_reparsed   ####
        rq = Request(json.dumps(data), url)
        rq.request()
        response = rq.response                  ####
        mt = Match(strategyName, parsedJson, response, fromDict)
        print(parsedJson)
        print(mt.checkResult())
        print("response: ", [[doc["from"], doc["cate"]] for doc in response])
        print("\n\n")
        time.sleep(3)
