import json
class Match:
    def __init__(self, strategyName, parsedJson, response, fromDict):
        self.strategyName = strategyName
        self.parsedJson = parsedJson
        self.response = response
        self.fromDict = fromDict
        print("init")

    def recursionReduceJson(self, doc_source, cate, tempParsedJson):
        if tempParsedJson["is_leaf"] == 0:
            print("note1")
            for i in range(len(tempParsedJson["recdoc_select"])):
                print("note2")
                code = self.recursionReduceJson(doc_source, cate, tempParsedJson["recdoc_select"][i])
                if code == 1:
                    print("note3")
                    tempParsedJson["candidate_size"] = tempParsedJson["candidate_size"] - 1
                    tempParsedJson["select_size"] = tempParsedJson["select_size"] - 1
                    return 1
            print("note4")
            return 0
        elif tempParsedJson["is_leaf"] == 1:
            print("note5")
            if doc_source == tempParsedJson["doc_source"] and cate in tempParsedJson["cates"]:
                print("note6")
                tempParsedJson["candidate_size"] = tempParsedJson["candidate_size"] - 1
                tempParsedJson["select_size"] = tempParsedJson["select_size"] - 1
                return 1
            else:
                print("note6")
                return 0

    def match(self):
        tempParsedJson = self.parsedJson.copy()
        for doc in self.response:
            if doc["strategyName"] != self.strategyName:
                print("StrategyName doesn't match: [%s] required, but [%s] got!!!" % (
                self.strategyName, doc["strategyName"]))
                return {}
            cate = doc["cate"]
            doc_source = self.fromDict[doc["from"] - 1000]
            print(cate, doc_source)
            if isinstance(doc_source, str):
                self.recursionReduceJson(doc_source, cate, tempParsedJson)
            elif isinstance(doc_source, list):
                print("In List")
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
        return self.recursonCheck(resultJson)


if __name__ == '__main__':
    strategyName = 'insv_beauty_plan_v5'
    jsonData = '{"doc_source":"union","is_leaf":0,"recdoc_select":[{"doc_source":"union","is_leaf":0,"recdoc_select":[{"doc_source":"sv_90_hottest_handlist_by_cates","is_leaf":1,"candidate_size":2,"cates":[107],"select_size":2},{"doc_source":"hotest_small_video","is_leaf":1,"candidate_size":2,"cates":[107],"select_size":2}],"candidate_size":2,"select_size":2.0},{"doc_source":"union","is_leaf":0,"recdoc_select":[{"doc_source":"in_sv_duration_hottest","is_leaf":1,"candidate_size":4,"cates":[108],"select_size":4},{"doc_source":"in_sv_hottest_handlist_by_cates","is_leaf":1,"candidate_size":4,"cates":[108],"select_size":4},{"doc_source":"in_sv_hotest_small_video","is_leaf":1,"candidate_size":4,"cates":[108],"select_size":4},{"doc_source":"in_sv_new_hottest_handlist_by_cates","is_leaf":1,"candidate_size":8,"cates":[108],"select_size":4},{"doc_source":"in_sv_explore_by_cates","is_leaf":1,"candidate_size":8,"cates":[108],"select_size":4},{"doc_source":"sv_hottest_handlist_by_cates","is_leaf":1,"candidate_size":4,"cates":[192,101,329,459,108,397,110,440,435,372,376,180,442,156,371],"select_size":4},{"doc_source":"hotest_small_video","is_leaf":1,"candidate_size":4,"cates":[192,101,329,459,108,397,110,440,435,372,376,180,442,156,371],"select_size":4}],"candidate_size":4,"select_size":4.0},{"doc_source":"union","is_leaf":0,"recdoc_select":[{"doc_source":"sv_duration_hottest","is_leaf":1,"candidate_size":8,"cates":[102,103,104,105,106,109,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,181,182,183,184,185,186,187,188,189,190,191,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,373,374,375,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,436,437,438,439,441,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500],"select_size":8},{"doc_source":"hotest_small_video","is_leaf":1,"candidate_size":8,"cates":[102,103,104,105,106,109,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,181,182,183,184,185,186,187,188,189,190,191,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,373,374,375,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,436,437,438,439,441,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500],"select_size":8}],"candidate_size":8,"select_size":8}],"candidate_size":8,"select_size":8}'
    response = [{'from': 1111, 'cate': 107, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1202, 'cate': 108, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1202, 'cate': 301, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1111, 'cate': 107, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1202, 'cate': 301, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1202, 'cate': 108, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1202, 'cate': 108, 'strategyName': 'insv_beauty_plan_v5'},
                {'from': 1202, 'cate': 108, 'strategyName': 'insv_beauty_plan_v5'}]
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
                29: ['in_sv_random_by_set_cate_or_one_cate', 'interesting', 'random_by_cates', 'random_by_set_cate_or_one_cate', 'sv_interesting_1st'],
                31: 'qukan_item_cf',
                32: 'recently_read_cate',
                33: 'recently_read_cates',
                34: 'explore',
                35: ['explore_by_cates', 'explore_by_set_cate_or_one_cate', 'in_sv_explore_by_cates', 'interesting', 'sv_interesting_1st'],
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
                100: ['in_sv_hottest_handlist_by_cates', 'interesting', 'sv_hottest_handlist_by_cates', 'sv_interesting_1st'],
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
    mt = Match(strategyName, json.loads(jsonData), response, fromDict)
    print( mt.checkResult() )

