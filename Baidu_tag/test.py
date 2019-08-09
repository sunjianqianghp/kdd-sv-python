# -*- coding: utf-8 -*-
import requests as r
import json
import time
import random
import datetime


def postInSV(device_code, timestamp, cate, url, contentid):
    data = {
        "uid": "1974024",
        "device_code": device_code,
        "count": 8,
        "page": 2,
        "region": 1,
        "content_type": [1],
        "ab_group": "ab_3",
        "register_timestamp": timestamp,
        "op": 2,
        "dtu": 501,
        "content_id": contentid,
        "cate": cate
    }

    data = json.dumps(data)

    response = r.post(url, data=data)

    recalls = []
    cates = []
    strategys = []
    froms = []
    docs2 = []

    if response.status_code == 200:
        res = json.loads(response.text)
        docs = res['docs']
        props = json.loads(res['props'])
        recalls = []
        cates = []
        strategys = []
        for doc in docs:
            tmp = props[str(doc)]
            froms.append(tmp["from"])
            docs2.append(doc)
            from_desc = tmp["from_desc"].split("_\n_")
            recalls.append(from_desc[0].split(":")[1])
            cates.append(from_desc[1].split(":")[1])
            strategys.append(from_desc[2].split(":")[1])

    return docs, recalls, cates, strategys, froms, docs2


def getDeviceCode():
    f = open("test.csv")
    lines = f.readlines()
    device_code = {}
    for line in lines:
        ll = line.split(",")
        if len(ll) > 1:
            device_code[ll[0]] = ll[1][0]
    f.close()

    return device_code  ##


def getCate():
    f = open("cate.csv")
    lines = f.readlines()
    cate_list = []
    for line in lines:
        cate_list.append(int(line))
    f.close()
    return cate_list


def checkInSV(device_code_list, cate_list):
    contentlist = [6813602,
                   6813604,
                   6813606,
                   6813607,
                   6813608,
                   6813610,
                   7735225,
                   10998690,
                   8964921,
                   7477290,
                   8234423,
                   8083232,
                   7759891,
                   8351626,
                   8145745,
                   8438480] # 非6开头的为百度解析的视频
    now_timestamp = int(time.time())
    # device_code = "00004"
    for device_code in device_code_list:
        sub_time = random.randint(1, 6)
        timestamp = now_timestamp - sub_time * 100000
        cate = cate_list[random.randint(0, len(cate_list) - 1)]
        video_id = contentlist[random.randint(0, len(contentlist) - 1)]
        docs, recalls, cates, strategys, froms, docs2 = postInSV(device_code, timestamp, cate,
                                                          "http://127.0.0.1:2051/in_small_video", video_id)
        flag = checkRight2(device_code_list[device_code], str(cate), cates)
        # if flag == 0:
        print(video_id, device_code, device_code_list[device_code], cate, cates, flag)
        print(recalls)
        print(froms)
        time.sleep(1)
    # return device_code,device_code_list[device_code], docs, recalls, cates, strategys


def checkRight2(group, cate, cates):
    beauty = ["101", "110", "180", "108", "156", "192", "329", "371", "372", "376", "397", "435", "440", "442", "459"]
    c107 = 0  # 107类别数
    cm = 0  # 美女类别数
    cf = 0  # 非美女类别是
    cac = 0  # 和输入别相同类别数
    for ca in cates:
        if ca == cate:
            cac = cac + 1
        ff = 1
        if ca == "107":
            c107 = c107 + 1
            ff = 0
        for be in beauty:
            if be == ca:
                cm = cm + 1
                ff = 0
                break
        cf = cf + ff
    # print(c107,cm,cf,cac)
    flag = 0
    group1 = ["0", "1", "2", "3"]
    group2 = ["4", "5", "6", "7"]
    group3 = ["8", "9", "a", "b"]
    group4 = ["c", "e", "f"]
    group5 = ["d"]

    g = 0  # group所属case
    print(g)
    if group in group1:
        g = 1
    elif group in group2:
        g = 2
    elif group in group3:
        g = 3
    elif group in group4:
        g = 4
    elif group in group5:
        g = 5
    else:
        g = 0

    b = 0  # 输入类别是否为美女
    if cate in beauty:
        b = 1

    #  g: group所属case
    #  b: 输入类别是否为美女
    # c107 = 0 # 107类别数
    # cm = 0  # 美女类别数
    # cf = 0  # 非美女类别是
    # cac = 0 # 和输入别相同类别数

    if g == 0 and cac == 8:
        flag = 1

    if g == 1 and b == 1 and c107 == 2 and cm == 4 and cf == 2:
        flag = 1

    if g == 1 and b == 0 and c107 == 1 and cm == 3 and cf == 4:
        flag = 1

    if g == 2 and b == 1 and c107 == 2 and cm == 4 and cf == 2:
        flag = 1
    # 小视频内流非美女进去 107 1个 美女 3个 当前cate非美女 4个
    if g == 2 and b == 0 and c107 == 1 and cm == 3 and cac == 4:
        flag = 1
    # 小视频内流美女进去 107 2个 当前cate美女 4个 非美女 2个
    if g == 3 and b == 1 and c107 == 2 and cac == 4 and cf == 2:
        flag = 1
    # 小视频内流非美女进去 107 1个 美女 3个 当前cate非美女 4个
    if g == 3 and b == 0 and c107 == 1 and cm == 3 and cac == 4:
        flag = 1
    # 小视频内流美女进去 107 2个 当前cate美女 4个 非美女 2个
    if g == 4 and b == 1 and c107 == 2 and cac == 4 and cf == 2:
        flag = 1
    # 小视频内流非美女进去 107 1个 美女 3个 非美女 4个
    if g == 4 and b == 0 and c107 == 1 and cm == 3 and cf == 4:
        flag = 1

    # 小视频内流美女进去 107 2个 当前cate美女 4个 非美女 2个
    if g == 5 and b == 1 and c107 == 2 and cac == 4 and cf == 2:
        flag = 1
    # 小视频内流非美女进去 107 1个 美女 3个 其他 4个
    if g == 5 and b == 0 and c107 == 1 and cm == 3:
        flag = 1
    # if flag == 0:
    print("group:%s,      if_beauty: %s, c107: %s, cm: %s, cf: %s, cac: %s" % (
    str(g), str(b), str(c107), str(cm), str(cf), str(cac)))
    return flag


if __name__ == "__main__":
    device_code_list = getDeviceCode()  ## {device_code: group}
    cate_list = getCate()  ## [cate1, cate2, ... ]
    checkInSV(device_code_list, cate_list)
