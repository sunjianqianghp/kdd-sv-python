# -*- coding: utf-8 -*-
import requests as r
import json
import time
import random


def postSmallVideo(data, url):
    data = json.dumps(data)

    response = r.post(url, data=data)

    docs = []
    recalls = []
    cates = []
    strategys = []
    froms = []

    if response.status_code == 200:
        res = json.loads(response.text)
        # print(res)
        docs = res['docs']
        print(type(docs))
        print(docs)
        props = res['props'][0:-3]
        props = json.loads(props)
        # print(props)
        for doc in docs:
            tmp = props[str(doc)]
            froms.append(tmp["from"])
            from_desc = tmp["from_desc"].split("_\n_")
            # from_desc = tmp["from_desc"]
            # print(type(from_desc))
            # print(from_desc)
            # print(json.loads(from_desc))
            recalls.append(from_desc[0].split(":")[1].encode('gbk'))
            cates.append(from_desc[1].split(":")[1].encode('gbk'))
            strategys.append(from_desc[2].split(":")[1].encode('gbk'))

    return docs, recalls, cates, strategys, froms

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
        "dtu": dtu
    }
    return data

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

def getDeviceCode():
    f = open("/home/work/sunjianqiang/data/deviceCode.csv")
    lines = f.readlines()
    device_code = {}
    for line in lines:
        ll = line.split(",")
        if len(ll) > 1:
            device_code[ll[0]] = ll[1][0]
    f.close()

    return device_code

def getTimestamp():
    now_timestamp = int(time.time())
    sub_time = random.randint(1, 100)
    timestamp = now_timestamp - sub_time * 100000
    return timestamp

def getUrl(if_inner_flow):
    if if_inner_flow:
        return "http://127.0.0.1:2051/in_small_video"
    return "http://127.0.0.1:2051/small_video"

def check(cates):
    flag = 1
    for c1 in cates:
        cnt = 0
        for c2 in cates:
            if c1 == c2:
                cnt += 1
        if cnt != 1:
            flag = 0
            break
    return flag

def Test(is_single=False):
    device_code_list = getDeviceCode()
    url = getUrl(True)
    for device_code in device_code_list:
        dtu, is_tx = getDtu()
        page = getPage()
        timestamp = getTimestamp()
        data = getData(device_code, timestamp, dtu, page)
        docs, recalls, cates, strategys, froms = postSmallVideo(data, url)
        if check(cates) == 0:
            print(device_code, dtu, is_tx)
            print(docs)
            print(recalls)
            print(cates)
            print(strategys)
            print(froms)

        if is_single == True:
            break
        time.sleep(2)


if __name__ == "__main__":
    Test()