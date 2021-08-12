from datetime import datetime
import re
from influxdb import InfluxDBClient
from itertools import chain, combinations
from efficient_apriori import apriori

import smtplib
from smtplib import SMTP
from email.mime.text import MIMEText
from email.header import Header

# 初始化数据库连接
client = InfluxDBClient('localhost', 8086, database='home_assistant_exp')


# 生成带状态属性的设备list
def genDevList():
    devList = list()
    res = client.query('show measurements;')
    meaGen = res.get_points()

    for measurement in meaGen:
        measurementDict = dict(measurement)
        devName = measurementDict.get("name")
        if(re.search("switch.", devName) != None or  # 二元开关
           re.search("climate.", devName) != None or  # 空调或地暖
           re.search("fan.", devName) != None or  # 风扇
           re.search("light.", devName) != None or  # 灯具
           re.search("media_player.", devName) != None):  # 电视
            devList.append(measurementDict.get("name"))
    return devList


# 设备打开操作的时间戳list
def singleTurnOnList(dev_name):
    res = client.query(
        'SELECT "state" FROM "'+dev_name+'" WHERE time < 1621213200s')  # 时间到 1621213200 为止

    devGen = res.get_points()

    devOn = list()

    for point in devGen:
        pointDict = dict(point)
        if(pointDict.get("state")):
            if(pointDict.get("state") != 'off'):
                devOn.append(datetime.strptime(
                    pointDict.get("time"), '%Y-%m-%dT%H:%M:%S.%fZ'))

    for i in range(len(devOn)-1, 0, -1):
        if((devOn[i]-devOn[i-1]).total_seconds() < 1800):  # 移去打开时的数据点
            devOn.remove(devOn[i])
    return devOn


# 设备打开操作的list的数量
def singleTurnOnNum(dev_name):
    return len(singleTurnOnList(dev_name))


# 生成非吃灰设备的dict list
def genUsefulDevList(devList, minTimes):
    usefulDevList = list()
    for device in devList:
        turnOnNum = singleTurnOnNum(device)
        if(turnOnNum >= minTimes):
            singleTurnOnDict = dict()
            singleTurnOnDict['name'] = device
            singleTurnOnDict['times'] = turnOnNum
            usefulDevList.append(singleTurnOnDict)
    return usefulDevList


def pushTask(rules):
    sendStr = '自动化推荐:\n'
    for rule in rules:
        sendStr += str(rule)+'\n'

    msg = MIMEText(sendStr, 'plain', 'utf-8')
    msg['From'] = "smarthome<xxx@xxx.com>"
    msg['To'] = 'master<xxx@xxx.xxx>'
    msg['Subject'] = Header('智能家居平台数据分析', 'utf-8')

    sender = 'xxx@xxx.com'
    user = 'xxx@xxx.com'
    password = ''
    smtpserver = 'smtp.xxx.com'

    receiver = ['mail-address1', 'mail-address2']

    smtp = smtplib.SMTP()
    smtp.connect(smtpserver, 25)
    smtp.login(user, password)
    smtp.sendmail(sender, receiver, msg.as_string())

    smtp.quit()

    print('Email sent!')
    print(sendStr)


usefulList = genUsefulDevList(genDevList(), 100)
for devDict in usefulList:
    timeFrames = list()
    for time in singleTurnOnList(devDict.get('name')):
        timeFrame = time.timestamp()/270  # 时间分段
        timeFrames.append(int(timeFrame))
    devDict['timeFrames'] = timeFrames  # 每次打开操作的时间戳


# 得到时间段范围
min = 9999999999
max = -1
for devDict in usefulList:
    for frame in devDict['timeFrames']:
        if(min > frame):
            min = frame
        if(max < frame):
            max = frame

TDBDictList = list()  # 包括时间段和设备两个键的dict list

for i in range(min, max, 1):
    for devDict in usefulList:
        for frame in devDict['timeFrames']:
            if(i == frame):
                wflag = 0
                for tdb in TDBDictList:
                    if(tdb.get('frame') == frame):
                        for device in tdb['devices']:  # 避免重复
                            if (device == devDict.get('name')):
                                wflag = 1
                        if(wflag == 0):
                            tdb['devices'].append(devDict.get('name'))
                            wflag = 1
                if(wflag == 0):
                    TDBDict = dict()
                    TDBDict['frame'] = frame
                    devList = list()
                    devList.append(devDict.get('name'))
                    TDBDict['devices'] = devList
                    TDBDictList.append(TDBDict)

TDBTupleList = list()
for line in TDBDictList:
    if(len(line.get('devices')) > 1):  # 过滤单个设备的item
        TDBTupleList.append(tuple(line.get('devices')))

TDBDictList.clear()

itemsets, rules = apriori(TDBTupleList, min_support=0.1,
                          min_confidence=0.7)  # apriori
pushTask(rules)
client.close()
