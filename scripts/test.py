# -*- coding: utf-8 -*-
#  http://36kr.com/
import requests,re,time,math
import threading,queue
import json,os
from random import choice
import pymysql
from selenium import webdriver
import sys
sys.path.append('../common/')
from common import *
import random
from bs4 import BeautifulSoup
from datetime import datetime as dt
import urllib
import datetime
import pytz
from tzlocal import get_localzone

from datetime import datetime
import time




if __name__ == '__main__':
    try:
        print(datetime_timestamp('2015-01-01 20:20:00', 's'))
        print(timestamp_datetime(1420114800))
        print(timestamp_datetime(1420114800123))
    except Exception as e:
        print(e.args[0])

'''
def store_product_shop(args):
    a = args[0]
    b = args[1]

    mysql = set_mysql("IFC")
    conn = mysql['conn']
    cur = mysql['cur']
    print(str(a) + " " + str(b))
    close_mysql(conn, cur)

def try_repeat(func,args,repeat_num):
    for try_i in range(repeat_num):
        try:
            func(args)
            break
        except:
            time.sleep(10 + random.randint(2, 5))
            pass

try_repeat(store_product_shop,[2,5],5)

t = datetime.datetime.utcnow()
tz = get_localzone()   #获得本地timezone
utc = pytz.utc         #获得UTC timezone
dt = datetime.datetime(2016, 6, 12, 5, 0, 0)
loc_dt = tz.localize(dt) #将DateTime数据贴上timezone
utc_dt = loc_dt.astimezone(utc)   #转换到新的timezone
a = datetime.datetime.strftime(utc_dt, "%Y-%m-%d %H:%M:%S")
print(a)

riqi = datetime.datetime.now()
riqi += datetime.timedelta(days=-7)
print(riqi.strftime('%Y-%m-%d'))

a = "开始看"
title_str = a.split('/')[-1]
print(title_str)

p = re.compile('(\d\.\d)')
flag = p.search("多对4.2阿萨斯")
if (flag is not None):
    print(flag.group(1))
else:
    print('aaa')


def url2Dict(url):
    query = urllib.parse.urlparse(url).query
    return dict([(k, v[0]) for k, v in urllib.parse.parse_qs(query).items()])

url="https://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&rsv_idx=2&tn=baiduhome_pg&wd=300200&rsv_spt=1&oq=python%20nonetype&rsv_pq=ddeee0430002a4d3&rsv_t=52bbRWiHUtsbsalW02NaxFrDqU5UVJ5TYaaeofTn0oNP0TZh2BQe7Fyh5viROnY%2BuYJg&rqlang=cn&rsv_enter=1&rsv_sug3=6&rsv_sug1=1&rsv_sug7=100&rsv_sug2=0&inputT=2497&rsv_sug4=2497"
d = url2Dict(url)
del d['ie']
print(d)
'''

'''
#这里开多线程
t_list = []
for i in range(5):  # 开几个线程
    get_data = threading.Thread(target=scrap_data, args=(cur,))
    get_data.start()
    t_list.append(get_data)
for t in t_list:
    t.join()
'''
'''
#设置selenium浏览器
chromedriver = "chromedriver.exe"
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--incognito")  # 隐藏模式登陆
browser_mail = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)
browser_mail.set_page_load_timeout(10)
mail_url = 'http://www.bccto.me/win/fartvlmh(a)cuirushi-_-org/5pZPvUs2d2FOgDcAYHk5Vw.eml'
get_page(browser_mail, mail_url)
'''