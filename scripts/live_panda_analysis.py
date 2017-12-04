# -*- coding: utf-8 -*-

import os
import os.path
import shutil
from bs4 import BeautifulSoup
from threading import Timer
import datetime
import traceback

import pymysql
import datetime
import random
import re
import time
import socket
import random
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号

socket.setdefaulttimeout(60)
import datetime
import sys
sys.path.append('../common/')
from common import *

def generate_riqi_list():
    riqi_list = []
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT scrap_riqi FROM live_panda WHERE 1 GROUP BY scrap_riqi ORDER BY scrap_riqi DESC")
    results = cur.fetchall()
    for r in results:
        riqi_list.append(r[0])
    close_mysql(conn, cur)
    return riqi_list

def get_game_category(game):
    [conn, cur] = set_mysql('IFC')
    game_search = "%"+game+"%"

    num = cur.execute("SELECT * FROM live_panda WHERE label_list LIKE %s AND category = %s", (game_search, "主机单机"))
    if (num > 0):
        close_mysql(conn, cur)
        return "主机单机"
    num = cur.execute("SELECT * FROM live_panda WHERE label_list LIKE %s AND category = %s", (game_search, "手游专区"))
    if (num > 0):
        close_mysql(conn, cur)
        return "手游专区"
    num = cur.execute("SELECT * FROM live_panda WHERE label_list LIKE %s AND category = %s", (game_search, "网游专区"))
    if (num > 0):
        close_mysql(conn, cur)
        return "网游专区"
    num = cur.execute("SELECT * FROM live_panda WHERE label_list LIKE %s AND category = %s", (game_search, "热门竞技"))
    if (num > 0):
        close_mysql(conn, cur)
        return "热门竞技"
    num = cur.execute("SELECT * FROM live_panda WHERE label_list LIKE %s AND category = %s", (game_search, "娱乐联盟"))
    if (num > 0):
        close_mysql(conn, cur)
        return "娱乐联盟"
    return "大杂烩"



def analysis_game():
    [conn, cur] = set_mysql('IFC')
    riqi_list = generate_riqi_list()
    standard_print("日期序列", riqi_list)

    not_game_array = ["主机游戏","怀旧经典","格斗游戏","新游中心","二次元手游","综合手游","体育竞技","网络游戏","桌游","棋牌游戏"]

    for riqi in riqi_list[1:]:#跳过现在可能正在抓取的时间
        #判断终止点
        num = cur.execute("SELECT * FROM live_panda_analysis_game WHERE scrap_riqi=%s",(riqi))
        if(num > 0):
            standard_print("该日期已经分析过，停止程序",[riqi])
            break

        game_list = []
        cur.execute("SELECT label_list FROM live_panda WHERE (category = '热门竞技' OR category = '手游专区' OR category = '主机单机' OR category = '网游专区') AND scrap_riqi =%s",(riqi))
        results = cur.fetchall()
        for r in results:
            label_list = r[0]
            game = label_list.split(",")[0]
            if '\\u' in game:
                game = game.encode('utf-8').decode('unicode_escape')
            if(game[0] == "u"):
                game = game[1:]
            if((game not in not_game_array) and (game not in game_list)):
                game_list.append(game)
        standard_print("游戏列表",[riqi,game_list])

        for game in game_list:
            game_search = "%"+game+"%"
            cur.execute("SELECT sum(video_num), sum(video_station_num), sum(fans),count(*) FROM `live_panda` WHERE label_list LIKE %s AND scrap_riqi=%s GROUP BY scrap_riqi",(game_search,riqi))
            results = cur.fetchall()
            for r in results:
                sum_video_num = r[0]
                sum_video_station_num = r[1]
                sum_fans = r[2]
                sum_zhubo = r[3]
                category = get_game_category(game)
                num = cur.execute("SELECT * FROM live_panda_analysis_game WHERE game = %s AND scrap_riqi =%s",(game,riqi))
                if(num > 0):
                    cur.execute("UPDATE `live_panda_analysis_game` SET category=%s,sum_video_num=%s,sum_video_station_num=%s,sum_fans=%s,summ_zhubo=%s "
                                "WHERE game = %s AND scrap_riqi=%s",(category,sum_video_num,sum_video_station_num,sum_fans,sum_zhubo,game,riqi))
                    cur.connection.commit()
                    standard_print("游戏数据更新",[riqi, game, category, sum_video_num, sum_video_station_num, sum_fans, sum_zhubo])
                else:
                    cur.execute("INSERT INTO `live_panda_analysis_game`(`scrap_riqi`, `game`,`category`, `sum_video_num`, `sum_video_station_num`, `sum_fans`, `sum_zhubo`) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s)",(riqi,game,category,sum_video_num,sum_video_station_num,sum_fans,sum_zhubo))
                    cur.connection.commit()
                    standard_print("游戏数据录入",[riqi,game,category,sum_video_num,sum_video_station_num,sum_fans,sum_zhubo])

    close_mysql(conn, cur)




'''
str = '\\u7089\\u77f3\\u4f20\\u8bf4'
if '\\u' in str:
    str=str.encode('utf-8').decode('unicode_escape')
print(str)
'''

analysis_game()