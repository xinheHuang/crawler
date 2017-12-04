# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import time
import datetime
import sys
sys.path.append('../common/')
from common import *

def get_panda(url,category,scrap_riqi):
    standard_print("开始抓取",[url,category,scrap_riqi])
    browser = get_phantomJS("")
    error_num = 0
    while error_num < 10:
        try:
            browser.get(url)
            break
        except:
            error_num += 1
            time.sleep(5)
    while 1:
        pageSource = browser.page_source
        soup = BeautifulSoup(pageSource, "html.parser")
        result = soup.select("div.list-container > ul.video-list > li.video-list-item")
        for item in result:
            room_id = item.get('data-id')

            sub_result = item.select("div.video-info > span.video-title")
            video_title = remove_space(sub_result[0].get_text())

            author = ""
            try:
                sub_result = item.select("div.video-info > span.video-nickname")
                author = remove_space(sub_result[0].get_text())
            except:
                pass

            author_level = 0
            try:
                sub_result = item.select("div.video-info > span.video-nickname > i")
                author_level = remove_space(sub_result[0].get("data-level"))
            except:
                pass

            sub_result = item.select("div.video-info > span.video-number")
            video_num = remove_space(sub_result[0].get_text())
            video_num = wash_num(video_num)

            sub_result = item.select("div.video-info > span.video-station-info")
            video_station_num = remove_space(sub_result[0].get_text().replace("人",""))

            label_list = []
            sub_result = item.select("div.video-label-content > a.video-label-item")
            for sub_item in sub_result:
                label = remove_space(sub_item.get_text())
                if '\\u' in label:
                    label = label.encode('utf-8').decode('unicode_escape')
                if (label[0] == "u"):
                    label = label[1:]
                label_list.append(label)
            label_list = wash_list(label_list)

            fans = 0
            try:
                timestamp = str(int(time.time() * 1000))
                url = "https://www.panda.tv/room_followinfo?token=&roomid=%s&_=%s" % (room_id, timestamp)
                json_reponse = get_json_api(url)
                fans = json_reponse['data']['fans']
            except:
                pass

            [conn,cur] = set_mysql('IFC')
            cur.execute("INSERT INTO `live_panda`(`room_id`, `video_title`, `author`, `author_level`, `video_num`, `video_station_num`, `label_list`, `fans`, `category`, `scrap_riqi`) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(room_id,video_title,author,author_level,video_num,video_station_num,label_list,fans,category,scrap_riqi))
            cur.connection.commit()
            close_mysql(conn, cur)
            standard_print("数据录入",[scrap_riqi, room_id, video_title, author, author_level, video_num, video_station_num, label_list, fans, category])


        #点击下一页
        if(click_next_page(browser,"//a[@class='j-page-next']",1) == 0):
            break
    browser.close()

#主函数
scrap_riqi = datetime.datetime.now()
scrap_riqi = scrap_riqi.strftime('%Y-%m-%d %H:%M:%S')
get_panda("https://www.panda.tv/cate/jingji","热门竞技",scrap_riqi)
get_panda("https://www.panda.tv/cate/yllm","娱乐联盟",scrap_riqi)
get_panda("https://www.panda.tv/cate/shouyou","手游专区",scrap_riqi)
get_panda("https://www.panda.tv/cate/zjdj","主机单机",scrap_riqi)
get_panda("https://www.panda.tv/cate/wangyou","网游专区",scrap_riqi)
get_panda("https://www.panda.tv/cate/recorded","大杂烩",scrap_riqi)
