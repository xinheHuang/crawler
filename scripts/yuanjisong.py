# -*-: encoding: utf-8 -*-

from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import re
import requests
import os
import sys
#sys.path.append('../common/')
from common import *
import urllib
#import matplotlib.pyplot as plt

browser = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true'])
#chromedriver = "chromedriver.exe"
#chrome_options = webdriver.ChromeOptions()
#chrome_options.add_argument("--incognito")  # 隐藏模式登陆
#browser = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)

def getlist_supply(url):
    browser.get(url)
    pageSource = browser.page_source
    soup = BeautifulSoup(pageSource, "html.parser")
    result = soup.select("div.weui_panel > a:nth-of-type(1)")
    for item in result:
        name = ''
        company = ''
        job = ''
        year = ''
        skill = ''
        experience = ''
        work_wage = ''
        work_time = ''
        work_location = ''


        sub_url = "http://www.yuanjisong.com"+item.get('href')
        html2 = urllib.request.urlopen(sub_url)
        soup2 = BeautifulSoup(html2, "html.parser")
        i = 0
        result2 = soup2.select("div.weui_panel_hd > div")
        for item2 in result2:
            content = remove_space(item2.get_text())
            if(i==0):
                company = content
            elif(i==1):
                job = content
            elif(i==2):
                year = content
            i = i + 1

        result2 = soup2.select("div.weui_media_bd > h4")
        for item2 in result2:
            name = remove_space(item2.get_text())

        i = 0
        result2 = soup2.select("div.weui_panel_bd > div")
        for item2 in result2:
            content = remove_space(item2.get_text())
            if (i == 0):
                skill = content
            elif (i == 1):
                experience = content
            elif (i == 2):
                work_wage = content
            elif (i == 3):
                pass
            elif (i == 4):
                work_location = content
            i = i + 1

        result2 = soup2.select("div.weui_panel_bd > p.p_list")
        for item2 in result2:
            work_time = remove_space(item2.get_text())

        year = year.replace("年", "")

        work_wage = work_wage.replace("兼职日薪：", "")
        work_wage = work_wage.replace("元/8小时", "")

        [conn,cur] = set_mysql("IFC")

        #判断截止点，不能用name，因为name里面有些奇怪字符
        num = cur.execute("SELECT * FROM yuanjisong_supply WHERE company = %s AND job = %s AND year = %s AND work_wage = %s AND work_time = %s AND work_location = %s",
                          (company,job,year,work_wage,work_time,work_location))
        if(num>0):
            print("抓取到重复条目，本次抓取结束")
            return 1

        cur.execute(
            "INSERT INTO `yuanjisong_supply`(`name`, `company`, `job`, `year`, `skill`, `experience`, `work_wage`, `work_time`, `work_location`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (name,company,job,int(year),skill,experience,int(work_wage),work_time,work_location))
        cur.connection.commit()
        print(name+" "+company+ " "+job+" "+str(year)+" ")
        close_mysql(conn, cur)
    return 0



def getlist_demand(url):
    browser.get(url)
    pageSource = browser.page_source
    soup = BeautifulSoup(pageSource, "html.parser")
    result = soup.select("div.weui_panel > a:nth-of-type(1)")
    for item in result:
        name = ''
        job = ''
        wage = ''
        length = ''
        total_wage = ''
        desp = ''
        apply_num = 0
        location = ''

        sub_url = "http://www.yuanjisong.com"+item.get('href')
        html2 = urllib.request.urlopen(sub_url)
        soup2 = BeautifulSoup(html2, "html.parser")

        result2 = soup2.select("div.weui_panel_hd > div.topic_title")
        for item2 in result2:
            job = remove_space(item2.get_text())

        i = 0
        result2 = soup2.select("div.weui_panel_bd")
        for item2 in result2:
            content = remove_space(item2.get_text())
            if(i==0):
                wage = content
            elif(i==1):
                length = content
            elif(i==2):
                total_wage = content
            elif(i==3):
                desp = content
            elif(i==4):
                name = content
            elif(i==5):
                location = content
            i = i + 1

        result2 = soup2.select("div.button_sp_area > span.zhushi_span > i")
        for item2 in result2:
            apply_num = remove_space(item2.get_text())

        #清洗数据
        total_wage = total_wage.replace("预估总价：", "")
        total_wage = total_wage.replace("元", "")

        wage = wage.replace("日薪：", "")
        wage = wage.replace("元/8小时", "")

        length = length.replace("预估工时：", "")
        length = length.replace("天", "")

        [conn, cur] = set_mysql("IFC")

        # 判断截止点
        num = cur.execute("SELECT * FROM yuanjisong_demand WHERE job = %s AND wage = %s AND length = %s AND total_wage = %s AND location = %s",
            (job,wage,length,total_wage,location))
        if (num > 0):
            print("抓取到重复条目，本次抓取结束")
            return 1

        cur.execute(
            "INSERT INTO `yuanjisong_demand`(`name`, `job`, `wage`, `length`, `total_wage`, `desp`, `apply_num`,`location`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (name,job,wage,length,total_wage,desp,apply_num,location))
        cur.connection.commit()
        print(name+" "+job+ " "+str(wage)+" "+str(length)+" "+str(total_wage)+" "+str(apply_num)+" "+location)
        close_mysql(conn, cur)
    return 0

def get_supply():
    for i in range(1, 311+1):
        print("page: "+str(i))
        url = "http://www.yuanjisong.com/webpage/consultant/listpage/city_id/0/district_id/0/role_id/0/sort_id/1/major_id/0/pageid/" + str(i)
        if(getlist_supply(url)):
            break
        time.sleep(1)

def get_demand():
    for i in range(1, 39+1):
        print("page: "+str(i))
        url = "http://www.yuanjisong.com/webpage/job/listpage/city_id/0/district_id/0/role_id/0/sort_id/1/pageid/" + str(i)
        if(getlist_demand(url)):
            break
        time.sleep(1)
'''
def analyze():
    x = []
    [conn,cur] = set_mysql("IFC")
    cur.execute("SELECT total_wage FROM yuanjisong_demand WHERE 1")
    results = cur.fetchall()
    for r in results:
        data = r[0]
        x.append(int(data))
    print(x)
    plt.hist(x, 20, normed=1, histtype='bar', facecolor='yellowgreen',cumulative=False, alpha=0.75)
    plt.title('Tecent CDN(Yuan)')

    plt.show()
    #plt.xlabel('价格（元）'.decode('utf-8'))
    #plt.ylabel('种草数/败家数'.decode('utf-8'))
    #plt.legend()


    close_mysql(conn, cur)

def wash():
    [conn,cur] = set_mysql("IFC")
    cur.execute("SELECT year,work_wage,ID FROM yuanjisong_supply WHERE 1")
    results = cur.fetchall()
    for r in results:
        year = r[0]
        year = year.replace("年", "")

        work_wage = r[1]
        work_wage = work_wage.replace("兼职日薪：", "")
        work_wage = work_wage.replace("元/8小时", "")

        ID = r[2]

        cur.execute("UPDATE `yuanjisong_supply` SET `year`=%s,`work_wage`=%s WHERE ID = %s", (year,work_wage,ID))
        cur.connection.commit()
        print(str(ID) + " " + str(work_wage) + " " + str(year))
'''
if __name__ == '__main__':
    get_supply()
    get_demand()
    #analyze()
    #wash()
