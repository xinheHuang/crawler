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


company_domestic_list = ["中国联航","海南航空","深圳航空","南方航空","西藏航空","中国国航"
    , "昆明航空","四川航空","东方航空","山东航空","厦门航空","天津航空","大新华航空"
    , "长龙航空","首都航空","上海航空","吉祥航空","金鹏航空","华夏航空","河北航空"
    , "青岛航空","成都航空","红土航空","春秋航空","东海航空","九元航空","长安航空"
    , "西部航空","福州航空","祥鹏航空","乌鲁木齐航空","奥凯航空","幸福航空","瑞丽航空"
    , "桂林航空","多彩航空","龙江航空","北部湾航空","江西航空"]

def get_company(riqi,attr):
    [conn, cur] = set_mysql('IFC')
    sql = ""
    if(attr == "scale"):
        sql = "SELECT count(DISTINCT flight_no) AS num, company FROM `otc_xiecheng_flight` WHERE type='国内机票' AND riqi = %s GROUP BY company"
    elif(attr == "scale_foreign"):
        sql = "SELECT count(DISTINCT flight_no) AS num, company FROM `otc_xiecheng_flight` WHERE type='国际机票' AND riqi = %s GROUP BY company"
    elif(attr == "price"):
        sql = "SELECT avg(price) AS avg_price, company FROM `otc_xiecheng_flight` WHERE price>0 AND seat_type = '经济舱' AND type='国内机票' AND riqi = %s GROUP BY company"
    elif(attr == "price_foreign"):
        sql = "SELECT avg(price) AS avg_price, company FROM `otc_xiecheng_flight` WHERE price>0 AND seat_type = '经济舱' AND type='国际机票' AND riqi = %s GROUP BY company"
    elif(attr == "discount"):
        sql = "SELECT avg(discount) AS avg_discount, company FROM `otc_xiecheng_flight` WHERE discount>0 AND seat_type = '经济舱' AND riqi = %s GROUP BY company"
    elif(attr == "punctuality"):
        sql = "SELECT avg(punctuality) AS avg_pun, company FROM `otc_xiecheng_flight` WHERE punctuality>0 AND riqi=%s GROUP BY company"

    if(sql == ""):
        print("数据参数有误")
        return

    cur.execute(sql,(riqi))

    result = cur.fetchall()
    for r in result:
        value = r[0]
        company = r[1]

        if (company in company_domestic_list):
            type = "国内航空公司"
        else:
            type = "国际航空公司"

        num = cur.execute("SELECT * FROM otc_xiecheng_flight_analysis_company WHERE company=%s AND riqi=%s",(company,riqi))
        if(num > 0):
            cur.execute("UPDATE `otc_xiecheng_flight_analysis_company` SET "+attr+" = %s WHERE company = %s AND riqi = %s", (value,company,riqi))
            cur.connection.commit()
            standard_print("数据更新:", [company, value,riqi])
        else:
            cur.execute("INSERT INTO `otc_xiecheng_flight_analysis_company`(`"+attr+"`,`type`,`company`,`riqi`) VALUES (%s,%s,%s,%s)",(value,type,company,riqi))
            cur.connection.commit()
            standard_print("数据录入:", [company, value,riqi])

    close_mysql(conn, cur)

def delete_company(riqi):
    [conn, cur] = set_mysql('IFC')
    num = cur.execute("DELETE FROM otc_xiecheng_flight_analysis_company WHERE type='国内航空公司'AND (scale=0 OR discount=0 OR price=0 OR punctuality=0)")
    cur.connection.commit()
    standard_print("数据删除国内:", [num])
    #num = cur.execute("DELETE FROM otc_xiecheng_flight_analysis_company WHERE type='国际航空公司'AND (scale_foreign=0 OR price_foreign=0)")
    #cur.connection.commit()
    #standard_print("数据删除国际:", [num])
    close_mysql(conn, cur)


def draw_scatter(riqi,attr1,attr2):
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT company,"+attr1+","+attr2+" FROM otc_xiecheng_flight_analysis_company WHERE riqi = %s",(riqi))
    result = cur.fetchall()
    x = []
    y = []
    l = []
    for r in result:
        company = r[0]
        value1 = r[1]
        value2 = r[2]
        x.append(value1)
        y.append(value2)
        l.append(company)

    close_mysql(conn, cur)

    plt.scatter(x, y, marker="o")
    for i, label in enumerate(l):
        plt.annotate(label, (x[i], y[i]))

    #plt.xlabel('折扣率(折)')
    #plt.ylabel('航班数(个)')
    plt.title('散点图')
    plt.legend()
    plt.show()


def get_price_diff(riqi):
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT city_from,city_to FROM `otc_xiecheng_flight` WHERE type='国际机票' AND riqi=%s GROUP BY city_from, city_to",(riqi))
    result = cur.fetchall()
    for r in result:
        city_from = r[0]
        city_to = r[1]
        price_list = []
        cur.execute("SELECT company,price FROM `otc_xiecheng_flight` WHERE type='国际机票' AND city_from=%s AND city_to=%s AND riqi=%s",(city_from,city_to,riqi))
        result2 = cur.fetchall()
        for r2 in result2:
            price = r2[1]
            price_list.append(price)
        avg_price = sum(price_list) / float(len(price_list))
        standard_print("平均价格:",[avg_price])

        cur.execute("SELECT company,price FROM `otc_xiecheng_flight` WHERE type='国际机票' AND city_from=%s AND city_to=%s AND riqi=%s",(city_from, city_to, riqi))
        result2 = cur.fetchall()
        for r2 in result2:
            company = r2[0]
            price = r2[1]
            price_diff = (price - avg_price)/avg_price
            cur.execute("INSERT INTO `otc_xiecheng_flight_analysis_price_diff`(`city_from`, `city_to`,`company`,`price`,`avg_price`,`price_diff`,`riqi`) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s)",(city_from,city_to,company,price,avg_price,price_diff,riqi))
            cur.connection.commit()
            standard_print("数据录入", [city_from,city_to,company,price,avg_price,price_diff,riqi])

    close_mysql(conn, cur)

def get_price_diff_step2(riqi):
    [conn,cur] = set_mysql('IFC')
    # 最后按公司维度做统计
    cur.execute("SELECT avg(price_diff) as price_diff,company FROM `otc_xiecheng_flight_analysis_price_diff` WHERE riqi=%s GROUP BY company",(riqi))
    result = cur.fetchall()
    for r in result:
        price_diff = r[0]
        company = r[1]

        if(company in company_domestic_list):
            type = "国内航空公司"
        else:
            type = "国际航空公司"

        num = cur.execute("SELECT * FROM otc_xiecheng_flight_analysis_company WHERE company=%s AND riqi=%s", (company,riqi))
        if (num > 0):
            cur.execute("UPDATE `otc_xiecheng_flight_analysis_company` SET price_diff=%s WHERE type=%s AND company = %s AND riqi = %s",(price_diff, type, company, riqi))
            cur.connection.commit()
            standard_print("数据更新:", [price_diff,type, company, riqi])
        else:
            cur.execute("INSERT INTO `otc_xiecheng_flight_analysis_company`(`price_diff`,`type`,`company`,`riqi`) VALUES (%s,%s,%s,%s)",(price_diff, type, company, riqi))
            cur.connection.commit()
            standard_print("数据录入:", [price_diff,type, company, riqi])

    cur.execute("SELECT count(*) as scale_foreign,company FROM `otc_xiecheng_flight_analysis_price_diff` WHERE riqi=%s GROUP BY company",(riqi))
    result = cur.fetchall()
    for r in result:
        scale_foreign = r[0]
        company = r[1]

        if (company in company_domestic_list):
            type = "国内航空公司"
        else:
            type = "国际航空公司"

        num = cur.execute("SELECT * FROM otc_xiecheng_flight_analysis_company WHERE company=%s AND riqi=%s", (company,riqi))
        if (num > 0):
            cur.execute("UPDATE `otc_xiecheng_flight_analysis_company` SET scale_foreign=%s WHERE type=%s AND company = %s AND riqi = %s",(scale_foreign,type, company, riqi))
            cur.connection.commit()
            standard_print("数据更新:", [scale_foreign,type, company, riqi])
        else:
            cur.execute("INSERT INTO `otc_xiecheng_flight_analysis_company`(`scale_foreign`,`type`,`company`,`riqi`) VALUES (%s,%s,%s,%s)",(scale_foreign,type, company, riqi))
            cur.connection.commit()
            standard_print("数据录入:", [scale_foreign,type, company, riqi])
    close_mysql(conn, cur)

def get_result_conclusion_domestic(riqi):
    conclusion_domestic = "<ol style='font-size:17px'>"
    [conn, cur] = set_mysql('IFC')

    #国内航空，结论1：市场份额
    total_scale = 0
    cur.execute("SELECT sum(scale) as total_scale FROM otc_xiecheng_flight_analysis_company WHERE riqi = %s GROUP BY riqi",(riqi))
    results = cur.fetchall()
    for r in results:
        total_scale = int(r[0])
    company_list = []
    scale_list = []
    cur.execute("SELECT company,scale,discount,punctuality FROM otc_xiecheng_flight_analysis_company WHERE riqi = %s AND type='国内航空公司' AND scale>10 ORDER BY scale DESC",(riqi))
    results = cur.fetchall()
    for r in results:
        company_list.append(r[0])
        scale_list.append(int(r[1]))

    CR5 = (scale_list[0] + scale_list[1] + scale_list[2] + scale_list[3] + scale_list[4]) / total_scale * 100
    CR5 = round(CR5,1)

    conclusion = "按每日航班数量计算，中国航空行业CR5达到"+str(CR5)+"%。"+"前五大航空公司分别为"
    conclusion = conclusion + company_list[0] + "(" + str(round(scale_list[0]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[1] + "(" + str(round(scale_list[1]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[2] + "(" + str(round(scale_list[2]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[3] + "(" + str(round(scale_list[3]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[4] + "(" + str(round(scale_list[4]/ total_scale*100,1)) + "%)。"
    conclusion = "<li>"+conclusion+"</li>"
    conclusion_domestic = conclusion_domestic+conclusion

    #国内航空公司平均折扣率
    company_list = []
    scale_list = []
    discount_list = []
    punctuality_list = []
    cur.execute("SELECT company,scale,discount,punctuality FROM otc_xiecheng_flight_analysis_company WHERE riqi = %s AND type='国内航空公司' AND scale>10 ORDER BY scale DESC",(riqi))
    results = cur.fetchall()
    for r in results:
        company_list.append(r[0])
        scale_list.append(r[1])
        discount_list.append(r[2])
        punctuality_list.append(r[3])

    max_index = get_max_index(discount_list)
    min_index = get_min_index(discount_list)
    avg_discount = get_average(discount_list)
    conclusion = "国内航空公司的平均折扣率为"+str(round(avg_discount,1))+"折，其中"
    conclusion = conclusion + company_list[min_index] + "的平均折扣最高，达到" + str(round(discount_list[min_index],1)) + "折；"
    conclusion = conclusion + company_list[max_index] + "的平均折扣最低，只有" + str(round(discount_list[max_index],1)) + "折。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_domestic = conclusion_domestic+conclusion

    max_index = get_max_index(punctuality_list)
    min_index = get_min_index(punctuality_list)
    avg_pun = get_average(punctuality_list)
    conclusion = "国内航空公司的平均准点率为" + str(round(avg_pun, 0)) + "%，其中"
    conclusion = conclusion + company_list[max_index] + "的准点率最高，达到" + str(round(punctuality_list[max_index], 0)) + "%；"
    conclusion = conclusion + company_list[min_index] + "的准点率最低，只有" + str(round(punctuality_list[min_index], 0)) + "%。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_domestic = conclusion_domestic+conclusion

    conclusion_domestic = conclusion_domestic+"</ol>"
    store_result("携程机票 国内结论", riqi, conclusion_domestic)
    close_mysql(conn, cur)

def get_result_conclusion_foreign(riqi):
    conclusion_foreign = "<ol style='font-size:17px'>"
    [conn, cur] = set_mysql('IFC')

    #国际航空，结论1：市场份额
    total_scale = 0
    cur.execute("SELECT sum(scale_foreign) as total_scale FROM otc_xiecheng_flight_analysis_company WHERE riqi = %s GROUP BY riqi",(riqi))
    results = cur.fetchall()
    for r in results:
        total_scale = int(r[0])
    company_list = []
    scale_list = []
    cur.execute("SELECT company,scale_foreign,discount,punctuality FROM otc_xiecheng_flight_analysis_company WHERE riqi = %s AND type='国际航空公司' AND scale_foreign>10 ORDER BY scale_foreign DESC",(riqi))
    results = cur.fetchall()
    for r in results:
        company_list.append(r[0])
        scale_list.append(int(r[1]))

    CR5 = (scale_list[0] + scale_list[1] + scale_list[2] + scale_list[3] + scale_list[4]) / total_scale * 100
    CR5 = round(CR5,1)

    conclusion = "按每日航班数量计算(国内航线不计)，国际航空行业CR5达到"+str(CR5)+"%。"+"前五大航空公司分别为"
    conclusion = conclusion + company_list[0] + "(" + str(round(scale_list[0]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[1] + "(" + str(round(scale_list[1]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[2] + "(" + str(round(scale_list[2]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[3] + "(" + str(round(scale_list[3]/ total_scale*100,1)) + "%)、"
    conclusion = conclusion + company_list[4] + "(" + str(round(scale_list[4]/ total_scale*100,1)) + "%)。"
    conclusion = "<li>"+conclusion+"</li>"
    conclusion_foreign = conclusion_foreign + conclusion


    #国内航空公司平均折扣率
    company_list = []
    pricediff_list = []
    cur.execute("SELECT company,price_diff FROM `otc_xiecheng_flight_analysis_company` WHERE riqi = %s AND type='国内航空公司' ORDER BY price_diff ASC",(riqi))
    results = cur.fetchall()
    for r in results:
        company_list.append(r[0])
        pricediff_list.append(r[1])
    conclusion = "中国航空公司在国际市场中票价普遍要比同航线公司更便宜，其中"
    for i in range(len(company_list)):
        if(pricediff_list[i] < 0):
            conclusion = conclusion + company_list[i]+"便宜"+ str(-round(pricediff_list[i]*100,1))+"%、"
    conclusion = conclusion[:-1]
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_foreign = conclusion_foreign+conclusion

    conclusion_foreign = conclusion_foreign+"</ol>"
    store_result("携程机票 国际结论", riqi, conclusion_foreign)
    close_mysql(conn, cur)


def get_result_airline_domestic(riqi):
    request = "携程机票 国内航线"
    [conn, cur] = set_mysql('IFC')
    important_domestic = ["北京","上海","广州","深圳","成都","杭州","武汉","西安","重庆","青岛","长沙","南京","厦门"
        ,"昆明","大连","天津","郑州","三亚","济南","福州","长春","常州","呼和浩特","合肥","石家庄","沈阳","贵阳"
        , "桂林","珠海","张家界","西宁","无锡","乌鲁木齐","太原","宁波","拉萨","九寨沟","哈尔滨"]

    response_list = []

    for city in important_domestic:
        city_from_list = []
        city_to_list = []
        scale_list = []
        cur.execute("SELECT count(DISTINCT flight_no) as scale,city_from,city_to FROM `otc_xiecheng_flight` WHERE type = '国内机票' AND (city_to=%s OR city_from=%s) AND riqi = %s GROUP BY city_from,city_to",(city,city,riqi))
        results = cur.fetchall()
        for r in results:
            scale_list.append(r[0])
            city_from_list.append(r[1])
            city_to_list.append(r[2])

        response = [city,city_from_list,city_to_list,scale_list]
        response_list.append(response)
    response_list = str(response_list)
    store_result(request,riqi,response_list)
    close_mysql(conn, cur)


def get_result_airline_foreign(riqi):
    request = "携程机票 国际航线"
    [conn, cur] = set_mysql('IFC')
    important_foreign = ["中国","日本","美国","英国","澳大利亚","法国","加拿大","南非","埃及","墨西哥","巴西","意大利","俄罗斯","土耳其","阿联酋","印度尼西亚","泰国","新加坡","韩国"]
    response_list = []

    for country in important_foreign:
        city_list = []
        cur.execute("SELECT otc_xiecheng_flight_city_foreign.city_name as city_name FROM `geo` INNER JOIN otc_xiecheng_flight_city_foreign "
                    "ON geo.city=otc_xiecheng_flight_city_foreign.city_name WHERE geo.province = %s",(country))
        results = cur.fetchall()
        for r in results:
            city_list.append(r[0])

        city_from_list = []
        city_to_list = []
        scale_list = []
        sql = "SELECT count(DISTINCT flight_no) as num,city_from,city_to FROM `otc_xiecheng_flight` WHERE type = '国际机票' AND riqi = '"+riqi+"' AND ("
        sql = sql+sql_add("city_to", "OR",city_list)
        sql = sql+" OR "
        sql = sql+sql_add("city_from", "OR",city_list)
        sql = sql+") GROUP BY city_from,city_to"
        cur.execute(sql)
        results = cur.fetchall()
        for r in results:
            scale_list.append(r[0])
            city_from_list.append(r[1])
            city_to_list.append(r[2])

        response = [country,city_from_list,city_to_list,scale_list]
        response_list.append(response)
    response_list = str(response_list)
    store_result(request,riqi,response_list)
    close_mysql(conn, cur)

def generate_airline_type():
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT ID,city_from,city_to FROM `otc_xiecheng_flight_city_matrix` WHERE 1")
    results = cur.fetchall()
    for r in results:
        ID = r[0]
        city_from = r[1]
        city_to = r[2]

        type_from = ""
        if(type_from == ""):
            try:
                cur.execute("SELECT type,city_name FROM `otc_xiecheng_flight_city` WHERE city_code=%s",(city_from))
                results = cur.fetchall()
                for r in results:
                    type_from = r[0]
                    city_name_from = r[1]
            except:
                pass
        if (type_from == ""):
            try:
                cur.execute("SELECT type,city_name FROM `otc_xiecheng_flight_city_foreign` WHERE city_code=%s", (city_from))
                results = cur.fetchall()
                for r in results:
                    type_from = r[0]
                    city_name_from = r[1]
            except:
                pass

        type_to = ""
        if (type_to == ""):
            try:
                cur.execute("SELECT type,city_name FROM `otc_xiecheng_flight_city` WHERE city_code=%s", (city_to))
                results = cur.fetchall()
                for r in results:
                    type_to = r[0]
                    city_name_to = r[1]
            except:
                pass
        if (type_to == ""):
            try:
                cur.execute("SELECT type,city_name FROM `otc_xiecheng_flight_city_foreign` WHERE city_code=%s", (city_to))
                results = cur.fetchall()
                for r in results:
                    type_to = r[0]
                    city_name_to = r[1]
            except:
                pass

        if(type_to == "" or type_from == ""):
            standard_print("数据有误，已经终止",[city_name_from,city_name_to,type_from,type_to])
            break

        type = type_from+" -> "+type_to
        cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET `type`=%s WHERE ID = %s", (type,ID))
        cur.connection.commit()
        standard_print("数据录入",[ID,city_name_from,city_name_to,type])
    close_mysql(conn, cur)


def initialize():
    [conn, cur] = set_mysql('IFC')
    num = cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET `state`=0 WHERE 1")
    cur.connection.commit()
    standard_print("初始化爬虫状态",[num])

    riqi = date_delta(1)
    cur.execute("INSERT INTO `otc_xiecheng_flight_batch`(`batch_riqi`) VALUES (%s)",(riqi))
    cur.connection.commit()
    standard_print("插入新batch",[riqi])
    close_mysql(conn, cur)

def get_seat_num(flight_type,transfer_detail):
    seat_num = 100
    #这里以后对机型进行拆分
    if (str_contain(flight_type + transfer_detail, "(小")):
        seat_num = random.randint(85,100)
    elif (str_contain(flight_type + transfer_detail, "(中")):
        seat_num = random.randint(120, 180)
    elif (str_contain(flight_type + transfer_detail, "(大")):
        seat_num = random.randint(220, 400)
    return seat_num

def get_transfer_num(riqi,city_from,city_to):
    [conn, cur] = set_mysql('IFC')
    transfer_num = 0
    cur.execute("SELECT count(DISTINCT flight_no1) FROM `otc_xiecheng_flight_transfer` WHERE riqi = %s AND city_from=%s AND city_to=%s",(riqi,city_from,city_to))
    results = cur.fetchall()
    for r in results:
        transfer_num = r[0]
    close_mysql(conn, cur)
    return transfer_num

def get_transfer_num_foreign(riqi,city_from,city_to):
    [conn, cur] = set_mysql('IFC')
    transfer_num = 0
    cur.execute("SELECT count(DISTINCT flight_no) FROM `otc_xiecheng_flight` WHERE riqi = %s AND city_from=%s AND city_to=%s AND transfer_num > 0",(riqi,city_from,city_to))
    results = cur.fetchall()
    for r in results:
        transfer_num = r[0]
    close_mysql(conn, cur)
    return transfer_num

def get_city_code(city_name):
    [conn, cur] = set_mysql('IFC')
    city_code = ""
    if (city_code == ""):
        try:
            cur.execute("SELECT city_code FROM `otc_xiecheng_flight_city` WHERE city_name=%s", (city_name))
            results = cur.fetchall()
            for r in results:
                city_code = r[0]
        except:
            pass
    if (city_code == ""):
        try:
            cur.execute("SELECT city_code FROM `otc_xiecheng_flight_city_foreign` WHERE city_name=%s", (city_name))
            results = cur.fetchall()
            for r in results:
                city_code = r[0]
        except:
            pass
    close_mysql(conn, cur)
    return city_code


def get_airline_type(city_from,city_to):
    airlint_type = ""
    city_from = get_city_code(city_from)
    city_to = get_city_code(city_to)
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT type FROM `otc_xiecheng_flight_city_matrix` WHERE city_from=%s AND city_to=%s",(city_from, city_to))
    results = cur.fetchall()
    for r in results:
        airlint_type = r[0]
    close_mysql(conn, cur)
    return airlint_type

def cal_analysis_airline(riqi):
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT riqi,city_from,city_to,type,count(DISTINCT flight_no) as scale FROM `otc_xiecheng_flight` WHERE riqi=%s GROUP BY city_from,city_to,riqi",(riqi))
    results = cur.fetchall()
    for r in results:
        riqi = r[0]
        city_from = r[1]
        city_to = r[2]
        type = r[3]
        scale = r[4]

        airline_type = get_airline_type(city_from,city_to)

        seat_num_list = []
        punctuality_list = []
        price_list = []
        discount_list = []

        cur.execute("SELECT flight_type,avg(discount),avg(price),avg(punctuality),transfer_detail FROM `otc_xiecheng_flight` WHERE riqi = %s AND city_from=%s AND city_to=%s GROUP BY flight_no",(riqi,city_from,city_to))
        results2 = cur.fetchall()
        for r2 in results2:
            flight_type = r2[0]
            discount = float(r2[1])
            price = float(r2[2])
            punctuality = float(r2[3])
            transfer_detail = r2[4]

            seat_num = get_seat_num(flight_type,transfer_detail)
            seat_num_list.append(seat_num)
            punctuality_list.append(punctuality)
            price_list.append(price)
            discount_list.append(discount)


        avg_seat = get_average(seat_num_list)
        avg_pun = get_average(punctuality_list)
        avg_price = get_average(price_list)
        avg_discount = get_average(discount_list)

        total_seat = sum(seat_num_list)
        total_flight = scale

        if (type == "国内机票"):
            transfer_num = get_transfer_num(riqi, city_from, city_to)
            transfer_percent = transfer_num / (scale + transfer_num)
        else:
            transfer_num = get_transfer_num_foreign(riqi, city_from, city_to)
            transfer_percent = transfer_num / scale

        cur.execute("INSERT INTO `otc_xiecheng_flight_analysis_airline`(`riqi`, `city_from`, `city_to`, `type`, `airline_type`, `avg_price`, `avg_discount`, `avg_seat`, `avg_pun`, `total_flight`, `total_seat`, `transfer_percent`) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (riqi,city_from,city_to,type,airline_type,avg_price,avg_discount,avg_seat,avg_pun,total_flight,total_seat,transfer_percent))
        cur.connection.commit()
        standard_print("数据录入",[riqi,city_from,city_to,type,airline_type,avg_price,avg_discount,avg_seat,avg_pun,total_flight,total_seat,transfer_percent,total_seat/total_flight])

    close_mysql(conn, cur)

def cal_analysis_airline_company(riqi):
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT DISTINCT(company) FROM `otc_xiecheng_flight` WHERE riqi=%s",(riqi))
    results = cur.fetchall()
    for r in results:
        company = r[0]

        cur.execute("SELECT city_from,city_to,type,flight_type,transfer_detail,price FROM `otc_xiecheng_flight` WHERE riqi=%s AND company=%s GROUP BY city_from,city_to,flight_no",(riqi,company))
        results2 = cur.fetchall()
        for r2 in results2:
            city_from = r2[0]
            city_to = r2[1]
            type = r2[2]
            flight_type = r2[3]
            transfer_detail = r2[4]
            price = r2[5]
            seat_num = get_seat_num(flight_type, transfer_detail)
            sales = price * seat_num
            airline_type = get_airline_type(city_from, city_to)
            cur.execute("INSERT INTO `otc_xiecheng_flight_analysis_company_airline`(`riqi`, `company`, `city_from`, "
                        "`city_to`, `type`, `airline_type`, `seat_num`, `sales`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (riqi,company,city_from,city_to,type,airline_type,seat_num,sales))
            cur.connection.commit()
            standard_print("数据录入",[company,riqi,city_from,city_to,type,seat_num,sales,airline_type])

    close_mysql(conn, cur)

def wash_analysis_airline_company(riqi):
    target_list = ["一线城市 -> 一线城市",
                   "一线城市 -> 其他协调机场",
                   "一线城市 -> 非协调机场",
                   "其他协调机场 -> 其他协调机场",
                   "其他协调机场 -> 非协调机场",
                   "非协调机场 -> 非协调机场",
                   "一线城市 -> 亚洲",
                   "一线城市 -> 欧洲",
                   "一线城市 -> 美洲",
                   "一线城市 -> 非洲",
                   "一线城市 -> 大洋洲",
                   "其他协调机场 -> 亚洲",
                   "其他协调机场 -> 欧洲",
                   "其他协调机场 -> 美洲",
                   "其他协调机场 -> 非洲",
                   "其他协调机场 -> 大洋洲",
                   "非协调机场 -> 亚洲",
                   "非协调机场 -> 欧洲",
                   "非协调机场 -> 美洲",
                   "非协调机场 -> 非洲",
                   "非协调机场 -> 大洋洲",
                   "亚洲 -> 亚洲",
                   "亚洲 -> 欧洲",
                   "亚洲 -> 美洲",
                   "亚洲 -> 非洲",
                   "亚洲 -> 大洋洲",
                   "欧洲 -> 欧洲",
                   "欧洲 -> 美洲",
                   "欧洲 -> 非洲",
                   "欧洲 -> 大洋洲",
                   "美洲 -> 美洲",
                   "美洲 -> 非洲",
                   "美洲 -> 大洋洲",
                   "非洲 -> 非洲",
                   "非洲 -> 大洋洲",
                   "大洋洲 -> 大洋洲"
                   ]
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT ID,airline_type FROM `otc_xiecheng_flight_analysis_company_airline` WHERE riqi=%s", (riqi))
    results = cur.fetchall()
    for r in results:
        ID = r[0]
        airline_type = r[1]
        type1 = airline_type.split(" -> ")[0]
        type2 = airline_type.split(" -> ")[1]

        if (airline_type not in target_list):
            new_airline_type = type2 + " -> " + type1
            cur.execute("UPDATE `otc_xiecheng_flight_analysis_company_airline` SET `airline_type`=%s WHERE ID = %s", (new_airline_type, ID))
            cur.connection.commit()
            standard_print("数据更新", [ID, airline_type, new_airline_type])
    close_mysql(conn, cur)


def get_result_indicator_industry(batch_riqi):
    request = "航空指标 行业分析"
    conclusion_indicator_industry = "<ol style='font-size:17px'>"
    [riqi,value] = get_indicator_lastest_value("2160002772")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year+"年"+month+"月，民航客运量为"+value+"万人，同比"+get_indicator_lastest_rate("2160002780")+"，其中国内航线民航客运量为"+get_indicator_lastest_value("2160002773")[1]+\
                 "万人，同比"+get_indicator_lastest_rate("2160002781")+"、国际航线民航客运量为"+get_indicator_lastest_value("2160002774")[1]+"万人，同比"+get_indicator_lastest_rate("2160002782")+"。"
    conclusion = "<li>"+conclusion+"</li>"
    conclusion_indicator_industry = conclusion_indicator_industry + conclusion

    [riqi, value] = get_indicator_lastest_value("2160002776")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year + "年" + month + "月，民航旅客周转量为" + value + "亿人公里，同比" + get_indicator_lastest_rate(
        "2160002784") + "，其中国内航线民航旅客周转量为" + str(float(get_indicator_lastest_value("2160002777")[1])/10000) + \
                 "亿人公里，同比" + get_indicator_lastest_rate("2160002785") + "、国际航线民航旅客周转量为" + \
                 str(float(get_indicator_lastest_value("2160002778")[1])/10000) + "亿人公里，同比" + get_indicator_lastest_rate("2160002786") + "。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_indicator_industry = conclusion_indicator_industry + conclusion

    [riqi, value] = get_indicator_lastest_value("2160001566")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year + "年" + month + "月，民航正班客座率为" + value + "%。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_indicator_industry = conclusion_indicator_industry + conclusion

    conclusion_indicator_industry = conclusion_indicator_industry + "</ol>";
    store_result(request, batch_riqi, conclusion_indicator_industry)


def get_result_indicator_company(batch_riqi):
    request = "航空指标 航空公司分析"
    conclusion_indicator_company = "<ol style='font-size:17px'>"
    [riqi,value] = get_indicator_lastest_value("2160001734")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year+"年"+month+"月，南方航空客座率为"+value+"%，旅客周转量为"+str(round(float(get_indicator_lastest_value("2160001702")[1])/100,1))\
                 +"亿客公里，可用货运吨公里为"+str(round(float(get_indicator_lastest_value("2160001730")[1])/100,1))+"亿吨公里。"
    conclusion = "<li>"+conclusion+"</li>"
    conclusion_indicator_company = conclusion_indicator_company + conclusion

    [riqi, value] = get_indicator_lastest_value("2160001876")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year + "年" + month + "月，东方航空客座率为" + value + "%，旅客周转量为" + str(round(float(get_indicator_lastest_value("2160001900")[1])/100,1))\
                 + "亿客公里，可用货运吨公里为" + str(round(float(get_indicator_lastest_value("2160001892")[1])/100,1)) + "亿吨公里。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_indicator_company = conclusion_indicator_company + conclusion

    [riqi, value] = get_indicator_lastest_value("2160001965")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year + "年" + month + "月，海南航空客座率为" + value + "%，客运量为" + str(round(float(get_indicator_lastest_value("2160001953")[1])/10000,1))\
                 + "万人次，货邮运输量为" +str(round(float(get_indicator_lastest_value("2160001969")[1])/10000,1))+ "万吨。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_indicator_company = conclusion_indicator_company + conclusion

    [riqi, value] = get_indicator_lastest_value("2160028907")
    year = riqi[:4]
    month = riqi[4:6]
    conclusion = year + "年" + month + "月，吉祥航空客座率为" + value + "%，客运量为" + str(round(float(get_indicator_lastest_value("2160028899")[1])/10,1))\
                 + "万人次，货邮运输量为" + str(round(float(get_indicator_lastest_value("2160028903")[1])/10000,1)) + "万吨。"
    conclusion = "<li>" + conclusion + "</li>"
    conclusion_indicator_company = conclusion_indicator_company + conclusion

    conclusion_indicator_company = conclusion_indicator_company + "</ol>";
    store_result(request, batch_riqi, conclusion_indicator_company)
    #standard_print("",[year,month,conclusion_indicator_company])

[conn, cur] = set_mysql('IFC')
cur.execute("SELECT batch_riqi FROM `otc_xiecheng_flight_batch` WHERE 1 ORDER BY batch_riqi DESC LIMIT 1")
results = cur.fetchall()
for r in results:
    riqi = str(r[0])
close_mysql(conn, cur)
standard_print("数据分析日期为",[riqi])
get_price_diff(riqi)
get_price_diff_step2(riqi)

get_company(riqi,"punctuality")
get_company(riqi,"scale")
get_company(riqi,"price")
get_company(riqi,"discount")
get_company(riqi,"scale_foreign")
get_company(riqi,"price_foreign")
delete_company(riqi)

cal_analysis_airline(riqi)
cal_analysis_airline_company(riqi)
wash_analysis_airline_company(riqi)

get_result_conclusion_domestic(riqi)
get_result_conclusion_foreign(riqi)
get_result_airline_domestic(riqi)
get_result_airline_foreign(riqi)
get_result_indicator_industry(riqi)
get_result_indicator_company(riqi)

initialize()

#draw_scatter("2017-11-05","punctuality","scale")
#generate_airline_type()
