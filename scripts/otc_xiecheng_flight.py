# -*-: encoding: utf-8 -*-

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time
import datetime
import requests
import os
import sys
#sys.path.append('../common/')
from common import *


company_domestic_list = ["中国联航","海南航空","深圳航空","南方航空","西藏航空","中国国航"
    , "昆明航空","四川航空","东方航空","山东航空","厦门航空","天津航空","大新华航空"
    , "长龙航空","首都航空","上海航空","吉祥航空","金鹏航空","华夏航空","河北航空"
    , "青岛航空","成都航空","红土航空","春秋航空","东海航空","九元航空","长安航空"
    , "西部航空","福州航空","祥鹏航空","乌鲁木齐航空","奥凯航空","幸福航空","瑞丽航空"
    , "桂林航空","多彩航空","龙江航空","北部湾航空","江西航空"]

def wash_seat_type(str):
    seat_type = ""
    if (seat_type == "" and len(str.split("经济舱")) > 1):
        seat_type = "经济舱"
    if (seat_type == "" and len(str.split("公务舱")) > 1):
        seat_type = "公务舱"
    if (seat_type == "" and len(str.split("头等舱")) > 1):
        seat_type = "头等舱"
    if (seat_type == "" and len(str.split("高铁")) > 1):
        seat_type = "高铁"
    if (seat_type == "" and len(str.split("大巴")) > 1):
        seat_type = "大巴"
    return seat_type

def get_ticket_type(sub_item):
    ticket_type = ''

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div.tag_name > img[data-saveagg='航司直销']")[0]
            ticket_type = '航司直销'
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > i.ico_fltnhtl_md")[0]
            ticket_type = '飞宿特惠'
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div[data-saveagg='优选']")[0]
            ticket_type = '优选'
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div[data-saveagg='特惠']")[0]
            ticket_type = '特惠'
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div[data-saveagg='专属']")[0]
            ticket_type = '专属'
        except:
            pass
    return ticket_type

def get_base_info_transfer(item):
    sub_result = item.select("div.logo > div.logo-item > span.J_craft > strong")
    flight_company1 = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.logo > div.logo-item > span.J_craft > span")
    flight_no1 = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.logo > div.logo-item > span.J_craft > strong")
    flight_company2 = remove_space(sub_result[1].get_text())

    sub_result = item.select("div.logo > div.logo-item > span.J_craft > span")
    flight_no2 = remove_space(sub_result[1].get_text())

    sub_result = item.select("div.right > div:nth-of-type(1) > strong.time")
    time_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.right > div:nth-of-type(2)")
    place_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.left > div:nth-of-type(1) > strong.time")
    time_end = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.left > div:nth-of-type(2)")
    place_end = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.center > span.stay-city > span.city-name")
    city_transfer = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.center > span.stay-time")
    time_transfer = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.service > div.service-item > span.J_punctuality")
    punctuality = remove_space(sub_result[0].get_text().replace("%",""))

    sub_result = item.select("div.price > span.J_base_price")
    lowest_price = remove_space(sub_result[0].get_text().replace("¥", ""))

    return {'flight_company1': flight_company1, 'flight_no1': flight_no1, 'flight_company2': flight_company2, 'flight_no2': flight_no2,
            'time_start': time_start, 'place_start': place_start, 'time_end': time_end, 'place_end': place_end,'city_transfer':city_transfer,
            'time_transfer':time_transfer,'punctuality':punctuality,'lowest_price': lowest_price}



def get_base_info_more(item):
    #把1和2的抓取顺序颠倒，这样就可以一次抓到大巴火车，一次抓到飞机了
    flight_company1 = ""
    flight_company2 = ""
    if(flight_company1 == ""):
        try:
            sub_result = item.select("div.logo > div.bus_logo > strong")
            flight_company1 = remove_space(sub_result[0].get_text())
            sub_result = item.select("div.logo > div.bus_logo > span")
            flight_no1 = remove_space(sub_result[0].get_text())
        except:
            pass

    if (flight_company1 == ""):
        try:
            sub_result = item.select("div.logo > div.train_logo > strong")
            flight_company1 = remove_space(sub_result[0].get_text())
            sub_result = item.select("div.logo > div.train_logo > span")
            flight_no1 = remove_space(sub_result[0].get_text())
        except:
            pass

    if (flight_company1 == ""):
        try:
            sub_result = item.select("div.logo > div.logo-item > span.J_craft > strong")
            flight_company1 = remove_space(sub_result[0].get_text())
            sub_result = item.select("div.logo > div.logo-item > span.J_craft > span")
            flight_no1 = remove_space(sub_result[0].get_text())
        except:
            pass

    if (flight_company2 == ""):
        try:
            sub_result = item.select("div.logo > div.logo-item > span.J_craft > strong")
            flight_company2 = remove_space(sub_result[0].get_text())
            sub_result = item.select("div.logo > div.logo-item > span.J_craft > span")
            flight_no2 = remove_space(sub_result[0].get_text())
        except:
            pass

    if (flight_company2 == ""):
        try:
            sub_result = item.select("div.logo > div.bus_logo > strong")
            flight_company2 = remove_space(sub_result[0].get_text())
            sub_result = item.select("div.logo > div.bus_logo > span")
            flight_no2 = remove_space(sub_result[0].get_text())
        except:
            pass

    if (flight_company2 == ""):
        try:
            sub_result = item.select("div.logo > div.train_logo > strong")
            flight_company2 = remove_space(sub_result[0].get_text())
            sub_result = item.select("div.logo > div.train_logo > span")
            flight_no2 = remove_space(sub_result[0].get_text())
        except:
            pass

    sub_result = item.select("div.right > div:nth-of-type(1) > strong.time")
    time_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.right > div:nth-of-type(2)")
    place_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.left > div:nth-of-type(1) > strong.time")
    time_end = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.left > div:nth-of-type(2)")
    place_end = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.center > span.stay-city > span.city-name")
    city_transfer = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.center > span.stay-time")
    time_transfer = remove_space(sub_result[0].get_text())

    punctuality = -1
    try:
        sub_result = item.select("div.service > div.service-item > span.J_punctuality")
        punctuality = remove_space(sub_result[0].get_text().replace("%",""))
    except:
        pass

    sub_result = item.select("div.price > span.J_base_price")
    lowest_price = remove_space(sub_result[0].get_text().replace("¥", ""))

    return {'flight_company1': flight_company1, 'flight_no1': flight_no1, 'flight_company2': flight_company2, 'flight_no2': flight_no2,
            'time_start': time_start, 'place_start': place_start, 'time_end': time_end, 'place_end': place_end,'city_transfer':city_transfer,
            'time_transfer':time_transfer,'punctuality':punctuality,'lowest_price': lowest_price}


def get_base_info(item):
    sub_result = item.select("td.logo > div.J_flight_no > strong")
    company = remove_space(sub_result[0].get_text())

    sub_result = item.select("td.logo > div.J_flight_no > span")
    flight_no = remove_space(sub_result[0].get_text())

    flight_type = ''
    if (flight_type == ''):
        try:
            sub_result = item.select("td.logo > div.low_text > span")
            flight_type = remove_space(sub_result[0].get_text())
        except:
            pass

    if (flight_type == ''):
        try:
            sub_result = item.select("td.logo > span.J_craft")
            flight_type = remove_space(sub_result[0].get_text().replace("舒适尊享", ""))
        except:
            pass

    sub_result = item.select("td.right > div:nth-of-type(1) > strong.time")
    time_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("td.right > div:nth-of-type(2)")
    place_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("td.left > div:nth-of-type(1) > strong.time")
    time_end = remove_space(sub_result[0].get_text())

    sub_result = item.select("td.left > div:nth-of-type(2)")
    place_end = remove_space(sub_result[0].get_text())

    punctuality = -1
    try:
        sub_result = item.select("td.service > div.service-item > span.J_punctuality")
        punctuality = remove_space(sub_result[0].get_text().replace("%",""))
    except:
        pass

    sub_result = item.select("td.price > span.J_base_price > span")
    lowest_price = remove_space(sub_result[0].get_text().replace("¥", ""))

    transfer_num = 0
    transfer_detail = ''

    return {'company':company,'flight_no':flight_no,'flight_type':flight_type,'time_start':time_start,
            'place_start':place_start,'time_end':time_end,'place_end':place_end,'punctuality':punctuality,
            'lowest_price':lowest_price,'transfer_num':transfer_num,'transfer_detail':transfer_detail}



def get_detail_info_transfer(sub_sub_item):
    discount = ""
    if (discount == ""):
        try:
            sub_sub_sub_result = sub_sub_item.select("div.discount > span.low_text > span")
            discount = remove_space(sub_sub_sub_result[0].get_text())
        except:
            pass

    seat_type1 = ""
    seat_type2 = ""
    if(discount != ""):
        seat_type1 = remove_space(discount.split("+")[0])
        seat_type2 = remove_space(discount.split("+")[1])
        seat_type1 = wash_seat_type(seat_type1)
        seat_type2 = wash_seat_type(seat_type2)

    sub_sub_sub_result = sub_sub_item.select("div.flt_price > span.J_flightPrice")
    price = remove_space(sub_sub_sub_result[0].get_text().replace("¥", ""))

    return {'seat_type1':seat_type1,'seat_type2':seat_type2,'price':price}

def get_detail_info_more(sub_sub_item):
    discount = ""
    if (discount == ""):
        try:
            sub_sub_sub_result = sub_sub_item.select("div.discount > span.low_text")
            discount = remove_space(sub_sub_sub_result[0].get_text())
        except:
            pass

    seat_type1 = ""
    seat_type2 = ""
    if(discount != ""):
        seat_type1 = remove_space(discount.split("+")[0])
        seat_type2 = remove_space(discount.split("+")[1])
        seat_type1 = wash_seat_type(seat_type1)
        seat_type2 = wash_seat_type(seat_type2)

    sub_sub_sub_result = sub_sub_item.select("div.flt_price > span.J_flightPrice")
    price = remove_space(sub_sub_sub_result[0].get_text().replace("¥", ""))

    return {'seat_type1':seat_type1,'seat_type2':seat_type2,'price':price}

def get_detail_info(sub_sub_item):
    sub_sub_sub_result = sub_sub_item.select("div[class='inb after_sales'] > span")
    endorse_fee = remove_space(sub_sub_sub_result[0].get_text())

    discount = ''
    if(discount == ""):
        try:
            sub_sub_sub_result = sub_sub_item.select("div.discount > div.low_text > label")
            discount = remove_space(sub_sub_sub_result[0].get_text())
        except:
            pass

    if (discount == ""):
        try:
            sub_sub_sub_result = sub_sub_item.select("div.discount > span.low_text > span")
            discount = remove_space(sub_sub_sub_result[0].get_text())
        except:
            pass

    seat_type = ""
    if (seat_type == "" and len(discount.split("经济舱")) > 1):
        seat_type = "经济舱"
    if (seat_type == "" and len(discount.split("公务舱")) > 1):
        seat_type = "公务舱"
    if (seat_type == "" and len(discount.split("头等舱")) > 1):
        seat_type = "头等舱"

    if(discount == ""):
        discount = "-1"
    elif(len(discount.split("全价")) > 1):
        discount = "10"
    else:
        p = re.compile('(\d\.\d)')
        flag = p.search(discount)
        if (flag is not None):
            discount = str(flag.group(1))

    sub_sub_sub_result = sub_sub_item.select("div.flt_price > span.J_flightPrice")
    price = remove_space(sub_sub_sub_result[0].get_text().replace("¥", ""))

    return {'endorse_fee':endorse_fee,'discount':discount,'price':price,'seat_type':seat_type}

def get_base_info_foreign(item):
    sub_result = item.select("div.flight-row > div.flight-col-base > div.airline-name")
    company = remove_space(sub_result[0].get_text())
    company = company.split("等")[0]

    flight_type = ''
    flight_no = ''
    transfer_num = 0
    transfer_detail = ''

    sub_result = item.select("div.flight-row > div.flight-col-base > div.flight-No")
    flight_no = remove_space(sub_result[0].get_text())
    try:
        sub_result = item.select("div.flight-row > div.flight-col-base > div.flight-No > span.plane-type")
        flight_type = remove_space(sub_result[0].get_text())
        flight_no = flight_no.replace(flight_type, "")
    except:  # 说明是组合航班
        sub_result = item.select("div.flight-row > div.flight-col-base > div.flight-No")
        transfer_num = remove_space(sub_result[0].get_text())
        transfer_num = transfer_num.split("程")[0]
        sub_result = item.select("div.flight-detail-expend")
        transfer_detail = remove_space(sub_result[0].get_text())
        pass

    sub_result = item.select("div.flight-row > div.flight-col-detail > div.flight-detail-depart > div.flight-detail-time")
    time_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.flight-row > div.flight-col-detail > div.flight-detail-depart > div.flight-detail-airport")
    place_start = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.flight-row > div.flight-col-detail > div.flight-detail-return > div.flight-detail-time")
    time_end = remove_space(sub_result[0].get_text())

    sub_result = item.select("div.flight-row > div.flight-col-detail > div.flight-detail-return > div.flight-detail-airport")
    place_end = remove_space(sub_result[0].get_text())

    lowest_price = -1
    punctuality = -1

    return {'company': company, 'flight_no': flight_no, 'flight_type': flight_type, 'time_start': time_start,
            'place_start': place_start, 'time_end': time_end, 'place_end': place_end, 'punctuality': punctuality,
            'lowest_price': lowest_price,'transfer_num': transfer_num, 'transfer_detail': transfer_detail}


def get_detail_info_foreign(sub_item):
    ticket_type = ""
    if (ticket_type == ""):
        try:
            sub_sub_result = sub_item.select("div.seat-special > span.tag-text")[0]
            ticket_type = "特惠推荐"
        except:
            pass

    if (ticket_type == ""):
        try:
            sub_sub_result = sub_item.select("div.seat-special > span.tag-business")[0]
            ticket_type = "商务优选"
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div[data-saveagg='24H特惠退']")[0]
            ticket_type = '24H特惠退'
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div[data-saveagg='旅行套餐']")[0]
            ticket_type = '旅行套餐'
        except:
            pass

    if (ticket_type == ''):
        try:
            sub_sub_result = sub_item.select("div.J_rowheader > div[data-saveagg='航司直销']")[0]
            ticket_type = '航司直销'
        except:
            pass


    sub_sub_result = sub_item.select("div.seat-type")
    seat_type = remove_space(sub_sub_result[0].get_text())

    sub_sub_result = sub_item.select("div.seat-price > div.mb5 > span.price")
    price = remove_space(sub_sub_result[0].get_text().replace("¥", ""))

    discount = 0
    endorse_fee = ''

    return {'endorse_fee': endorse_fee, 'discount': discount, 'price': price, 'seat_type': seat_type, 'ticket_type':ticket_type}

def loop_foreign(result,city_from_name,city_to_name,riqi):
    count = 0
    for item in result:
        base_info = get_base_info_foreign(item)
        base_info['city_from'] = city_from_name
        base_info['city_to'] = city_to_name
        base_info['riqi'] = riqi
        base_info['type'] = "国际机票"
        sub_result = item.select("div.seats-list > div.seat-row")
        lowest_price = -1
        for sub_item in sub_result:
            detail_info = get_detail_info_foreign(sub_item)
            store_flight(base_info, detail_info)
            count += 1
    return count

def loop_domestic(result,city_from_name,city_to_name,riqi):
    count = 0
    if (len(result) == 0):
        print("没有航班信息")
        return 0
    for item in result:
        is_store = 0
        sub_result = item.select("div.J_pricePannel > div.search_table > div.J_group")
        for sub_item in sub_result:
            #直飞
            try:
                base_info = get_base_info(item)
                base_info['city_from'] = city_from_name
                base_info['city_to'] = city_to_name
                base_info['riqi'] = riqi
                base_info['type'] = "国内机票"
                ticket_type = get_ticket_type(sub_item)
                sub_sub_result = sub_item.select("div.search_table_col")
                for sub_sub_item in sub_sub_result:
                    detail_info = get_detail_info(sub_sub_item)
                    detail_info['ticket_type'] = ticket_type
                    store_flight(base_info,detail_info)
                    count += 1
                is_store = 1
            except:
                pass

            #中转
            try:
                if(is_store == 0):
                    base_info = get_base_info_transfer(item)
                    base_info['city_from'] = city_from_name
                    base_info['city_to'] = city_to_name
                    base_info['riqi'] = riqi
                    base_info['type'] = "国内机票"
                    ticket_type = get_ticket_type(sub_item)
                    sub_sub_result = sub_item.select("div.search_table_col")
                    for sub_sub_item in sub_sub_result:
                        detail_info = get_detail_info_transfer(sub_sub_item)
                        detail_info['ticket_type'] = ticket_type
                        store_flight_transfer(base_info,detail_info)
                        count += 1
                is_store = 1
            except:
                pass

        if(is_store == 0):
            sub_result = item.select("div.J_pricePannel > div.J_row")
            for sub_item in sub_result:
                # 更多出行方案
                base_info = get_base_info_more(item)
                base_info['city_from'] = city_from_name
                base_info['city_to'] = city_to_name
                base_info['riqi'] = riqi
                base_info['type'] = "国内机票"
                ticket_type = get_ticket_type(sub_item)
                sub_sub_result = sub_item.select("div.search_table_col")
                for sub_sub_item in sub_sub_result:
                    detail_info = get_detail_info_more(sub_sub_item)
                    detail_info['ticket_type'] = ticket_type
                    store_flight_more(base_info, detail_info)
                    count += 1

    return count

def store_flight(b,d):
    if(b['company'] in company_domestic_list):
        company_type = "国内航空公司"
    else:
        company_type = "国际航空公司"

    [conn, cur] = set_mysql('IFC')

    cur.execute("INSERT INTO `otc_xiecheng_flight`(`city_from`, `city_to`, `riqi`, `company`, `flight_no`, "
                "`flight_type`, `time_start`, `time_end`, `place_start`, `place_end`, `punctuality`, "
                "`lowest_price`, `ticket_type`, `endorse_fee`, `discount`, `price`, `seat_type`, `type`,`company_type`,`transfer_num`,`transfer_detail`) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (b['city_from'],b['city_to'],b['riqi'],b['company'],b['flight_no'],b['flight_type'],
                 b['time_start'],b['time_end'],b['place_start'],b['place_end'],b['punctuality'],
                 b['lowest_price'],d['ticket_type'],d['endorse_fee'],d['discount'],d['price'],d['seat_type'],
                 b['type'],company_type,b['transfer_num'],b['transfer_detail']))
    cur.connection.commit()
    print("数据录入-直飞: " + str(b) + " | "+ str(d))
    close_mysql(conn, cur)


def store_flight_transfer(b,d):
    if(b['flight_company1'] in company_domestic_list):
        company_type1 = "国内航空公司"
    else:
        company_type1 = "国际航空公司"

    if (b['flight_company2'] in company_domestic_list):
        company_type2 = "国内航空公司"
    else:
        company_type2 = "国际航空公司"

    [conn, cur] = set_mysql('IFC')

    cur.execute("INSERT INTO `otc_xiecheng_flight_transfer`(`city_from`, `city_transfer`, `city_to`,`riqi`, "
                "`time_start`, `time_end`, `time_transfer`, `place_start`, `place_end`, `flight_company1`, "
                "`flight_company2`, `flight_no1`, `flight_no2`, `punctuality`, `seat_type1`, `seat_type2`, "
                "`lowest_price`, `price`, `company_type1`, `company_type2`,`ticket_type`) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (b['city_from'],b['city_transfer'],b['city_to'],b['riqi'],b['time_start'],b['time_end'],b['time_transfer'],
                 b['place_start'],b['place_end'],b['flight_company1'],b['flight_company2'],b['flight_no1'],
                 b['flight_no2'],b['punctuality'],d['seat_type1'],d['seat_type2'],b['lowest_price'],d['price'],
                 company_type1,company_type2,d['ticket_type']))
    cur.connection.commit()
    print("数据录入-中转: " + str(b) + " | "+ str(d))
    close_mysql(conn, cur)



def store_flight_more(b,d):
    if(b['flight_company1'] in company_domestic_list):
        company_type1 = "国内航空公司"
    else:
        company_type1 = "国际航空公司"

    if (b['flight_company2'] in company_domestic_list):
        company_type2 = "国内航空公司"
    else:
        company_type2 = "国际航空公司"

    [conn, cur] = set_mysql('IFC')

    cur.execute("INSERT INTO `otc_xiecheng_flight_transfer`(`city_from`, `city_transfer`, `city_to`,`riqi`, "
                "`time_start`, `time_end`, `time_transfer`, `place_start`, `place_end`, `flight_company1`, "
                "`flight_company2`, `flight_no1`, `flight_no2`, `punctuality`, `seat_type1`, `seat_type2`, "
                "`lowest_price`, `price`, `company_type1`, `company_type2`,`ticket_type`) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (b['city_from'],b['city_transfer'],b['city_to'],b['riqi'],b['time_start'],b['time_end'],b['time_transfer'],
                 b['place_start'],b['place_end'],b['flight_company1'],b['flight_company2'],b['flight_no1'],
                 b['flight_no2'],b['punctuality'],d['seat_type1'],d['seat_type2'],b['lowest_price'],d['price'],company_type1,company_type2,d['ticket_type']))
    cur.connection.commit()
    print("数据录入-更多: " + str(b) + " | "+ str(d))
    close_mysql(conn, cur)

def lock_resource(ID):
    [conn, cur] = set_mysql('IFC')
    cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET `state`=1 WHERE ID = %s", (ID))
    cur.connection.commit()
    standard_print("锁定资源:", [ID])
    close_mysql(conn, cur)

def release_resource(ID):
    [conn, cur] = set_mysql('IFC')
    cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET `state`=0 WHERE ID = %s", (ID))
    cur.connection.commit()
    standard_print("释放资源:", [ID])
    close_mysql(conn, cur)

def pass_resource(ID):
    [conn, cur] = set_mysql('IFC')
    cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET `state`=2 WHERE ID = %s", (ID))
    cur.connection.commit()
    standard_print("完成资源:", [ID])
    close_mysql(conn, cur)

def scrap_domestic(batch_ID,batch_riqi, scraper):
    # 抓取国内航班
    error_num = 0
    ip_proxy = wash_proxy()['http']
    while 1:
        # 提取出等待抓取的网址
        [conn, cur] = set_mysql('IFC')
        scrap_riqi = batch_riqi
        #scrap_riqi = datetime.datetime.now()
        #scrap_riqi = scrap_riqi.strftime('%Y-%m-%d')
        # 跑完一遍之后，加入is_important的筛选
        num = cur.execute("SELECT ID,city_from,city_to FROM otc_xiecheng_flight_city_matrix WHERE "
                          "last_scrap_riqi < %s AND is_important >= 1 AND is_foreign=0 AND state=0 AND scraper=%s ORDER BY RAND() LIMIT 1", (scrap_riqi,scraper))
        if (num == 0):
            print("全部任务抓取完毕")
            break
        results = cur.fetchall()

        for r in results:
            ID = r[0]
            city_from = r[1]
            city_to = r[2]

        lock_resource(ID)

        cur.execute("select city_name from otc_xiecheng_flight_city where city_code = %s", city_from)
        results = cur.fetchall()
        for r in results:
            city_from_name = r[0]

        cur.execute("select city_name from otc_xiecheng_flight_city where city_code = %s", city_to)
        results = cur.fetchall()
        for r in results:
            city_to_name = r[0]
        close_mysql(conn, cur)

        if(city_from_name == "" or city_to_name == ""):
            standard_print("这里有问题",[city_from,city_to,city_from_name,city_to_name])
            break

        riqi = datetime.datetime.now() + datetime.timedelta(days=1)  # 往前进一天
        riqi = riqi.strftime('%Y-%m-%d')
        url = "http://flights.ctrip.com/booking/" + city_from + "-" + city_to + "-day-1.html?DDate1=" + str(riqi)
        standard_print("开始抓取:", [url])

        # 构造浏览器，进行抓取。continue的不release，只有break的才release
        while error_num < 20:  # 只有break出去的，才选抓取完毕
            #user_agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36"
            #host = "flights.ctrip.com"
            #origin = "http://flights.ctrip.com"
            #browser = get_chrome("",0,1)
            browser = get_phantomJS(ip_proxy)

            try:
                browser.get(url)
                browser.refresh()
                print(browser.title)
                if (len(browser.title.split("机票预订")) > 1):
                    pass
                else:  # 如果连接不了网页，那么title是flights.ctrip.com
                    print("访问被拒绝")
                    ip_proxy = wash_proxy()['http']
                    error_num += 1
                    browser.close()
                    continue
            except:
                print("打开页面超时，更换proxy中")
                ip_proxy = wash_proxy()['http']
                error_num += 1
                browser.close()
                continue

            time.sleep(2)

            try:
                for i in range(0, 10):
                    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
            except:
                print("操作页面超时，更换proxy中")
                ip_proxy = wash_proxy()['http']
                error_num += 1
                browser.close()
                continue

            pageSource = browser.page_source
            soup = BeautifulSoup(pageSource, "html.parser")
            try:
                result = soup.select("div#J_searchError > div#error_content > div.alert_content")
                error_message = remove_space(result[0].get_text())
                print(error_message)
                if (len(error_message.split("没有直飞航班")) > 1):
                    count = 0
                    error_num = 0
                    browser.close()
                    release_resource(ID)
                    break
                if (len(error_message.split("您搜索的航班已售完")) > 1):
                    count = 0
                    error_num = 0
                    browser.close()
                    release_resource(ID)
                    break
                if (len(error_message.split("您访问的太快了")) > 1):
                    count = 0
                    error_num += 1
                    # time.sleep(60)
                    print("抓取出现异常，更换proxy中")
                    ip_proxy = wash_proxy()['http']
                    browser.close()
                    continue
            except:
                result = soup.select("div.flight-list-content > div#J_flightlist2 > div.search_box")
                count = loop_domestic(result,city_from_name,city_to_name,batch_riqi)
                error_num = 0
                browser.close()
                release_resource(ID)
                break

        if (error_num >= 20):
            print("连续错误20次，停止程序")
            release_resource(ID)
            break

        [conn, cur] = set_mysql('IFC')
        cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET last_scrap_riqi=%s,is_important=%s,state=2 WHERE ID = %s",(scrap_riqi, count, ID))
        cur.connection.commit()
        standard_print("释放资源并更新最近录入时间:", [scrap_riqi, ID, count])


        #计算工作量
        cur.execute("SELECT count(*) FROM otc_xiecheng_flight_city_matrix WHERE is_important >=1 AND state=2")
        results = cur.fetchall()
        for r in results:
            num_done = r[0]

        cur.execute("SELECT count(*) FROM otc_xiecheng_flight_city_matrix WHERE is_important >=1 AND state=0")
        results = cur.fetchall()
        for r in results:
            num_todo = r[0]

        cur.execute("UPDATE `otc_xiecheng_flight_batch` SET num_done=%s,num_todo=%s WHERE batch_ID = %s",(num_done,num_todo,batch_ID))
        cur.connection.commit()
        standard_print("更新工作量:", [batch_ID,batch_riqi,num_done,num_todo])

        close_mysql(conn, cur)

def scrap_foreign(batch_ID,batch_riqi, scraper):
    error_num = 0
    ip_proxy = wash_proxy()['http']
    while 1:
        # 提取出等待抓取的网址
        [conn, cur] = set_mysql('IFC')
        scrap_riqi = batch_riqi
        #scrap_riqi = datetime.datetime.now()
        #scrap_riqi = scrap_riqi.strftime('%Y-%m-%d')
        # 跑完一遍之后，加入is_important的筛选

        num = cur.execute("SELECT ID,city_from,city_to FROM otc_xiecheng_flight_city_matrix WHERE "
                          "last_scrap_riqi < %s AND is_important >= 1 AND is_foreign=1 AND state=0 AND scraper=%s ORDER BY RAND() ASC LIMIT 1", (scrap_riqi, scraper))
        if (num == 0):
            print("全部任务抓取完毕")
            break
        results = cur.fetchall()
        close_mysql(conn, cur)
        for r in results:
            ID = r[0]
            city_from = r[1]
            city_to = r[2]

        lock_resource(ID)

        [conn, cur] = set_mysql('IFC')
        cur.execute("select city_name,city_ename from otc_xiecheng_flight_city_foreign where city_code = %s", city_from)
        results = cur.fetchall()

        for r in results:
            city_from_name = r[0]
            city_from_ename = r[1]

        cur.execute("select city_name,city_ename from otc_xiecheng_flight_city_foreign where city_code = %s", city_to)
        results = cur.fetchall()
        for r in results:
            city_to_name = r[0]
            city_to_ename = r[1]

        #检查这两个name是不是都是中国的，比如北京->广州
        num1 = cur.execute("select * from otc_xiecheng_flight_city where city_code = %s", city_from)
        num2 = cur.execute("select * from otc_xiecheng_flight_city where city_code = %s", city_to)
        if(num1>0 and num2>0):
            standard_print("这趟航班属于隐藏在国际航班中的国内航班,已经修正matrix",[city_from,city_to])
            cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET is_foreign=0,state=0 WHERE ID = %s", (ID))
            cur.connection.commit()
            continue

        close_mysql(conn, cur)

        riqi = datetime.datetime.now() + datetime.timedelta(days=1)  # 往前进一天
        riqi = riqi.strftime('%Y-%m-%d')
        url = "http://flights.ctrip.com/international/" + city_from_ename + "-" + city_to_ename + "-" + city_from + "-" + city_to + "?" + riqi + "&y_s"
        standard_print("开始抓取:", [url])


        # 构造浏览器，进行抓取
        while error_num < 10:  # 只有break出去的，才选抓取完毕
            # 构造浏览器
            #browser = get_chrome("",0,0)
            browser = get_phantomJS(ip_proxy)

            try:
                browser.get(url)
                print(browser.title)
                if (len(browser.title.split("机票预订")) > 1):
                    pass
                else:  # 如果连接不了网页，那么title是flights.ctrip.com
                    print("访问被拒绝")
                    ip_proxy = wash_proxy()['http']
                    error_num += 1
                    browser.close()
                    continue
            except:
                print("打开页面超时，更换proxy中")
                ip_proxy = wash_proxy()['http']
                error_num += 1
                browser.close()
                continue

            time.sleep(2)

            try:
                for i in range(0, 10):
                    browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
            except:
                print("操作页面超时，更换proxy中")
                ip_proxy = wash_proxy()['http']
                error_num += 1
                browser.close()
                continue

            pageSource = browser.page_source
            soup = BeautifulSoup(pageSource, "html.parser")
            try:
                result = soup.select("li#base_bd > div.submit_box > p.failure_content")
                error_message = remove_space(result[0].get_text())
                print(error_message)
                if (len(error_message.split("您访问的太快了")) > 1):
                    count = 0
                    error_num += 1
                    # time.sleep(60)
                    print("抓取出现异常，更换proxy中")
                    ip_proxy = wash_proxy()['http']
                    browser.close()
                    continue
            except:
                pass

            try:
                result = soup.select("div.flights-header-box clearfix > h3.flights-header")
                error_message = remove_space(result[0].get_text())
                print(error_message)
                if (len(error_message.split("正在努力为您筛选价格优惠的航班信息")) > 1):
                    count = 0
                    error_num += 1
                    # time.sleep(60)
                    print("抓取出现异常，更换proxy中")
                    ip_proxy = wash_proxy()['http']
                    browser.close()
                    continue
            except:
                pass

            result = soup.select("div.flights-list > div.flight-item")
            count = loop_foreign(result,city_from_name,city_to_name,batch_riqi)
            result = soup.select("div.flights-list > div > div.flight-item")
            count += loop_foreign(result,city_from_name,city_to_name,batch_riqi)
            error_num = 0
            browser.close()
            release_resource(ID)
            break

        if (error_num >= 20):
            print("连续错误20次，停止程序")
            release_resource(ID)
            break

        [conn, cur] = set_mysql('IFC')
        cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET last_scrap_riqi=%s,is_important=%s,state=2 WHERE ID = %s",(scrap_riqi, count, ID))
        cur.connection.commit()
        standard_print("更新最近录入时间:", [scrap_riqi, ID, count])

        # 计算工作量
        cur.execute("SELECT count(*) FROM otc_xiecheng_flight_city_matrix WHERE is_important >=1 AND state=2")
        results = cur.fetchall()
        for r in results:
            num_done = r[0]

        cur.execute("SELECT count(*) FROM otc_xiecheng_flight_city_matrix WHERE is_important >=1 AND state=0")
        results = cur.fetchall()
        for r in results:
            num_todo = r[0]

        cur.execute("UPDATE `otc_xiecheng_flight_batch` SET num_done=%s,num_todo=%s WHERE batch_ID = %s",(num_done, num_todo, batch_ID))
        cur.connection.commit()
        standard_print("更新工作量:", [batch_ID, batch_riqi, num_done, num_todo])

        close_mysql(conn, cur)

if __name__ == '__main__':
    [conn, cur] = set_mysql('IFC')

    #把之前抓到一半的全部归零，报错的时候使用
    num = cur.execute("UPDATE `otc_xiecheng_flight_city_matrix` SET state=0 WHERE state=1")
    cur.connection.commit()
    standard_print("成功归零",[num])

    scraper = sys.argv[1]

    print("本次使用的账户是:"+scraper)

    cur.execute("SELECT batch_ID,batch_riqi FROM otc_xiecheng_flight_batch WHERE 1 ORDER BY batch_ID DESC LIMIT 1")
    results = cur.fetchall()
    close_mysql(conn, cur)
    for r in results:
        batch_ID = r[0]
        batch_riqi = r[1]
        standard_print("本次使用的batch如下:",[batch_ID,batch_riqi])
    if (scraper[0] == "d"):
        scrap_domestic(batch_ID, batch_riqi, scraper)
    elif(scraper[0] == "f"):
        scrap_foreign(batch_ID,batch_riqi, scraper)




