# -*-: encoding: utf-8 -*-

from selenium import webdriver
import time
import re
import requests
from os import environ
from selenium.webdriver.common.keys import Keys
import sys
#sys.path.append('../common/')
from common import *
import random

def store_product(item_title, shop, price, sold_count, comment_count, batch_date, catalog_ID, product_ID):
    [conn, cur] = set_mysql("IFC")
    cur.execute("INSERT INTO `ec_tmall_value_dynamic`(`batch_date`, `catalog_ID`, `product_ID`, `title`, "
                "`price`, `volume`, `comment`, `shop`, `popularity_detail`, `rating_detail`, `rating_list_detail`) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(batch_date,catalog_ID,product_ID,item_title,
                price,sold_count,comment_count,shop,-1,-1,""))
    cur.connection.commit()
    standard_print("数据录入", [item_title, shop, price, sold_count, comment_count, batch_date, catalog_ID, product_ID])
    close_mysql(conn, cur)

def get_product(browser, item_url, catalog_ID, batch_date):
    #需要检查ip是否失效
    try:
        print(item_url)
        browser.get(item_url)
        time.sleep(2)
    except:
        return 0

    [conn, cur] = set_mysql("IFC")
    while True:
        print(browser.title)
        if (browser.title == "错误: 不能获取请求的 URL" or browser.title == "list.tmall.com" or browser.title == "理想生活上天猫"):
            return 0

        products_obj = browser.find_elements_by_class_name("product  ")
        for product_obj in products_obj:
            product_a_obj = product_obj.find_elements_by_tag_name("a")[0]
            product_url = product_a_obj.get_attribute('href')
            product_ID = str(url_2_dict(product_url)['id'])
            product_content = product_obj.text
            product_items = product_content.split('\n')

            price = ""
            sold_count = ""
            comment_count = ""
            item_title = ""
            shop = ""

            try:
                if(product_items[0] == "<" and product_items[1] == ">"):
                    item_title = product_items[3].strip()
                else:
                    item_title = product_items[1].strip()

                item_title = item_title.replace("'", "\\'")

                for item in product_items:
                    p = re.compile('¥(.+)')
                    flag = p.search(item)
                    if (flag is not None):
                        price = flag.group(1)

                    p = re.compile('月成交 (.+)笔')
                    flag = p.search(item)
                    if (flag is not None):
                        sold_count = flag.group(1)
                        if sold_count.find(u"万") != -1:
                            sold_count = sold_count.replace("万","")
                            sold_count = float(sold_count)*10000

                    p = re.compile('评价 (.+)')
                    flag = p.search(item)
                    if (flag is not None):
                        comment_count = flag.group(1)
                        if comment_count.find(u"万") != -1:
                            comment_count = comment_count.replace("万", "")
                            comment_count = float(comment_count) * 10000

                    shop_list = ["店","专营","超市","旗舰","专卖","直营","阿里健康"]
                    if(str_contain_list(item, shop_list)):
                        shop = item

            except:
                print("数据采集出错，产品数据为 " + str(product_items))

            if(shop == ''):
                print("缺失门店数据 " + str(product_items))

            if ((price == '') or (sold_count == '')):
                print("该商品没有价格数据或没有销量数据，已跳过")
                continue

            #try_repeat(5,store_product,[tem_title, shop, price, sold_count, comment_count, batch_date, catalog_ID, product_ID])
            store_product(item_title, shop, price, sold_count, comment_count, batch_date, catalog_ID, product_ID)

        # 点击下一页
        if (click_next_page(browser, "//a[@class='ui-page-s-next']", 3) == 0):
            break
    close_mysql(conn, cur)
    return 1


if __name__ == '__main__':
    batch_name = "天猫-全站扫描"
    scraper = sys.argv[1]
    print("本次使用的账户是:" + scraper)
    #browser = get_chrome(wash_proxy()['http'], 1, 1)
    browser = get_phantomJS(wash_proxy()['http'])
    while 1:
        #第一步，取出batch对象
        [stop_loop, batch_date, catelog_tablename] = get_batch(batch_name)
        if (stop_loop == 1):#说明全部batch执行完毕
            break

        #第二步，取出catalog对象
        [conn, cur] = set_mysql("IFC")
        num = cur.execute("SELECT ID,url,category1,category2,brand FROM ec_tmall_catalog WHERE scraper=%s AND state=0 ORDER BY RAND() LIMIT 1",(scraper))
        if (num == 0):
            cur.execute("UPDATE `batch` SET `last_batch_date`=`batch_date` WHERE batch_name=%s", (batch_name))
            cur.connection.commit()
            cur.execute("UPDATE `ec_tmall_catalog` SET `state`=0 WHERE scraper=%s", (scraper))
            cur.connection.commit()
            standard_print("该采集账户执行完毕", [batch_name,scraper])
            browser.close()
            break

        result = cur.fetchall()
        for r in result:
            catalog_ID = r[0]
            url = r[1]
            label = r[2]+"-"+r[3]+"-"+r[4]
        close_mysql(conn, cur)
        standard_print("开始采集",[catalog_ID,label,url])

        #第三步，执行数据采集
        while 1:
            # 删除由于ip原因没有成功抓取的内容
            [conn, cur] = set_mysql("IFC")
            num = cur.execute("DELETE FROM `ec_tmall_value_dynamic` WHERE catalog_ID=%s AND batch_date=%s",(catalog_ID,batch_date))
            cur.connection.commit()
            close_mysql(conn, cur)
            if (num > 0):
                print("删除由于ip原因未完成抓取的条目: " + str(num) + "条")

            is_success = get_product(browser, url, catalog_ID, batch_date)
            if (is_success == 1):
                break
            else:
                print("ip出错，正在更换ip")
                browser.close()
                #browser = get_chrome(wash_proxy()['http'], 1, 1)
                browser = get_phantomJS(wash_proxy()['http'])

        #第四步，更新catalog及batch表情况
        [conn, cur] = set_mysql("IFC")
        cur.execute("UPDATE `ec_tmall_catalog` SET `state`=1 WHERE ID=%s", (catalog_ID))
        cur.connection.commit()
        standard_print("该品类采集完毕", [label])

        cur.execute("SELECT count(*) FROM ec_tmall_catalog WHERE state=1")
        results = cur.fetchall()
        for r in results:
            num_done = r[0]

        cur.execute("SELECT count(*) FROM ec_tmall_catalog WHERE state=0")
        results = cur.fetchall()
        for r in results:
            num_todo = r[0]

        cur.execute("UPDATE `batch` SET num_done=%s,num_todo=%s WHERE batch_name = %s",(num_done, num_todo, batch_name))
        cur.connection.commit()
        standard_print("更新工作量:", [batch_name, num_done, num_todo])
        close_mysql(conn, cur)