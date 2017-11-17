from bs4 import BeautifulSoup
import requests,queue
import math,re,os
import threading
import time
from urllib import request
import datetime
import random
import sys
sys.path.append('../common/')
from common import *

#这个函数用来更新每个job的详细信息
def get_job_info(company_id,company_name):
    while True:
        [conn,cur] = set_mysql("IFC")
        num = cur.execute("SELECT job_url FROM job_boss WHERE job_url != '' AND tags = '' AND duty = '' ORDER BY RAND() LIMIT 1")
        results = cur.fetchall()
        if (num == 0):
            print("该线程任务结束")
            break

        job_url = ''
        for r in results:
            job_url = r[0]

        url = Base_url + str(job_url) + '?ka=job-7'
        try:
            job_data_source = requests_session.get(url, headers=headers,proxies = proxies).text
            job_soup = BeautifulSoup(job_data_source,'lxml')

            text_sour = job_soup.find_all(attrs=['job-sec'])[0].find(attrs=['text'])
            text = str(text_sour).replace('<br/>','').replace('<div class="text">','').replace('</div>','').replace('\xa0','').strip()
            tag_sour = job_soup.find_all(attrs=['job-tags'])
            tag_re = re.compile(r'<span>(.*?)</span>')

            tag = tag_re.findall(str(tag_sour))[0]

            cur.execute("UPDATE `job_boss` SET `duty`=%s,`tags`=%s WHERE job_url=%s",(text,tag,job_url))
            cur.connection.commit()
            print("数据更新: " + company_name + " | " + tag + " | " + text)
            time.sleep(30 + random.randint(2, 5))
        except BaseException as x:
            print("ERROR x",x,"文章url:",url)

        close_mysql(conn, cur)

def get_info_nest(company_name,company_id,data_source):
    soup = BeautifulSoup(data_source, "lxml")
    jobs_list = soup.find_all(attrs=['job-list'])[0].find_all('li')  # 所有招聘信息

    job_url_re = re.compile(r'<a href="(.*?)" ka="job-(\d*)" target="_blank">')
    job_time_re = re.compile(r'<span class="time">(.*?)</span>')
    job_name_re = re.compile(r'<h3 class="name">(.*?)</h3>')
    job_xinzi_re = re.compile(r'<p class="salary">(.*?)-(.*?)</p>')
    job_other_re = re.compile(r'<p>(.*?)<em class="vline"></em>(.*?)<em class="vline"></em>(.*?)</p>')

    for job in jobs_list:
        job_url = job_url_re.findall(str(job))[0][0]  # 每一页的所有职位url
        job_name = job_name_re.findall(str(job))[0]
        job_time = job_time_re.findall(str(job))[0]
        job_xinzi_di = job_xinzi_re.findall(str(job))[0][0]
        job_xinzi_top = job_xinzi_re.findall(str(job))[0][1]
        job_area = job_other_re.findall(str(job))[0][0]
        job_jingyan = job_other_re.findall(str(job))[0][1]
        job_xueli = job_other_re.findall(str(job))[0][2]

        job_dict = {
            'job_url': job_url,
            'job_name': job_name,
            'job_time': job_time,
            'job_xinzi_di': job_xinzi_di,
            'job_xinzi_top': job_xinzi_top,
            'job_area': job_area,
            'job_jingyan': job_jingyan,
            'job_xueli': job_xueli
        }

        # 处理job_riqi
        job_riqi = job_dict['job_time']
        job_riqi = job_riqi.replace("发布于", "")
        if ((len(job_riqi.split(':')) > 1)):
            job_riqi = datetime.datetime.now()
            job_riqi = job_riqi.strftime('%Y-%m-%d')
        elif ((len(job_riqi.split('昨天')) > 1)):
            job_riqi = datetime.datetime.now() - datetime.timedelta(days=1)
            job_riqi = job_riqi.strftime('%Y-%m-%d')
        else:
            job_riqi = job_riqi.replace("月", "-")
            job_riqi = job_riqi.replace("日", "")
            job_riqi = datetime.datetime.now().strftime('%Y-') + job_riqi

        # 处理wage
        wage1 = job_dict['job_xinzi_di'].replace("K", "000")
        wage1 = int(wage1)

        wage2 = job_dict['job_xinzi_top'].replace("K", "000")
        wage2 = int(wage2)

        [conn, cur] = set_mysql("IFC")

        # 判断截止点，不能用name，因为name里面有些奇怪字符
        num = cur.execute("SELECT * FROM job_boss WHERE job_url = %s", (job_dict['job_url']))
        if (num > 0):
            if (last_scrap_riqi == '0000-00-00'):  # 说明是第一次抓取，不能就这么停了
                print("抓取到重复条目，由于是首次抓取，进程继续: " + job_dict['job_url'])
                continue
            else:
                print("抓取到重复条目，本次抓取结束: " + job_dict['job_url'])
                break

        cur.execute(
            "INSERT INTO `job_boss`(`job_url`,`title`, `wage1`, `wage2`, `location`, `experience`, `education`,`riqi`, `company`,`company_id`,`duty`,`tags`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (job_dict['job_url'], job_dict['job_name'], wage1, wage2, job_dict['job_area'],
             job_dict['job_jingyan'], job_dict['job_xueli'], job_riqi, company_name, company_id, '', ''))
        cur.connection.commit()
        print("数据录入: " + company_name + " | " + job_dict['job_name'] + " | " + job_riqi + " | " + job_dict[
            'job_area'] + " | " + str(wage1) + " | " + str(wage2) + " | " + job_dict['job_xueli'])
        close_mysql(conn, cur)


def get_info(company_id,company_name,last_scrap_riqi):
    Base_url = 'http://www.zhipin.com/'
    page_id = 1
    error_num = 0
    proxy = ':'
    while error_num < 100:#要是连续出错100次，可能就有比较大的问题了
        try:
            print("正在抓取的页码: "+str(page_id))
            error_proxy_num = 0
            while proxy == ':':#反复取proxy，直到proxy不再为空
                if(error_proxy_num > 1000):
                    print("该proxy源已经失效，请更换proxy")
                    return 0

                proxy = get_ip_proxy(requests_session, headers)
                time.sleep(2)
                error_proxy_num += 1

            print("本次使用代理: " + proxy)
            proxies = {"http": proxy, "https": proxy}

            url = Base_url + 'gongsi/'+company_id+'.html?page=' + str(page_id) + '&ka=page-8'
            data_source = requests_session.get(url, headers=headers,timeout=3,proxies = proxies).text
            next_page_re = re.compile(r'<a href="(.*?)" ka="page-next" class="(.*?)"></a>')
            next_page_result = next_page_re.findall(data_source)
            if(len(next_page_result)<1):
                print("只有一页") #如果只有一页，那么不能取next_page[0]
                get_info_nest(company_name,company_id,data_source)
                print("数据录入完毕: " + company_name)
                return 1
            else:
                next_page = next_page_re.findall(data_source)[0]
                if next_page[1] != 'next disabled':
                    get_info_nest(company_name, company_id,data_source)
                else:
                    print("数据录入完毕: "+company_name)
                    return 1
                page_id += 1
            time.sleep(5+random.randint(2,5))
            error_num = 0

        except:
            error_num += 1
            proxy = ':' # 重新抓proxy
            print("数据录入失败，连续失败次数为: "+str(error_num))

    return 0 #连续出错，退出



if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # 该程序所在目录
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    headers = {'User-Agent': user_agent,
               'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
               'Cache-Control':'max-age=0',
               'Accept-Encoding':'zip, deflate, sdch',
               'Accept-Language':'zh-CN,zh;q=0.8',
               'Connection':'keep-alive',
               'Upgrade-Insecure-Requests':'1'
               }
    Base_url = 'http://www.zhipin.com/'
    requests_session = requests.session()

    [conn, cur] = set_mysql("IFC")
    riqi = datetime.datetime.now() + datetime.timedelta(days=-7)  # 每周抓一次
    riqi = riqi.strftime('%Y-%m-%d')
    num = cur.execute("SELECT company_id,company_name,ID,last_scrap_riqi FROM job_catalog_boss WHERE last_scrap_riqi < %s", (riqi))
    results = cur.fetchall()
    close_mysql(conn, cur)
    if (num == 0):
        print("全部任务抓取完毕")
    else:
        for r in results:
            print("开始抓取: "+r[1])

            last_scrap_riqi = ''
            if(r[3] is None):
                last_scrap_riqi = '0000-00-00'

            if(get_info(r[0], r[1],last_scrap_riqi)):
                # 更新抓取时间
                riqi = datetime.datetime.now().strftime('%Y-%m-%d')
                [conn, cur] = set_mysql("IFC")
                cur.execute("UPDATE `job_catalog_boss` SET last_scrap_riqi=%s WHERE ID = %s", (riqi, r[2]))
                cur.connection.commit()
                print("更新最近录入时间: " + riqi + " " + r[1])
                close_mysql(conn, cur)
            else:
                print("连续出错，退出该进程，不更新最近录入时间")

            #get_job_info(r[0], r[1])

