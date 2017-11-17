#  http://36kr.com/
import requests,re,time,math
import threading,queue
import json,os
from urllib import request
from random import choice
import pymysql
import sys
sys.path.append('../common/')
from common import *

now_time = time.strftime("%Y-%m-%d", time.localtime())
hour = int(time.strftime("%H", time.localtime()))
minute = int(time.strftime("%M", time.localtime()))

def href():
    flag = 1
    for n in range(1, pages):
        Base_url = 'http://36kr.com/api/search/articles/%20?page='+str(n)+'&pageSize=40&ts='+str(int(time.time()))
        try:
            articles = request_session.get(Base_url, headers=headers, timeout=3, proxies=get_proxies(0)).json()
            datalist = articles['data']['data']
            count = 0
            for post in datalist:
                q.put(post['id'])
                flag += 1
                count += 1
                #print(flag)
            print('fetched page %d with %d articles' % (n, count))
        except BaseException as g:
            print("ERROR:",g)

    print("共计%s篇文章~" % flag)

def page(name):
    flag = 1
    while True:
        if not q.empty():
            flag = 1
            try:

                id = q.get()
                url = 'http://36kr.com/p/' + str(id) + '.html'
                # print(url)
                content = request_session.get(url, headers=headers, timeout=3).text
                #content = request_session.get(url, headers=headers,timeout=3, proxies=get_proxies()).text
                props = re.findall(r'\"detailArticle\|post\":(.*?),\"abTest\|abtest\"', content)
                if props and props[0]:    #跳过付费才能看的文章
                    post_data = json.loads(props[0])
                    #print(post_data)
                    time1 = post_data['created_at']
                    day_time = time1.split()[0]   #年月日
                    hour_time = time1.split()[1].split(':')[0]  #小时
                    minute_time = time1.split()[1].split(':')[1]#分钟
                    page_tag = post_data['extraction_tags']
                    tag_list = []
                    page_tag1 = page_tag.split(',')
                    len1 = len(page_tag1)
                    j = 0
                    while j < len1:
                        tag_list.append(page_tag1[j].strip("["))
                        j += 3

                    #先判重
                    if(exist_news(url)):
                        print("链接重复，本次抓取结束！")
                        q.queue.clear()
                        return
                        #pass
                    else:

                        #下载图片到本地并且替换链接
                        content = post_data['content']
                        pict_sour = str(post_data['content'])
                        picture_re = re.compile(r'src="https://(.*?)" ')
                        picture_list = picture_re.findall(pict_sour)
                        if len(picture_list) != 0:
                            os.mkdir(BASE_DIR +"/news_36Kr_images/" + str(id))
                            try:
                                page = 1
                                for pic in picture_list:
                                    image_src = pic
                                    image_dir= '/news_36Kr_images/%s/%s.jpg' % (str(id),page)
                                    #request.urlretrieve('http://' + pic, BASE_DIR + image_dir)
                                    download_file('http://' + pic, BASE_DIR + image_dir,0)
                                    content = content.replace('http://'+image_src,image_dir)
                                    content = content.replace('https://' + image_src, image_dir)
                                    page += 1
                            except BaseException as p:
                                continue
                        page_dict = {
                        'title':post_data['title'],                           # 标题
                        'riqi' :time1,                                         # 日期
                        'tag' : post_data['column']['name'],                 # 标签，时间后面那个
                        'author' : post_data['user']['name'],                #作者
                        'content' :content,                                   #正文
                        'summary' : post_data['summary'],                    # 梗概
                        'extraction_tags' :tag_list,                         # 文章下面的标签,
                        'like' : post_data['counters']['like'],             # 点赞数,
                        'comment'  : post_data['counters']['comment'],     #评论数,
                        'page_href' :  url ,                                  # 链接,
                        }

                        tag_list = str(tag_list)
                        tag_list = tag_list[1:-1]
                        tag_list = tag_list.replace('"','')
                        tag_list = tag_list.replace("'", "")
                        pattern = re.compile('\s')
                        tag_list = re.sub(pattern, '', tag_list)

                        try:
                            answer = get_category(post_data['title'],tag_list)
                            category = answer['category']
                            title_fenci = answer['title']

                            if(len(category)==0):
                                category.append("科技")
                        except BaseException as e:
                            print(e)


                        try:
                            print("关键词为: %s | %s" % (title_fenci, tag_list))
                            store_news(category,time1,post_data['title'],post_data['summary'],content,-1,post_data['counters']['like'],post_data['counters']['comment'],'36氪',url,post_data['user']['name'],tag_list)
                        except BaseException as e:
                            print(e)

                    #time.sleep(10)
            except BaseException as e:
                ERROR_list.append(id)
                continue
            time.sleep(requests_interval * 3)
        else:
            if flag < 180:  # 队列空就等一秒，如果等待3分钟后，队列还是空，视所有文章已抓取完毕，退出程序
                #print("队列中无数据")
                time.sleep(1)
                flag += 1
                #print("等待%s秒" % flag)
            else:
                return "All work is Done!"



if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # 该程序所在目录
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    headers = {'User-Agent': user_agent}
    request_session = requests.Session()
    Base_url = 'http://36kr.com/api/search/articles/%20?page=1&pageSize=40&ts=' + str(int(time.time()))

    articles = request_session.get(Base_url, headers=headers).json()
    #articles = request_session.get(Base_url, headers=headers, proxies=get_proxies()).json()
    data = articles['data']
    total_count = data['total_count']
    page_size = data['page_size']
    pages = math.ceil(total_count / page_size) + 1
    ERROR_list = []
    print('%d pages' % pages)

    if not os.path.exists(BASE_DIR + '/news_36Kr_images'):
        os.mkdir(BASE_DIR + '/news_36Kr_images')

    q = queue.Queue(maxsize=10)

    href = threading.Thread(target=href)
    href.start()

    t_list = []
    for i in range(1):#开几个线程
        get_user1 = threading.Thread(target=page,args=(i,))
        get_user1.start()
        t_list.append(get_user1)

    for t in t_list:
        t.join()
    os.kill(os.getpid(), 9)#没办法，否则进程不终止


