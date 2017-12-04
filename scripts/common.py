# -*-: encoding: utf-8 -*-
import pymysql
import re
import urllib
import random
import requests
from selenium import webdriver
import socket
import datetime
import json
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.proxy import ProxyType

#分词
import jieba
from operator import itemgetter
import jieba.posseg as pseg

requests_interval = 1

"""
用途: 用于建立数据库连接
"""
def set_mysql(db):
    error_num = 0
    while error_num < 10:
        try:
            conn = pymysql.connect(host='bj-cdb-a40358la.sql.tencentcdb.com', port=63991, user='root', passwd='76&6Ttz3=q7S', db=db, charset='utf8')
            cur = conn.cursor()
            break
        except:
            time.sleep(2)
            error_num += 1
            pass
    return [conn,cur]

"""
用途: 用于关闭数据库连接
"""
def close_mysql(conn, cur):
    error_num = 0
    while error_num < 10:
        try:
            cur.close()
            conn.close()
            break
        except:
            time.sleep(2)
            error_num += 1
            pass

def set_mysql2(db):
    error_num = 0
    while error_num < 10:
        try:
            conn = pymysql.connect(host='5906e72cd13a2.bj.cdb.myqcloud.com', port=9237, user='root', passwd='fbjjiayou123', db=db, charset='utf8')
            cur = conn.cursor()
            break
        except:
            time.sleep(2)
            error_num += 1
            pass
    return [conn, cur]

"""
用途: 让模拟浏览器一小步一小步向下滚动，以加载出想要抓取的元素
"""
def browser_scroll_down_step(browser):
    browser.execute_script("""
        (function () {
          var y = 0;
          var step = 100;
          window.scroll(0, 0);

          function f() {
            if (y < document.body.scrollHeight) {
              y += step;
              window.scroll(0, y);
              setTimeout(f, 100);
            } else {
              window.scroll(0, 0);
              document.title += "scroll-done";
            }
          }

          setTimeout(f, 1000);
        })();
      """)

"""
用途: 模拟生成密码字符串，用来批量注册账户
"""
def get_random_password():
    password_length = random.randint(8,15)
    password = random.sample('zyxwvutsrqponmlkjihgfedcba0123456789.+-', password_length)
    password = "".join(password)
    return password

"""
用途: 提取可用ip资源
"""
def get_ip_proxy(request_session,headers):
    [conn,cur] = set_mysql("platform")
    ip = ""
    port = ""

    cur.execute("SELECT provider,spider_id,order_no FROM ip_proxy WHERE 1 ORDER BY ID DESC") #不能加limit 1
    results = cur.fetchall()
    for r in results:
        provider = r[0]
        spider_id = r[1]
        order_no = r[2]
        if(provider == '讯代理'):
            try:
                ip_url = 'http://api.xdaili.cn/xdaili-api//greatRecharge/getGreatIp?spiderId=%s&orderno=%s&returnType=2&count=1' % (spider_id, order_no)
                ip_proxies = request_session.get(ip_url, headers=headers).json()
                results = ip_proxies['RESULT']
                for result in results:
                    ip = result['ip']
                    port = result['port']
            except:
                continue #这个ip订单没有余额了，用下一个
    close_mysql(conn, cur)
    return str(ip)+":"+str(port)

"""
用途: 提取ip对应的物理地址以及运营商类型
"""
def get_ip_address(ip_proxy):
    try:
        browser = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true'])
        if(len(ip_proxy.split(":"))>1):
            browser.get("http://ip.chinaz.com/" + ip_proxy.split(":")[0])
        else:#说明ip地址后面没有带有port
            browser.get("http://ip.chinaz.com/" + ip_proxy)

        ip_para = browser.find_element_by_xpath('//*[@id="leftinfo"]/div[3]/div[2]/p[2]/span[4]').text
        ip_address = ip_para.split(" ")[0]
        ip_ISP = ip_para.split(" ")[1]
        browser.close()
    except:
        ip_address = ""
        ip_ISP = ""
        print("无法获取ip所在地")
    return {'ip_address': ip_address, 'ip_ISP': ip_ISP}

"""
用途: ip提取不稳定，这时候需要用wash_proxy函数，保证每次都能提取到ip
"""
def wash_proxy():
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    headers = {'User-Agent': user_agent}
    request_session = requests.Session()

    error_proxy_num = 0
    proxy = ':'
    while proxy == ':':  # 反复取proxy，直到proxy不再为空
        if (error_proxy_num > 1000):
            print("该proxy源已经失效，请更换proxy")
            break
        proxy = get_ip_proxy(request_session, headers)
        error_proxy_num += 1
        time.sleep(2)
    proxies = {"http": proxy, "https": proxy}
    print("本次使用代理: " + proxy)
    return proxies

"""
用途: 对文本进行分词处理
"""
def fenci(text):
    wg = jieba.cut(text, cut_all=False)
    wd = {}
    nonsense = [
        u"装置", u"设备", u"内容", u"系统", u"无标题",u"方法",u"文献",u"事件",u"任务",u"信息",
    ]
    answer = list()
    for w in wg:
        if len(w) < 2:
            continue
        elif w in nonsense:
            continue
        else:
            answer.append(w)
    return answer

"""
用途: 去除字符串中的全部空格
"""
def remove_space(str):
    str = str.replace('/n', '')
    pattern = re.compile('\s')
    str = re.sub(pattern, '', str)
    pattern = re.compile('	')  # nbsp
    str = re.sub(pattern, '', str)
    return str

"""
用途: 新闻资讯模块，为新闻打标签
"""
def get_category(title,tag):
    category = []
    titles = fenci(title)
    title = ""
    for t in titles:
        title = title+" "+t
    [conn,cur] = set_mysql('IFC')
    cur.execute("select keyword,category from news_keyword WHERE 1")
    results = cur.fetchall()
    for r in results:
        keyword = r[0]
        if(len(title.split(keyword)) > 1 or len(tag.split(keyword)) > 1):
            category.append(r[1])
    close_mysql(conn, cur)
    return {'category': category,'title':title}

"""
用途: 判断新闻条目是否已经存在
"""
def exist_news(link):
    [conn,cur] = set_mysql('IFC')
    num = cur.execute("SELECT * FROM NEWS WHERE link= %s", link)
    close_mysql(conn, cur)
    return num > 0

"""
用途: 获取行业名称对应的industry_id
"""
def get_CID(name):
    [conn, cur] = set_mysql('IFC')
    cur.execute("select industry_id from INDUSTRY where name=%s", (name))
    results = cur.fetchall()
    close_mysql(conn, cur)
    for r in results:
        CID = r[0]
    return CID

"""
用途: 获取industry_id对应的行业名称
"""
def get_industry(CID):
    [conn, cur] = set_mysql('IFC')
    cur.execute("select name from INDUSTRY where industry_id=%s", (CID))
    results = cur.fetchall()
    close_mysql(conn, cur)
    for r in results:
        name = r[0]
    return name

"""
用途: 存储新闻条目
"""
def store_news(category,riqi, title, abstract, content, num_read, num_like, num_comment, source, link, author, tags):
    last_ID = 0
    [conn, cur] = set_mysql('IFC')
    if not exist_news(link):
        cur.execute("INSERT INTO `NEWS`(`riqi`, `title`, `abstract`, `content`, `num_read`, `num_like`, `num_comment`, `source`, `link`, `author`, `tags`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (riqi, title, abstract, content, num_read, num_like, num_comment, source, link, author, tags))
        cur.connection.commit()
        print("saved news " + riqi + " " + title)
        cur.execute("SELECT LAST_INSERT_ID()")
        results = cur.fetchall()
        for r in results:
            last_ID = r[0]
        store_cats(last_ID, category)
        print('----------------------')
    close_mysql(conn, cur)

"""
用途: 存储新闻条目的标签们
"""
def store_cats(NID,category):
    [conn, cur] = set_mysql('IFC')
    cats = ""
    for cat in category:
        CID = get_CID(cat)
        num = cur.execute("SELECT * FROM NEWS_INDUSTRY_RELATION WHERE news_id= %s and industry_id=%s", (NID,CID))
        if num==0:
            cur.execute("INSERT INTO `NEWS_INDUSTRY_RELATION`(`news_id`, `industry_id`) VALUES (%s,%s)",(NID,CID))
            cur.connection.commit()
            cats = cats+" "+cat
    print("saved relation " + cats)
    close_mysql(conn, cur)

"""
用途: 下载文件
"""
def download_file(url, file,flag):
    # create the object, assign it to a variable
    proxy = urllib.request.ProxyHandler(get_proxies(flag))
    # construct a new opener using your proxy settings
    opener = urllib.request.build_opener(proxy)
    # install the openen on the module-level
    urllib.request.install_opener(opener)
    # make a request
    urllib.request.urlretrieve(url, file)
    print('图片下载: %s -> %s' % (url, file))

"""
用途: 下载文件2
"""
def download_file2(url, file_path):
    # local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return True

"""
用途: 提取出url中的各项参数
"""
def url_2_dict(url):
    query = urllib.parse.urlparse(url).query
    return dict([(k, v[0]) for k, v in urllib.parse.parse_qs(query).items()])

"""
用途: 标准化的输出方式
"""
def standard_print(prefix,attrs):
    output = prefix
    for attr in attrs:
        output = output+" | "+str(attr)
    print(output)

"""
用途: 把None变量进行转化
"""
def parse_none(attr,attr_type):
    if (attr is None):
        if(attr_type == 'str'):
            attr = ""
        elif (attr_type == 'int'):
            attr = 0
        elif (attr_type == 'float'):
            attr = 0.0
    return attr

"""
用途: 这几个batch_task先忽略
"""
def batch_task1(db,table):
    myname = socket.getfqdn(socket.gethostname())
    myaddr = socket.gethostbyname(myname)
    username = ''
    password = ''

    standard_print("本台服务器的配置参数为",[myname,myaddr])

    [conn, cur] = set_mysql(db)
    query = "SELECT username,password FROM %s WHERE host = %%s" % table
    cur.execute(query, (myname))
    results = cur.fetchall()
    for r in results:
        username = r[0]
        password = r[1]
    close_mysql(conn, cur)
    print("采用以下用户名登陆抓取: " + username + " " + password + " " + myname)
    return [username,password]

def batch_task2(db,table_batch,username):
    # 获取待执行批次，保证每个batch用同一个batch_riqi
    stop_loop = 0
    riqi = datetime.datetime.now()
    riqi = riqi.strftime('%Y-%m-%d')
    [conn, cur] = set_mysql(db)
    query = "SELECT batch_name, batch_ID,batch_state,current_scrap_riqi,final_target_ID FROM %s WHERE (last_scrap_riqi = '0000-00-00' OR DATE_ADD(last_scrap_riqi,INTERVAL batch_interval DAY) <= %%s) AND batch_state!=2 AND username=%%s ORDER BY RAND() LIMIT 1" % table_batch
    num = cur.execute(query, (riqi, username))
    close_mysql(conn, cur)
    if (num == 0):
        [conn, cur] = set_mysql(db)
        query = "UPDATE %s SET `batch_state`=0,`current_scrap_riqi`='0000-00-00' WHERE username = %%s" % table_batch
        cur.execute(query, (username))
        cur.connection.commit()
        close_mysql(conn, cur)
        print("全部批次执行完毕")
        stop_loop = 1
        return [stop_loop, "", "", "", "", "", ""]

    # 到这里说明有批次需要执行
    batch_name = ""
    batch_ID = ""
    batch_state = ""
    current_scrap_riqi = ""
    final_target_ID = ""
    results = cur.fetchall()
    for r in results:
        batch_name = r[0]
        batch_ID = r[1]
        batch_state = r[2]
        current_scrap_riqi = r[3]
        final_target_ID = r[4]

    # 如果该批次之前已经在执行过程中了，那么batch_riqi就用之前的current_scrap_riqi；如果是新开启的批次，那么就用最新的日期
    batch_riqi = ""
    if (batch_state == 1):
        batch_riqi = str(current_scrap_riqi)
    elif (batch_state == 0):
        batch_riqi = riqi
    print("本批次的批次时间戳为: " + str(batch_riqi))

    # 拿到该批次的执行情况后，开始更新批次表
    [conn, cur] = set_mysql(db)
    query = "UPDATE %s SET `batch_state`=1, `current_scrap_riqi`=%%s WHERE batch_ID = %%s" % table_batch
    cur.execute(query, (batch_riqi, batch_ID))
    cur.connection.commit()
    close_mysql(conn, cur)
    print("开始执行批次: " + str(batch_riqi) + " | " + batch_name + " | " + str(batch_ID))
    return [stop_loop, batch_name, batch_ID, batch_riqi, batch_state, current_scrap_riqi, final_target_ID]

def batch_task3(db,table_batch,table_target,batch_ID,batch_riqi):
    # 并对批次中的任务列表逐条执行
    stop_loop = 0
    [conn, cur] = set_mysql(db)
    query = "SELECT target,url,target_ID FROM %s WHERE target_state!=2 AND batch_ID=%%s ORDER BY RAND() LIMIT 1" % table_target
    num = cur.execute(query, (batch_ID))
    close_mysql(conn, cur)
    if (num == 0):
        # 这里说明这个batch执行完毕，需要对batch表进行操作
        [conn, cur] = set_mysql(db)
        query = "UPDATE %s SET `last_scrap_riqi`=%%s, `current_scrap_riqi`='0000-00-00',`batch_state`=2 WHERE batch_ID = %%s" % table_batch
        cur.execute(query, (batch_riqi, batch_ID))
        cur.connection.commit()
        query = "UPDATE %s SET `target_state`=0 WHERE batch_ID = %%s AND target_state=2" % table_target
        cur.execute(query, (batch_ID))
        cur.connection.commit()
        close_mysql(conn, cur)
        print("该批次下的任务全部执行完毕")
        stop_loop = 1
        return [stop_loop, "","",""]

    # 到这里说明该批次下存在任务没有执行完毕，获取一个任务
    target = ""
    url = ""
    target_ID = ""
    results = cur.fetchall()
    for r in results:
        target = r[0]
        url = r[1]
        target_ID = r[2]
    return [stop_loop, target, url, target_ID]

def batch_task4(db,table_values,target,target_ID,batch_riqi):
    # 首先删除之前执行到一半退出的未完成任务的残存数据
    [conn, cur] = set_mysql(db)
    num = 0
    for table_value in table_values:
        query = "DELETE FROM %s WHERE target_ID = %%s AND riqi = %%s" % table_value
        num += cur.execute(query, (target_ID, batch_riqi))
        cur.connection.commit()
    close_mysql(conn, cur)
    print("删除该批次中该任务未完成抓取的数据: " + target + " | " + str(num) + "条")

def batch_task5(db,table_target,target,target_ID,url):
    # 下面开始正式抓取
    [conn, cur] = set_mysql(db)
    query = "UPDATE %s SET `target_state`=1 WHERE target_ID = %%s" % table_target
    cur.execute(query, (target_ID))
    cur.connection.commit()
    print("开始执行任务: " + target + " " + url)
    close_mysql(conn, cur)

def batch_task6(db,table_target,target,target_ID):
    # 释放资源并更新时间
    [conn, cur] = set_mysql(db)
    query = "UPDATE %s SET `target_state`=2 WHERE target_ID = %%s" % table_target
    cur.execute(query, (target_ID))
    cur.connection.commit()
    print("该任务执行完毕: " + target)
    close_mysql(conn, cur)

"""
用途: 获取地理位置的经纬度数据
"""
def get_lng_lat(query, region):
    lng = ''
    lat = ''
    Map = requests.get(
        'http://api.map.baidu.com/place/v2/suggestion?query={0}&region={1}&city_limit=true&output=json&ak=30LrNPyjEx2EDQGfWOtPNhCe'.format(
            query, region))
    JJ = json.loads(Map.content.decode('utf8'))
    for lat_lng in JJ['result']:
        lat = lat_lng['location']['lat']
        lng = lat_lng['location']['lng']
        break
    return {'lng': lng, 'lat': lat}

"""
用途: 重复多次执行同一个操作，直到操作成功，适合mysql操作
"""
def try_repeat(repeat_num,func,args):
    for try_i in range(repeat_num):
        try:
            func(args)
            break
        except:
            time.sleep(10 + random.randint(2, 5))
            pass

"""
用途: 从timestamp转化成datetime
举例: print(timestamp_datetime(1420114800))
"""
def timestamp_datetime(ts):
    if isinstance(ts, (int, float, str)):
        try:
            ts = int(ts)
        except ValueError:
            raise

        if len(str(ts)) == 13:
            ts = int(ts / 1000)
        if len(str(ts)) != 10:
            raise ValueError
    else:
        raise ValueError()

    return datetime.datetime.fromtimestamp(ts)

"""
用途: 从datetime转化成timestamp
举例: print(datetime_timestamp('2015-01-01 20:20:00', 's'))
"""
def datetime_timestamp(dt, type='ms'):
    if isinstance(dt, str):
        try:
            if len(dt) == 10:
                dt = datetime.datetime.strptime(dt.replace('/', '-'), '%Y-%m-%d')
            elif len(dt) == 19:
                dt = datetime.datetime.strptime(dt.replace('/', '-'), '%Y-%m-%d %H:%M:%S')
            else:
                raise ValueError()
        except ValueError as e:
            raise ValueError(
                "{0} is not supported datetime format." \
                "dt Format example: 'yyyy-mm-dd' or yyyy-mm-dd HH:MM:SS".format(dt)
            )

    if isinstance(dt, time.struct_time):
        dt = datetime.datetime.strptime(time.stftime('%Y-%m-%d %H:%M:%S', dt), '%Y-%m-%d %H:%M:%S')

    if isinstance(dt, datetime.datetime):
        if type == 'ms':
            ts = int(dt.timestamp()) * 1000
        else:
            ts = int(dt.timestamp())
    else:
        raise ValueError(
            "dt type not supported. dt Format example: 'yyyy-mm-dd' or yyyy-mm-dd HH:MM:SS"
        )
    return ts

"""
用途: 获取phantomJS无头浏览器
"""
def get_phantomJS(ip_proxy):
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    headers = {'User-Agent': user_agent}
    request_session = requests.Session()

    desired_capabilities = DesiredCapabilities.PHANTOMJS.copy()
    # 从USER_AGENTS列表中随机选一个浏览器头，伪装浏览器
    desired_capabilities["phantomjs.page.settings.userAgent"] = (user_agent)
    # 不载入图片，爬页面速度会快很多
    desired_capabilities["phantomjs.page.settings.loadImages"] = False
    # 利用DesiredCapabilities(代理设置)参数值，重新打开一个sessionId，我看意思就相当于浏览器清空缓存后，加上代理重新访问一次url
    if(ip_proxy != ""):
        proxy = webdriver.Proxy()
        proxy.proxy_type = ProxyType.MANUAL
        proxy.http_proxy = ip_proxy
        proxy.add_to_capabilities(desired_capabilities)
    # 打开带配置信息的phantomJS浏览器
    browser = webdriver.PhantomJS(desired_capabilities=desired_capabilities)
    # browser = webdriver.PhantomJS(executable_path=phantomjs_driver)
    browser.start_session(desired_capabilities)
    # 隐式等待5秒，可以自己调节
    browser.implicitly_wait(5)
    # 设置10秒页面超时返回，类似于requests.get()的timeout选项，driver.get()没有timeout选项
    # 以前遇到过driver.get(url)一直不返回，但也不报错的问题，这时程序会卡住，设置超时选项能解决这个问题。
    browser.set_page_load_timeout(20)
    # 设置10秒脚本超时时间
    browser.set_script_timeout(20)
    browser.maximize_window()

    return browser

"""
用途: 获取chrome浏览器
参数：is_secret表示是否进入隐身模式，is_maximize表示是否最大化浏览器
"""
def get_chrome(ip_proxy,is_secret,is_maximize):
    chromedriver = "chromedriver.exe"
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--proxy-server=%s' % ip_proxy)
    # 更换头部
    #chrome_options.add_argument('user-agent=%s' % user_agent)
    #chrome_options.add_argument('host=%s' % host)
    #chrome_options.add_argument('origin=%s' % origin)

    #最大化浏览器
    if(is_maximize == 1):
        chrome_options.add_argument("start-maximized")

    #图片不加载
    prefs = {
        'profile.default_content_setting_values': {
            'images': 2
        }
    }
    chrome_options.add_experimental_option('prefs', prefs)

    if(is_secret == 1):
        chrome_options.add_argument("--incognito")  # 隐藏模式登陆
    browser = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)
    browser.set_page_load_timeout(60)
    browser.set_script_timeout(60)  # 这两种设置都进行才有效
    return browser

"""
用途: 判断str1是否包含str2
"""
def str_contain(str1,str2):
    if (len(str1.split(str2)) > 1):
        return 1
    else:
        return 0

"""
用途: 判断list是否包含str2
"""
def str_contain_list(str1,list):
    is_contain = 0
    for str2 in list:
        is_contain = str_contain(str1,str2)
        if(is_contain == 1):
            break
    return is_contain

"""
用途: 判断str是否包含list里的内容
"""
def list_contain_str(str,list):
    is_contain = 0
    for item in list:
        if(item in str):
            is_contain = 1
            break
    return is_contain

"""
用途: 点击下一页
"""
def click_next_page(browser,xpath,try_num):
    print("翻页中")
    click_ok = 0
    for try_i in range(try_num):
        try:
            next_page_obj = browser.find_element_by_xpath(xpath)
            next_page_obj.click()
            time.sleep(5 + random.randint(0,3))
            click_ok = 1
            break
        except:
            pass
    return click_ok

"""
用途: 从url获取json格式的数据输出
"""
def get_json_api(url):
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36'
    headers = {'User-Agent': user_agent}
    request_session = requests.Session()
    json_response = request_session.get(url,headers=headers).text  # 必须用.text，不能用.content,否则会报错：the JSON object must be str, not 'bytes'
    json_response = json.loads(json_response)  # 这里要用loads，不能用load
    return json_response

"""
用途: 按数据单位进行数据调整
"""
def wash_num(num_str):
    num = ""
    if(num == ""):
        if (len(num_str.split("千")) > 1):
            num = float(remove_space(num_str.split("千")[0])) * 1000
    if (num == ""):
        if (len(num_str.split("万")) > 1):
            num = float(remove_space(num_str.split("万")[0])) * 10000
    if (num == ""):
        num = num_str
    return num

"""
用途: 把list转化成str
"""
def wash_list(tag_list):
    tag_list = str(tag_list)
    tag_list = tag_list[1:-1]
    tag_list = tag_list.replace('"', '')
    tag_list = tag_list.replace("'", "")
    pattern = re.compile('\s')
    tag_list = re.sub(pattern, '', tag_list)
    return tag_list

"""
用途: 获取list中数值最大的值的index
"""
def get_max_index(list):
    max = 0
    max_index = 0
    for i in range(len(list)):
        if list[i] > max:
            max = list[i]
            max_index = i
    return max_index

"""
用途: 获取list中数值最小的值的index
"""
def get_min_index(list):
    min = 10000000
    min_index = 0
    for i in range(len(list)):
        if list[i] < min:
            min = list[i]
            min_index = i
    return min_index

"""
用途: 获取list的平均值
"""
def get_average(list):
    if(len(list)==0):
        print("数据为空")
        return

    #把list中的所有元素都转成float
    for i in range(0,len(list)):
        list[i] = float(list[i])

    average = sum(list) / float(len(list))
    return average

"""
用途: 把运算分析好的结论存入RESULT表
"""
def store_result(request, riqi, content):
    [conn, cur] = set_mysql('IFC')
    cur.execute("INSERT INTO `result`(`request`, `riqi`, `content`) VALUES (%s,%s,%s)", (request, riqi, str(content)))
    cur.connection.commit()
    standard_print("RESULT录入", [request,riqi,content])
    close_mysql(conn, cur)

"""
用途: 批量构造sql
"""
def sql_add(attr,op,value_array):
    sql = "("
    for i in range(len(value_array)):
        if(i == 0):
            sql = sql + " " + attr + " = '" + value_array[i] + "' "
        else:
            sql = sql + " " + op + " " + attr + " = '" + value_array[i] + "' "
    sql = sql + ")"
    return sql

"""
用途: 对日期进行位移操作
"""
def date_delta(num):
    riqi = datetime.datetime.now() + datetime.timedelta(days=num)  # 往前进num天
    riqi = riqi.strftime('%Y-%m-%d')
    return riqi

"""
用途: 从indicator表中提取出最新数值
"""
def get_indicator_lastest_value(ID_tong):
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT time,value FROM `indicator_value` WHERE ID_tong = %s ORDER BY time DESC LIMIT 1", (ID_tong))
    results = cur.fetchall()
    for r in results:
        riqi = str(r[0])
        value = str(r[1])
    close_mysql(conn, cur)
    return [riqi, value]

"""
用途: 从indicator表中提取出最新比率
"""
def get_indicator_lastest_rate(ID_tong):
    conclusion = ""
    [conn, cur] = set_mysql('IFC')
    cur.execute("SELECT time,value FROM `indicator_value` WHERE ID_tong = %s ORDER BY time DESC LIMIT 1", (ID_tong))
    results = cur.fetchall()
    for r in results:
        riqi = str(r[0])
        value = r[1]
    close_mysql(conn, cur)
    if(value > 0):
        conclusion = "增加"+str(value)+"%"
    else:
        conclusion = "减少" + str(-value) + "%"
    return conclusion

"""
用途: 为任务分配采集账户
"""
def assign_scraper(table_target,column_ID,colume_scraper,scraper_list):
    [conn,cur] = set_mysql('IFC')

    query = "UPDATE %s SET %s='' WHERE 1" % (table_target, colume_scraper)
    cur.execute(query, ())

    query = "SELECT %s FROM %s WHERE %s=''" % (column_ID, table_target, colume_scraper)
    num = cur.execute(query, ())
    if(num == 0):
        print("分配结束")
        return
    else:
        result = cur.fetchall()
        for r in result:
            target_ID = r[0]
            scraper = random.choice(scraper_list)
            query = "UPDATE %s SET %s=%%s WHERE %s=%%s" % (table_target, colume_scraper, column_ID)
            cur.execute(query, (scraper,target_ID))
            cur.connection.commit()
            standard_print("数据更新:", [scraper, target_ID])
    close_mysql(conn, cur)

"""
用途: 获取batch
"""
def get_batch(batch_name):
    # 获取待执行批次，保证每个batch用同一个batch_riqi
    [conn, cur] = set_mysql("IFC")
    riqi = datetime.datetime.now()
    riqi = riqi.strftime('%Y-%m-%d')

    num = cur.execute("SELECT catalog_tablename,num_todo,num_done FROM `batch` WHERE "
                "(last_batch_date = '0000-00-00' OR DATE_ADD(last_batch_date,INTERVAL batch_interval HOUR) <= %s) "
                "AND batch_name=%s AND batch_date<>last_batch_date",(riqi,batch_name))

    if (num == 0):
        cur.execute("UPDATE `batch` SET `last_batch_date`=`batch_date`, num_todo=0, num_done=0 WHERE batch_name=%s", (batch_name))
        cur.connection.commit()
        standard_print("该批次执行完毕",[batch_name])
        return ["", ""]

    result = cur.fetchall()
    for r in result:
        catalog_tablename = r[0]
        num_todo = r[1]
        num_done = r[2]

    # 拿到该批次的执行情况后，开始更新批次表
    if(num_todo == 0 and num_done == 0):#说明是新开启的batch
        cur.execute("UPDATE `batch` SET `batch_date`=%s WHERE batch_name=%s", (riqi, batch_name))
        cur.connection.commit()
    else:#说明是继续上次的batch
        pass
    standard_print("开始执行批次",[batch_name,riqi,catalog_tablename])
    close_mysql(conn, cur)
    return [riqi,catalog_tablename]

"""
用途: 按照正则表达来提取字符串中的内容
参数：num指的是第几个括号
"""
def wash_regex(str, regex, num):
    response = ""
    p = re.compile(regex)
    flag = p.search(str)
    if (flag is not None):
        response = flag.group(num)
    return response

"""
用途: 判断ip是否失效，如果失效的话就换新的ip
"""
def refresh_browser(browser,url,type,error_list):
    while 1:
        try:
            browser.get(url)
            if (browser.title in error_list):
                browser.close()
                time.sleep(5)
                if(type=="chrome"):
                    browser = get_chrome(wash_proxy()['http'], 1, 0)
                elif(type=="phantomJS"):
                    browser = get_phantomJS(wash_proxy()['http'])
                continue
            else:
                break
        except Exception as e:
            print(e)
            browser.close()
            time.sleep(5)
            if (type == "chrome"):
                browser = get_chrome(wash_proxy()['http'], 1, 0)
            elif (type == "phantomJS"):
                browser = get_phantomJS(wash_proxy()['http'])
            continue
    return browser