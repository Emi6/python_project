# coding:utf-8
from sqlalchemy import create_engine
import sqlite3
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
import datetime
import re
import urllib2
from bs4 import BeautifulSoup
import inspect

today = datetime.date.today().strftime("%Y%m%d")
t = 'price'+today

conn = sqlite3.connect('SHRENT.db',timeout=10)
engine = create_engine('sqlite:///D:\\爬虫\\python project\\%s' % 'SHRENT.db', echo=False)

cur = conn.cursor()

cur.execute('''DROP TABLE price_temp''')
cur.execute('''CREATE TABLE price_temp(
    'index' ,URL ,price20170403, number)''')
query1 = 'select URL from basic_information'
urlist_basic = list(pd.read_sql(query1, conn)['URL'])  # 在basic表中的列表
query2 = 'select * from price_temp'
try:
    alreadylist = list(pd.read_sql(query2, conn)['URL'])  # 在price_temp表中的表
except:
    alreadylist = []
# basic和price表的差
urlist2 = list(set(urlist_basic).union(set(alreadylist)).difference(set(alreadylist)))


def read_url(url):
    req = urllib2.Request(url)
    fails = 0
    while fails < 5:
        try:
            content = urllib2.urlopen(req, timeout=20).read()
            break
        except:
            fails += 1
        print(inspect.stack()[1][3] + ' occused error')
    soup = BeautifulSoup(content, "lxml")
    return soup


errorlist = []


def save_price(urls):
    soup = read_url(urls)
    price = []
    number = []
    URLs = []

    try:
        price1 = soup.find('div', class_='price').get_text()
        price.append(int(re.findall(r'\d+', price1)[0]))  # 价格
        number.append(soup.find('span', class_='houseNum').get_text()[5:])  # 房源编号
        URLs.append(urls)  # 链接
    except:
        errorlist.append(urls)

    # df_dic = {'URL': URLs, 'price' + today : price, 'number':number}
    df_dic = {'URL': URLs, 'price'+ today: price, 'number': number}
    try:
        dataset = pd.DataFrame(df_dic, index=number)
        dataset = dataset.drop(['number'], axis=1)
    except:
        dataset = pd.DataFrame()
    dataset.to_sql('price_temp', engine, if_exists='append')  # 先临时存放在price_temp的表里


pool = ThreadPool(4)
pool.map(save_price, urlist2)
pool.close()
pool.join()

f = open('Notupdated.txt', 'w')  # 把失败的链接存下来
# print(errorlist, file = f)
f.close()

df1 = pd.read_sql("select * from  basic_information", conn)
df2 = pd.read_sql("select * from price_temp", conn)
df = pd.merge(df1, df2, how='outer', on=['index', 'URL'])  # 注1
df = df.set_index('index')
df.to_sql('basic_information', engine, if_exists='replace')  # 存入数据

# cu = conn.cursor()
# cu.execute('DROP TABLE price_temp')  # 把price_temp删除
# conn.close()
