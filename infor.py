#coding:utf-8
import os
import inspect
import urllib
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
import sqlite3
import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool
from itertools import chain
import glob
import urllib2

txtlist = glob.glob(os.path.join("", 'lj_links*.txt'))
temp1 = {}
for i in txtlist:
    temp1[i] = os.path.getmtime(i)
filename = sorted(temp1.items(),key=lambda item:item[1],reverse = True)[0][0]
f = open(filename,"r")
finalset = eval(f.read())

fullset = list(chain(*finalset))

df = pd.DataFrame(fullset)
# print(df.iat[1,0])

alreadylist = []
conn=sqlite3.connect('%s' %'SHRENT.db')

cur=conn.cursor()

cur.execute('''DROP TABLE basic_information''')
cur.execute('''CREATE TABLE basic_information(
    'index' ,URL , address , area , direct , district1 , district2 , floor1 ,
    floor2 , latitude , longitude , onsaledate ,price , room , title , xiaoqu)''')
# http://stackoverflow.com/questions/33270534/sqlite3-programmingerror-incorrect-number-of-bindings-supplied-the-current-sta
# title1 = df.iat[1,0]
# for x in fullset:
#  cur.execute('''INSERT INTO basic_information(URL)
# VALUES(?)''', (x,))
# conn.commit()
query = 'select URL from basic_information'
alreadylist = list(pd.read_sql(query, conn)['URL'])
fullset = list(set(fullset).union(set(alreadylist)).difference(set(alreadylist)))
print(alreadylist)
# print(fullset)

errorlist = []

# engine = create_engine('sqlite:///%s' %'SHRENT.db', echo = True)
engine = create_engine('sqlite:///D:\\爬虫\\python project\\%s' %'SHRENT.db',echo = False)

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
        raise
    soup = BeautifulSoup(content, "lxml")
    return soup

def save(urlset):
    title = []
    price = []
    room = []
    area = []
    floor1 = []
    floor2 = []
    direct = []
    district1 = []
    district2 = []
    onsaledate = []
    xiaoqu = []
    address = []
    number = []
    longitude = []
    latitude = []
    URL = []

    try:
        soup = read_url(urlset)
        print(soup)
        title.append(soup.find('h1', class_ = 'main').get_text())
        price1 = soup.find('div', class_ = 'price').get_text()
        price.append(int(re.findall(r'\d+', price1)[0]))
        room.append(soup.find('div', class_ = 'room').get_text().strip())
        area1 = soup.find('div', class_ = 'area').get_text()
        area.append(int(re.findall(r'\d+', area1)[0]))
        floor_ori = soup.find_all('td')[1].get_text()
        floor1.append(floor_ori.split("/")[0])
        floor2.append(int(re.findall(r'\d+', floor_ori.split("/")[1])[0]))
        direct.append(soup.find_all('td')[3].get_text().strip())
        district_ori = soup.find_all('td')[5].get_text()
        district1.append(district_ori.split(" ")[0])
        district2.append(district_ori.split(" ")[1])
        onsaledate.append(soup.find_all('td')[7].get_text())
        xiaoqu.append(soup.p.get_text().strip())
        address.append(soup.find_all('p')[1].get_text().strip())
        number.append(soup.find('span', class_ = 'houseNum').get_text()[5:])
        temp1 = str(soup.find_all('div', class_='around js_content')[0])
        temp2 = re.findall(r'\d+\.\d+',temp1)
        longitude.append(temp2[1])
        latitude.append(temp2[0])
        URL.append(urlset)
    except:
        errorlist.append(urlset)

    df_dic = {'title':title, 'price':price, 'room':room, 'area':area, 'floor1':floor1, 'floor2':floor2,
    'direct':direct, 'district1':district1, 'district2':district2, 'onsaledate':pd.to_datetime(onsaledate),
    'xiaoqu': xiaoqu, 'address': address, 'number':number, 'longitude':longitude, 'latitude':latitude, 'URL':URL}
    try:
        dataset = pd.DataFrame(df_dic,index = number)
        dataset = dataset.drop(['number'], axis = 1)
    except:
        dataset = pd.DataFrame()
    dataset.to_sql('basic_information', engine, if_exists = 'append')


pool = ThreadPool(4)
pool.map(save, fullset)
pool.close()
pool.join()

f = open('Notsaved.txt', 'w')
# print(errorlist, f)
f.close()


