import urllib2
from bs4 import BeautifulSoup
import inspect
from multiprocessing.dummy import Pool as ThreadPool
import math
import datetime

starturl="http://sh.lianjia.com/zufang/d1l2"

req = urllib2.Request(starturl)
content = urllib2.urlopen(req).read()
soup = BeautifulSoup(content, "lxml")
page = soup.find_all('a')
pagenum1 = page[-2].get_text()
totalpage = int(math.ceil(float(soup.h2.span.get_text())/20))
first_urlset = []
for i in range(1,2):
    url = "http://sh.lianjia.com/zufang/d" + str(i) + "l2"
    first_urlset.append(url)

def read_url(url):
    req = urllib2.Request(url)
    fails = 0
    while fails < 5:
        try:
            content = urllib2.urlopen(req, timeout=20).read()
            print'find item'
            break
        except:
            fails += 1
        print inspect.stack()[1][3] + ' occused error'
    soup = BeautifulSoup(content, "lxml")
    return soup

def get_houselinks(url):
    soup = read_url(url)
    firstlinkset = soup.find_all('h2')
    firstlinkset = firstlinkset[1:]
    houselink = ['http://sh.lianjia.com' + i.a['href'] for i in firstlinkset]
    return houselink

pool = ThreadPool(4)
finalset = pool.map(get_houselinks, first_urlset)
pool.close()
pool.join()

today = datetime.date.today().strftime("%Y%m%d")
f = open("%s" %'lj_links' + today + '.txt',"w")
f.write(str(finalset))
f.close()