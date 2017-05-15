from bs4 import BeautifulSoup


url= "http://sh.lianjia.com/zufang/shz3568538.html"
req = urllib2.Request(url)
content = urllib2.urlopen(req).read()
soup = BeautifulSoup(content, "lxml")

soup = read_url(url)
print(soup)