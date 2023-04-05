#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests   #引入抓取页面库
from bs4 import BeautifulSoup #网页tag分析查找库
from opencc import OpenCC  #简繁体转换库
from hdfs import *         #hdfs读写库
import lzo                #文本压缩后保存用
import subprocess         #hdfs内容手动输出到stdin时用
import sys
from simhash import Simhash #文章相似度判定
import jieba                 #“结巴”分词库，配合上面的simhash使用


class wikinews(object):
    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36'
        }
        self.url = 'http://192.168.5.5/news/X/'
        self.cc = OpenCC('t2s')
        self.root_path = "/"
        self.client = InsecureClient('http://192.168.5.20:9870',user='hdfs',root=self.root_path)

    #转换繁体为简体字
    def conv_lang(self,text):
        converted = self.cc.convert(text)
        return(converted)

    #获取新闻列表页
    def get_html(self):
        response = requests.get(self.url, headers=self.headers)
        html = response.content.decode('utf-8')
        return html

    #获取新闻内容页
    def get_news(self,url):
        response = requests.get(url, headers=self.headers)
        html = response.content.decode('utf-8')
        return html

    #写新闻文本并压缩lzo后到hadoop
    def write_news(self,filename,url):
        html=self.get_news(url)
        soup = BeautifulSoup(html, 'lxml')
        text= lzo.compress(self.conv_lang(soup.text))
        sfilename = self.conv_lang(filename)
        self.client.write('/user/input1/'+sfilename, data=text, overwrite=True)
        self.client.write('/user/idx1/'+sfilename, data=filename+' '+filename+'\n', overwrite=True)
#        file = open("./news/"+sfilename, 'w', encoding='utf-8')
#        file.write(text)
#        file.close()

    def get_list(self):
        html = self.get_html()
        soup = BeautifulSoup(html, 'lxml')
        file = open('list.txt', 'w', encoding='utf-8')
        file2 = open('filenamelist.txt', 'w', encoding='utf-8')
        x=soup.find_all(name='a')
        del(x[0])
        i=1
        for link in x:
            title = link.string
            url = self.url+link.get('href')
            file.write(url)
            file.write('\n')
            file2.write(self.conv_lang(title))
            file2.write('\n')
            try:
                self.write_news(str(i),url)
                i=i+1
            except ValueError:
                print(title)

    def test(self):
#       data1 = lzo.compress(str1, 5)
#       client.write('/tmp/list.txt', data=data1, overwrite=True)
        #   client.download('/tmp/list.txt', '/root/list.txt1',overwrite=True)
        #    obj=client.read("/tmp/list.txt")
        output1 = subprocess.check_output("hdfs dfs -cat "+"\""+"/user/input/4"+"\"", shell=True)
        output2 = subprocess.check_output("hdfs dfs -cat "+"\""+"/user/input/5"+"\"", shell=True)
        data1 = lzo.decompress(output1)
        data2 = lzo.decompress(output2)
        self.test_simhash(data1.decode(),data2.decode())
#       client.upload('/tmp','./list.txt',True)

    def test_simhash(self,text1,text2):
        words1 = jieba.lcut(text1, cut_all=True)
        words2 = jieba.lcut(text2, cut_all=True)
        return words2
#        print("distance:",Simhash(words1).distance(Simhash(words2)))

 #       print(words1)
if __name__ == '__main__':
    web = wikinews()
#   web.get_list()
#   web.test()
    txt1="互联网+交通”借助移动互联网、云计算、大数据、物联网等信息通信新技术，将互联网产业与传统交通运输业完美融合，形成“线上资源合理分配，线下高效优质运行”的新格局，满足更便捷出行、更人性服务和更科学决策的需求，加快推进交通运输由传统产业向现代服务业转型升级。"
    txt2="互联网+交通”充分优化人、车、路之间的网络，提高信息采集强度、采集量及信息处理水平，把所得信息通过各种渠道传送给需求者，提高整个交通系统及个人出行的应变性，使交通更智能、精细和人性。"
    x=web.test_simhash(txt1,txt2)
    print(' '.join(x))

