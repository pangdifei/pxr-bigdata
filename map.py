#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def test_simhash(text1,text2):
    words1 = jieba.lcut(text1, cut_all=True)
    words2 = jieba.lcut(text2, cut_all=True)
    print(Simhash(words1).distance(Simhash(words2)))

for line in sys.stdin:
    data1 = "Asia全球抢滩潮_申请量单日逼近三十万"
    data2 = lzo.decompress(line)
    test_simhash(data1,data2.decode())