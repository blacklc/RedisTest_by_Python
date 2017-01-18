#!/usr/bin/env python
#coding:utf-8

'''
Created on 2016年11月15日

@author: lichen
'''

import redis

def main():
    r=redis.StrictRedis(host="localhost",port=6379,db=0)
    #发布/订阅
    print r.publish("redisChat","client_test8888888888") #发布消息
    
main()