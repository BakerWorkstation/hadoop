#!/usr/bin/python
# coding:utf-8
import time

from kafka import KafkaProducer

def product(producer):
    for i in range(100):
        msg = "msg%d" % i
        print msg
        producer.send('test', msg)
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])
while 1:
    product(producer)
    time.sleep(5)
#producer.close()
