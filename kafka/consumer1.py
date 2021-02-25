#!/usr/bin/python
# coding:utf-8

from kafka import KafkaConsumer

consumer = KafkaConsumer('test',
                         group_id='group1',
                         bootstrap_servers=['127.0.0.1:9092']
                         )

print 'ready'
i = 1
for message in consumer:
    print ("NO: %d" % i)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    i += 1
print 'end'
