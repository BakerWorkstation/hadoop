#!/usr/bin/env python
# -*- coding:UTF-8 -*-

import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class EsManul(object):

    def __init__(self, host, port):
        self.es = Elasticsearch([{'host': host, 'port': port}])

    def search(self):
        # index - 索引名
        # q - 查询指定匹配  使用Lucene查询语法
        # from_ - 查询起始点  默认0
        # doc_type - 文档类型
        # size - 指定查询条数  默认10
        # field - 指定字段  逗号分隔
        # sort - 排序  字段：asc / desc
        # body - 使用Query  DSL
        # scroll - 滚动查询

        # query_all = {
        #               "query": {
        #                         #"match_all": {},
        #                         "term": {
        #                                  "money": "10"
        #                         },
        #                         "range": {
        #                                     "money": {
        #                                                 "gte": 10,
        #                                                 "lte": 30
        #                                     }
        #                         }
        #               }
        # }
        query_all = {
                     #"size": 100,
                     "query": {
                                "match_all": {}
                     },
                     "aggs": {  # 聚合查询
                                "avg_money": {
                                            "percentiles": {  # extended_stats
                                                    "field": "money"
                                            }
                                }
                     }
        }

        return self.es.search(index="price",  body=query_all)

    def get(self, index, type, id):
        return self.es.get(index=index, doc_type=type, id=id)

    def insert(self):
        actions = []
        action = {
                    "_index": "price",
                    "_type": "shoes",
                    "_id": "1",
                    "_source": {
                                "money": 10
                    }
        }
        actions.append(action)
        action = {
                    "_index": "price",
                    "_type": "shoes",
                    "_id": "2",
                    "_source": {
                                "money": 20
                    }
        }
        actions.append(action)
        action = {
                    "_index": "price",
                    "_type": "shoes",
                    "_id": "2",
                    "_source": {
                                "money": 30
                    }
        }
        actions.append(action)
        rs = bulk(self.es, actions=actions)
        print(u'成功插入%d个文档...' % (rs[0]))

    def delete(self):
        body = {
                 "query": {
                            "match": {
                                        "money": "30"
                          }
               }
        }
        self.es.delete_by_query(index='price', doc_type='shoes', body=body)
        #self.es.delete(index="price", doc_type="shoes", id="3")


def main():
    host = '10.255.52.3'
    port = 9200
    Es = EsManul(host, port)
    data = Es.search()#['hits']['hits']
    #print json.dumps(data, sort_keys=True, indent=2)
    index = "price"
    type = "shoes"
    id = "2"
    print Es.get(index, type, id)
    #Es.insert()
    #Es.delete()


if __name__ == '__main__':
    main()
