#!/usr/bin/env python
# -*- coding:UTF-8 -*-

import json
import requests

# 创建索引-类型-文档
def create():
    url = 'http://10.255.52.3:9200/website/blog/1?op_type=create&pretty'
    headers = {'content-type': 'application/json'}
    args = {
            "title": "My first blog entry",
            "text":  "Just trying this out...",
            "date":  "2014/01/03"
    }
    data = json.dumps(args)
    ropen = requests.put(url=url,data=data, headers=headers)
    print ropen.text

# 搜索
def search():
    #url = 'http://10.255.52.3:9200/website/blog/_search?pretty&_source=title,text'
    #url = 'http://10.255.52.3:9200/website/blog/1/_source?pretty'
    url = 'http://10.255.52.3:9200/website/blog/1?pretty'
    url = 'http://10.255.52.3:9200/_search?pretty&size=5&from=9000'
    headers = {'content-type': 'application/json'}
    args = {
            "title": "My first blog entry",
            "text":  "Just trying this out...",
            "date":  "2014/01/01"
    }
    data = json.dumps(args)
    ropen = requests.get(url=url, headers=headers)
    print ropen.text

# 删除
def delete():
    #url = 'http://10.255.52.3:9200/website/blog/_search?pretty&_source=title,text'
    url = 'http://10.255.52.3:9200/website/blog/1'
    headers = {'content-type': 'application/json'}
    args = {
            "title": "My first blog entry",
            "text":  "Just trying this out...",
            "date":  "2014/01/01"
    }
    data = json.dumps(args)
    ropen = requests.delete(url=url, headers=headers)
    print ropen.text


if __name__ == '__main__':
    #create()
    search()
    #delete()