#!/usr/bin/env python
# !coding:utf-8

import sys
from hdfs.client import Client


# 关于python操作hdfs的API可以查看官网:
# https://hdfscli.readthedocs.io/en/latest/api.html

class HdfsManul(object):

    def __init__(self, url):
        self.client = Client(url, root="/", timeout=100, session=False)
        # self.client = Client(url, root=None, proxy=None, timeout=None, session=None)
        # self.client = Client("http://hadoop:50070")
        # self.client = Client("http://host1:50070", root="/", timeout=100, session=False)
        # self.client = InsecureClient("http://120.78.186.82:50070", user='ann');

    # 读取hdfs文件内容,将每行存入数组返回
    def read_hdfs_file(self, filename):
        lines = []
        with self.client.read(filename, encoding='utf-8', delimiter='\n') as reader:
            for line in reader:
                # pass
                # print line.strip()
                lines.append(line.strip())
        return lines

    # 创建目录
    def mkdirs(self, hdfs_path):
        self.client.makedirs(hdfs_path)

    # 删除hdfs文件
    def delete_hdfs_file(self, hdfs_path):
        self.client.delete(hdfs_path)

    # 上传文件到hdfs
    def put_to_hdfs(self, local_path, hdfs_path):
        self.client.upload(hdfs_path, local_path, cleanup=True)

    # 从hdfs获取文件到本地
    def get_from_hdfs(self, hdfs_path, local_path):
        self.client.download(hdfs_path, local_path, overwrite=False)

    # 追加数据到hdfs文件
    def append_to_hdfs(self, hdfs_path, data):
        self.client.write(hdfs_path, data, overwrite=False, append=True, encoding='utf-8')

    # 覆盖数据写到hdfs文件
    def write_to_hdfs(self, hdfs_path, data):
        self.client.write(hdfs_path, data, overwrite=True, append=False, encoding='utf-8')

    # 移动或者修改文件
    def move_or_rename(self, hdfs_src_path, hdfs_dst_path):
        self.client.rename(hdfs_src_path, hdfs_dst_path)

    # 返回目录下的文件
    def list(self, hdfs_path):
        return self.client.list(hdfs_path, status=False)

    # 获取指定路径的具体信息
    def status(self, hdfs_path):
        return self.client.status(hdfs_path)


def main():
    url = "http://host1:50070"
    hdfs = HdfsManul(url)
    # 获取目录下文件
    print hdfs.list("/")
    # 创建目录
    hdfs.mkdirs("/test3")
    # 读取hdfs文件内容,将每行存入数组返回
    print hdfs.read_hdfs_file('/test2/test1.txt')
    # 获取指定路径的具体信息
    print hdfs.status("/test2/test1.txt")
    # 追加数据到hdfs文件
    hdfs.append_to_hdfs('/test2/test1.txt', u'测试追加'+'\n')
    # 覆盖数据写到hdfs文件
    hdfs.write_to_hdfs('/test2/test1.txt', "sadfafdadsf")
    # 从hdfs获取文件到本地
    hdfs.get_from_hdfs('/test2/test1.txt', 'asdasd')
    # 删除文件
    hdfs.delete_hdfs_file("/test2/test1.txt")

if __name__ == '__main__':
    main()