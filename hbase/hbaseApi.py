#!/usr/bin/env python
#  -*- coding:UTF-8 -*-

# 需要安装如下模块
# pip install happybase
# pip install thrift

import happybase

class HbaseManul(object):

    def __init__(self, ip, port):
        self.conn = happybase.Connection(ip, port)

    # 获取所有表
    def allTables(self):
        return self.conn.tables()

    # 创建表 t1，指定列簇名"info"
    def createTable(self, tableName, data):
        self.conn.create_table(tableName, data)

    # 连接表
    def switchTable(self, tableName):
        self.table = self.conn.table(tableName)

    # 插入数据
    def putData(self, rowkey, data):
        # put(row, data, timestamp=None, wal=True)---> 插入数据，无返回值
        #   row---> 行，插入数据的时候需要指定；
        #   data---> 数据，dict类型，{key:value}构成，列与值均为str类型
        #   timestamp--->时间戳，默认None, 即写入当前时间戳
        #   wal---> 是否写入wal, 默认为True
        # rowkey: test1, data: {"info:data":"test hbase"}
        self.table.put(rowkey, data)

    # 查询数据
    def getData(self, rowkey):
        return self.table.row(rowkey)

    # 遍历表
    def scanTable(self):
        for key, value in self.table.scan():   # scan参数 范围查询 row_start='test1',row_stop='test3' 或者 指定查询的row  row_prefix='test3'
            print "key: %s,  value: %s" % (key, value)

    # 删除行
    def deleteRow(self, rowkey):
        self.table.delete(rowkey)

    # 删除表
    def deleteTable(self, tableName):
        self.conn.delete_table(tableName, True)

def main():
    ip = "10.255.52.81"
    port = 9090
    hBase = HbaseManul(ip, port)
    print hBase.allTables()
    # 创建表 t1，指定列簇名"info"
    tableName = "t1"
    #data = {"info": {}}
    #hBase.createTable(tableName, data)
    # 连接表
    hBase.switchTable(tableName)
    hBase.scanTable()
    #hBase.deleteRow("test3")
    #hBase.scanTable()
    # 插入数据, rowkey: test1, data: {"info:data":"test hbase"}
    rowkey = "test3"
    data = {"info:data":"test hbase"}
    hBase.putData(rowkey, data)
    hBase.scanTable()
    # # 获取数据
    # print hBase.getData(rowkey)
    # # 删除表
    # #hBase.deleteTable(tableName)

if __name__ == '__main__':
    main()