# python 批量远程执行SQL SERVER 脚本
#!/usr/bin/env python
# encoding: utf-8
 
"""
@version: ??
@author: phpergao
@license: Apache Licence 
@file: mssql.py
@time: 2016/7/18 10:18
"""
import pymssql,os
from multiprocessing import Process
import threading
import time
 
class MSSQL:
    """
    对pymssql的简单封装
    pymssql库，该库到这里下载：http://www.lfd.uci.edu/~gohlke/pythonlibs/#pymssql
    使用该库时，需要在Sql Server Configuration Manager里面将TCP/IP协议开启
 
    用法：
 
    """
 
    def __init__(self,host,user,pwd,db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
 
    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur
 
    def ExecQuery(self,sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段
 
        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()
 
        # 完毕后必须关闭连接
        self.conn.close()
        return resList
 
    def ExecNonQuery(self,sql):
        """
        执行非查询语句
 
        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()
 
def select(hostname,ip,username,password,dbname,cmd):
    if password==None:
        password=''
    cha = MSSQL(host=ip, user=username, pwd=password, db=dbname)
    resList = cha.ExecQuery(cmd)
    # print(resList)
    print("主机{}执行查询命令的结果：".format(hostname))
    for i in resList:
 
        print(i)
 
def not_select(hostname,ip,username,password,dbname,cmd):
    if password == None:
        password = ''
    ms = MSSQL(host=ip, user=username, pwd=password, db=dbname)
    ms.ExecNonQuery(cmd)
    print("主机{}执行:{}.成功".format(hostname,cmd))
 
def main(cmd):
## ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
## #返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段
## ms.ExecNonQuery("insert into WeiBoUser values('2','3')")
 
    ms = MSSQL(host="127.0.0.1",user="sa",pwd="123456",db="test")
    resList = ms.ExecQuery("SELECT * FROM dbo.db_host")#从本地数据库获取到要操作的远程数据库信息
    for i in resList:
        #print (i)
        time.sleep(0.1)
        ip_add=os.popen("ping -n 1 {}".format(i[1])).read().strip()#如果ping不通，就不操作了。
        if 'TTL'not in ip_add:
            continue
 
        if  "SELECT" in cmd  or 'select' in cmd:
            #select(i[0],i[1],i[2],i[3],i[4],cmd)
            p = Process(target=select,args=(i[0],i[1],i[2],i[3],i[4],cmd))
            p.start()
 
        else:
            p = Process(target=not_select, args=(i[0], i[1], i[2], i[3], i[4], cmd))
            p.start()
            #not_select(i[0],i[1],i[2],i[3],i[4],cmd)
 
    #dele =ms.ExecNonQuery("DELETE FROM waimai4.dbo.baidu_rueren")
    #update=ms.ExecNonQuery("UPDATE dbo.GOODS SET CLASSID='19' WHERE GOODSNAME LIKE'%牛肉%'")
if __name__ == '__main__':
    cmd=input("请输入SQL语句：").strip()
    main(cmd)
