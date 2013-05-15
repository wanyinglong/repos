#/usr/bin/env python
#coding: utf-8
#from mysql_lib import PyMysql
import datetime,os,sys
import MySQLdb
import MySQLdb.cursors

STORE_RESULT_MODE=0
USE_RESULT_MODE=1
CURSOR_MODE=0
DICTCURSOR_MODE=1
SSCURSOR_MODE=2
SSDICTCURSOR_MODE=3

FETCH_ONE=0
FETCH_MANY=1
FETCH_ALL=2

"""mysql工具类"""
class PyMysql:
    def __init__(self):
        self.conn=None
        pass
    def newConnection(self,host,user,passwd,defaultdb):
        """建立一个新连接，指定host，用户名，密码，默认数据库"""
        self.conn=MySQLdb.Connect(host,user,passwd,defaultdb)
        if self.conn.open==False:
            raise None
        curclass=MySQLdb.cursors.Cursor
        cur=self.conn.cursor(cursorclass=curclass)
        line=cur.execute("set names utf8")
        
    def closeConnection(self):
        """关闭当前连接"""
        self.conn.close()
    def commit(self):
        """提交数据"""
        self.conn.commit()

    def execute(self,sqltext,args=None,mode=CURSOR_MODE,many=False):
        """
        更新update，delete
        作用：使用游标（cursor）的execute 执行query
        参数：sqltext： 表示sql语句
             args： sqltext的参数
             mode：以何种方式返回数据集
                CURSOR_MODE = 0 ：store_result , tuple
                DICTCURSOR_MODE = 1 ： store_result , dict
                SSCURSOR_MODE = 2 : use_result , tuple
                SSDICTCURSOR_MODE = 3 : use_result , dict 
             many：是否执行多行操作（executemany）
             execute方法，执行单条sql语句，调用executemany方法很好用，数据库性能瓶颈很大一部分就在于网络IO和磁盘IO将多个insert放在一起，
             只执行一次IO，可以有效的提升数据库性能。游标cursor具有fetchone、fetchmany、fetchall三个方法提取数据，每
             个方法都会导致游标游动，所以必须关注游标的位置。游标的scroll(value, mode)方法可以使得游标进行卷动，
             mode参数指定相对当前位置(relative)还是以绝对位置(absolute)进行移动。
        返回：元组（影响行数（int），游标（Cursor））
        """
        if mode==CURSOR_MODE:
            curclass=MySQLdb.cursors.Cursor
        elif mode==DICTCURSOR_MODE:
            curclass=MySQLdb.cursors.DictCursor
        elif mode==SSCURSOR_MODE:
            curclass=MySQLdb.cursors.SSCursor
        elif mode==SSDICTCURSOR_MODE:
            curclass=MySQLdb.cursors.SSDictCursor
        else:
            raise Exception("mode value is wrong")
        cur=self.conn.cursor(cursorclass=curclass)
        #print sqltext
        line=0
        if many==False:
            if args==None:
                line = cur.execute(sqltext)
            else:
                line=cur.execute(sqltext,args)
        else:
            if args==None:
                line=cur.executemany(sqltext)
            else:
                line=cur.executemany(sqltext,args)
        self.conn.commit()
        return(line,cur)


def query_insert_many(queryfile,query_error_file):
    """批量插入mysql数据"""
    args20=[]
    args19=[]
    args3=[]
    fp=open(queryfile,'r')
    lines=fp.readlines()
    for line in lines:
        query = line.split('|')
        if len(query) == 20:
            sql20 = """insert into query(colum1,colum2,colum3,colum4,colum5,colum6,colum7,colum8,colum9,colum10,
            colum11,colum12,colum13,colum14,colum15,colum16,colum17,colum18,colum19,colum20) values 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            args20.append(tuple(query))
            if len(args20)>20000:
                mysql.execute(sql20,args=args20,many=True)
                args20=[]
        elif len(query) == 19:
            sql19 = """insert into query(colum1,colum2,colum3,colum4,colum5,colum6,colum7,colum8,colum9,colum10,
            colum11,colum12,colum13,colum14,colum15,colum16,colum17,colum18,colum20)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            args19.append(tuple(query))
            if len(args19)>20000:
                mysql.execute(sql19,args=args19,many=True)
                args19=[]
        elif len(query) == 3:
            sql3 = """insert into query(colum1,colum2,colum3)
            values (%s,%s,%s)"""
            args3.append(tuple(query))
            if len(args3)>20000:
                mysql.execute(sql3,args=args3,many=True)
                args3=[]            
        else:
            f2=open(query_error_file,'a+')
            f2.write(line)
            f2.close
    if args20:
        mysql.execute(sql20,args=args20,many=True)
    if args19:
        mysql.execute(sql19,args=args19,many=True)
    if args3:
        mysql.execute(sql3,args=args3,many=True)


def query_insert(queryfile,query_error_file):
    """单条插入mysql数据"""
    i=0
    sqltext=""
    fp=open(queryfile,'r')
    lines=fp.readlines()
    for line in lines:
        query = line.split('|')    
        if len(query) == 20:
            sql = '''insert into query values ('%s','%s','%s','%s',
                  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                  '%s','%s','%s','%s','%s','%s')''' % (query[0],query[1],query[2],query[3],query[4],
                                                       query[5],query[6],query[7],query[8],query[9],
                                                       query[10],query[11],query[12],query[13],query[14],
                                                       query[15],query[16],query[17],query[18],query[19],)
            mysql.execute(sql)
        elif len(query) == 19:
            sql = '''insert into query(colum1,colum2,colum3,colum4,colum5,colum6,colum7,colum8,colum9,colum10,
            colum11,colum12,colum13,colum14,colum15,colum16,colum17,colum18,colum20) values ('%s','%s','%s','%s',
                  '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',
                  '%s','%s','%s','%s','%s')''' % (query[0],query[1],query[2],query[3],query[4],
                                                       query[5],query[6],query[7],query[8],query[9],
                                                       query[10],query[11],query[12],query[13],query[14],
                                                       query[15],query[16],query[17],query[18],)
            mysql.execute(sql)
        elif len(query) == 3:
            sql = '''insert into query(colum1,colum2,colum3) 
            values ('%s','%s','%s')''' % (query[0],query[1],query[2])
            mysql.execute(sql)
        else:
            f2=open(query_error_file,'a+')
            f2.write(line)
            f2.close            

if __name__=='__main__':
    """输入需要处理的文件名，并判断是否存在"""
    if len(sys.argv)<2:
        print '''Error:Usage:%s logfile ''' % sys.argv[0]
        sys.exit(1)
    if not os.path.isfile(sys.argv[1]):
        print '''the specify %s is not a file,please enter a valid query log file''' % sys.argv[1]
        sys.exit(1)    
    print datetime.datetime.now()
    mysql = PyMysql()
    mysql.newConnection(host='172.16.0.195',user='zhao',passwd='zhao',defaultdb='zhao')
    #query_log='/home/eden/query.log'
    query_log=sys.argv[1]
    query_error_file='/opt/resin-pro-3.1.12/logs/'+'query_error.log'
    query_insert_many(queryfile=query_log,query_error_file=query_error_file)
    print datetime.datetime.now()
    mysql.closeConnection()
