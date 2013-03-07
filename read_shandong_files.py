#-*-coding:utf-8-*-

import os
import re
import time
import sqlite3
import config
import env_var
from optparse import OptionParser
import socket,struct

def prepareQzone(db):
    db.execute('create table if not exists qzone_url(ip unsigned int,time float,qq varchar(32))')

def ProcessQZone(lines,db):
    reres=re.match('^http:\/\/user\.qzone\.qq\.com\/(?P<qq>\d{5,})\/infocenter$',lines.get('url'),re.I)
    if reres:
        db.execute('insert or ignore into qzone_url(ip,time,qq) values(?,?,?)',(lines.get('src_ip'),lines.get('time'),reres.group('qq')))
        print lines.get('url')
        return True
    return None

def prepareWeibo(db):
    db.execute('create table if not exists weibomain_url(ip insigned int,time float,weibo_id varchar(32))')
def ProcessWeiboCom(lines,db):
    reres=re.match('^(http|https):\/\/www\.weibo\.com\/(u/)?(?P<wid>[^?]+)\?.*&lf=reg$',lines.get('url'),re.I)
    if reres:
        db.execute('insert or ignore into weibomain_url(ip,time,weibo_id) values(?,?,?)',(lines.get('src_ip'),lines.get('time'),reres.group('wid')))
        print '%s'%(lines.get('url'))
        return True
    return None

preparedb_table=[prepareQzone,prepareWeibo]
process_table=[ProcessQZone,ProcessWeiboCom]
if __name__ == '__main__':
    data_dir='testdata/jinan'
    output_dir='data2'
    res=os.listdir(data_dir)
    toproc_path=[]
    for path in res:
        fullpath=os.path.join(data_dir,path)
        if os.path.isdir(fullpath):
            if not os.path.isfile(os.path.join(output_dir,'%s.sqlite'%path)):
                toproc_path.append(path)
    for path in toproc_path:
        fullpath=os.path.join(data_dir,path)
        if os.path.isdir(fullpath):
            infodb=sqlite3.connect(os.path.join(output_dir,'%s.sqlite'%path))
            infocur=infodb.cursor()
            for preparedb in preparedb_table:
                preparedb(infocur)
            infodb.commit()
            for file in os.listdir(fullpath):
                filepath=os.path.join(data_dir,path,file)
                if os.path.isfile(filepath):
                    with open(filepath) as fp:
                        for line in fp:
                            try:
                                elements=line.strip().split('\t')
                                if len(elements)<4:
                                    continue
                                dataline={}
                                dataline['time']=time.mktime(time.strptime(elements[4],'%Y-%m-%d %H:%M:%S'))
                                dataline['src_ip']=socket.ntohl(struct.unpack("I",socket.inet_aton(elements[1]))[0])
                                dataline['fun']=elements[3]
                                dataline['url']=elements[5]

                                for procer in process_table:
                                    if procer(dataline,infocur) is not None:
                                        break
                            except Exception,e:
                                print e
            infodb.commit()
            infodb.close()