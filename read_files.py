#-*-coding:utf-8-*-
import os
import re
import time
import sqlite3
import config
import env_var
import socket,struct
from optparse import OptionParser
import multiprocessing

process_table=config.GetProcessTable()
preparedb_table=config.GetPrepareDBTable()

def DirProcessor(path,data_dir,resautl_dir):
    fullpath=os.path.join(data_dir,path)
    if os.path.isdir(fullpath):
        infodb=sqlite3.connect(os.path.join(resautl_dir,'%s.sqlite'%path))
        infocur=infodb.cursor()
        for preparedb in preparedb_table:
            preparedb(infocur)
        infodb.commit()
        for file in os.listdir(fullpath):
            filepath=os.path.join(data_dir,path,file)
            if os.path.isfile(filepath):
                fp=open(filepath)
                for line in fp:
                    try:
                        elements=line.strip().split('\t')
                        if len(elements)<4:
                            continue
                        dataline={}
                        dataline['time']=time.mktime(time.strptime(elements[3],'%Y-%m-%d %H:%M:%S'))
                        dataline['vid']=elements[0]
                        dataline['fun']=elements[1]
                        dataline['url']=elements[4]
                        if len(elements)>=6:
                            dataline['ref']=elements[5]
                        if len(elements)>=7:
                            dataline['cookie']=elements[6]
                        if len(elements)>=8:
                            dataline['src_ip']=socket.ntohl(struct.unpack("I",socket.inet_aton(elements[7]))[0])
                        if len(elements)>=9:
                            dataline['dis_ip']=socket.ntohl(struct.unpack("I",socket.inet_aton(elements[8]))[0])
                        for procer in process_table:
                            if procer(dataline,infocur) is not None:
                                break
                    except Exception,e:
                        print e
                fp.close()
        infodb.commit()
        infodb.close()

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dir", dest="dir",
                      help="set data root dir", default=env_var.data_dir)
    parser.add_option("-r", "--resdir", dest="resdir",
                      help="resault dir", default='data')
    (options, args) = parser.parse_args()

    res=os.listdir(options.dir)
    toproc_path=[]
    for path in res:
        fullpath=os.path.join(options.dir,path)
        if os.path.isdir(fullpath):
            if not os.path.isfile(os.path.join(options.resdir,'%s.sqlite'%path)):
                toproc_path.append(path)

    pool=multiprocessing.Pool()
    for path in toproc_path:
        pool.apply_async(DirProcessor,(path,options.dir,options.resdir))
    pool.close()
    pool.join()
    #for path in toproc_path:
        #DirProcessor(path,options.dir,options.resdir)