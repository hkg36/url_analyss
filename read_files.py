#-*-coding:utf-8-*-
import os
import re
import time
import sqlite3
import config
import env_var

process_table=config.GetProcessTable()
preparedb_table=config.GetPrepareDBTable()
if __name__ == '__main__':
    data_dir=env_var.data_dir
    res=os.listdir(data_dir)
    toproc_path=[]
    for path in res:
        fullpath='%s/%s'%(data_dir,path)
        if os.path.isdir(fullpath):
            if not os.path.isfile('data/%s.sqlite'%path):
                toproc_path.append(path)
    for path in toproc_path:
        fullpath='%s/%s'%(data_dir,path)
        if os.path.isdir(fullpath):
            infodb=sqlite3.connect('data/%s.sqlite'%(path,))
            infocur=infodb.cursor()
            for preparedb in preparedb_table:
                preparedb(infocur)
            infodb.commit()
            for file in os.listdir(fullpath):
                filepath='%s/%s/%s'%(data_dir,path,file)
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
                                dataline['src_ip']=elements[7]
                            if len(elements)>=9:
                                dataline['dis_ip']=elements[8]
                            for procer in process_table:
                                if procer(dataline,infocur) is not None:
                                    break
                        except Exception,e:
                            print e
                    fp.close()
            infodb.commit()
            infodb.close()