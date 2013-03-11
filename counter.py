#-*-coding:utf-8-*-
import env_var
import sqlite3
import os
import subset
import datetime
import time
from optparse import OptionParser
import pymongo
import json
class Account:
    name=None
    type=None
    def __hash__(self):
        return hash(self.name)^hash(self.type)
    def __str__(self):
        return '[%s(%s)]'%(self.name,self.type)
    def __eq__(self, other):
        return self.name==other.name and self.type==other.type
    def __cmp__(self, other):
        type_cmp=cmp(self.type,other.type)
        if type_cmp==0:
            return cmp(self.name,other.name)
        return type_cmp
def loadFromDb(file):
    group1={}

    db=sqlite3.connect(file)
    dbc=db.cursor()
    dbc.execute('select ip,time,qq from qzone_url order by ip')

    for ip,time,qq in dbc:
        if ip in group1:
            pointusers=group1[ip]
        else:
            pointusers=set()
            group1[ip]=pointusers
        acc=Account()
        acc.name=qq
        acc.type='qq'
        pointusers.add(acc)
    dbc.execute('select ip,time,weibo_id from weibomain_url')
    for ip,time,weibo_id in dbc:
        if ip in group1:
            pointusers=group1[ip]
        else:
            pointusers=set()
            group1[ip]=pointusers
        acc=Account()
        acc.name=weibo_id
        acc.type='weibo'
        pointusers.add(acc)
    dbc.close()
    db.close()
    return group1

class MaybeGroup:
    def __init__(self,group):
        self.group=group
        self.count=0
        self.h=0
    def __hash__(self):
        if self.h:
            return self.h
        for one in self.group:
            self.h ^= hash(one)
        return self.h
    def __eq__(self, other):
        intercount=len(set(self.group).intersection(other.group))
        return intercount==len(self.group) and intercount==len(other.group)
    def __str__(self):
        str_res=''
        for one in self.group:
            str_res+=str(one)
        return '%s=>%d'%(str_res,self.count)
    def getJson(self):
        qqs=[]
        weibos=[]
        for one in self.group:
            if one.type=='qq':
                qqs.append(one.name)
            elif one.type=='weibo':
                weibos.append(one.name)
        qqs.sort()
        weibos.sort()
        return {'qq':qqs,'wb':weibos,'c':self.count}

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-r", "--resdir", dest="resdir",
                      help="resault dir", default='data')
    parser.add_option('-p','--processed',dest="procdir",
                      help="processed resault",default='res')
    parser.add_option('-s','--starttime',dest='starttime',
                      help="start date (YYYYMMDD)")
    parser.add_option('-t','--starthour',dest='starthour',
                      help='start hour (int value)')
    parser.add_option('-d','--database',dest='database',
                      help='mongo db',default='218.241.207.46:27010')
    (options, args) = parser.parse_args()
    options.starthour=int(options.starthour)
    options.starttime=datetime.datetime.fromtimestamp(time.mktime(time.strptime(options.starttime, "%Y%m%d")))

    files=os.listdir(options.resdir)
    basefile=os.path.join(options.resdir,'%s.sqlite'%((options.starttime+datetime.timedelta(hours=options.starthour)).strftime("%Y%m%d%H")))

    cmpfile=[]
    for i in range(1,3):
        time2=options.starttime+datetime.timedelta(hours=options.starthour+i)
        cf=os.path.join(options.resdir,'%s.sqlite'%(time2.strftime("%Y%m%d%H")))
        cmpfile.append(cf)
    for i in range(0,3):
        time2=options.starttime+datetime.timedelta(days=1,hours=options.starthour+i)
        cf=os.path.join(options.resdir,'%s.sqlite'%(time2.strftime("%Y%m%d%H")))
        cmpfile.append(cf)

    group1=loadFromDb(basefile)

    maybe_groups=set()
    maybe_key={}
    onlyone=[]
    for g in group1.values():
        g=list(g)
        if len(g)>12:
            print 'to match acc(%d)'%len(g)
            continue
        res=subset.getSubSet(g,1)
        if len(res)==1:
            onlyone.extend(res)
        else:
            for oneres in res:
                maybegroup=MaybeGroup(oneres)
                if maybegroup in maybe_groups:
                    continue
                maybe_groups.add(maybegroup)
                for one_key in oneres:
                    if one_key in maybe_key:
                        sublist=maybe_key[one_key]
                    else:
                        sublist=list()
                        maybe_key[one_key]=sublist
                    sublist.append(maybegroup)
    group1=None
    for j in cmpfile:
        group2=loadFromDb(j)
        for g in group2.values():
            if len(g)==1:
                continue
            sub_maybe=set()
            for one in g:
                sub_g=maybe_key.get(one)
                if sub_g:
                    for oo in sub_g:
                        sub_maybe.add(oo)

            for sm in sub_maybe:
                if len(set(sm.group).intersection(g))==len(sm.group):
                    sm.count+=1


    min_match=len(cmpfile)/2+1
    con=pymongo.Connection('mongodb://%s/'%options.database)
    for mg in maybe_groups:
        if mg.count>min_match:
            js=mg.getJson()
            key={'qq':js['qq'],'wb':js['wb']}
            value={'$set':{'qq':js['qq'],'wb':js['wb']},'$inc':{'c':js['c']}}
            con.maybe_group.data.update(key,value,upsert=True)
    con.close()