#-*-coding:utf-8-*-
import env_var
import sqlite3
import os
import subset
class Account:
    name=None
    type=None
    def __hash__(self):
        return hash(self.name)^hash(self.type)
    def __str__(self):
        return '%s(%s)'%(self.name,self.type)
    def __eq__(self, other):
        return self.name==other.name and self.type==other.type
def loadFromDb(file):
    db=sqlite3.connect('data/'+file)
    dbc=db.cursor()
    dbc.execute('select ip,time,qq from qzone_url order by ip')
    group1={}
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
        return '%s => %d'%(str_res,self.count)

if __name__ == '__main__':
    files=os.listdir('data')
    for i in xrange(0,len(files)-1):
        group1=loadFromDb(files[i])

        maybe_groups=set()
        maybe_key={}
        onlyone=[]
        for g in group1.values():
            g=list(g)
            if len(g)>10:
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
        print 'maybe count %d'%len(maybe_groups)
        for j in range(1,3):
            group2=loadFromDb(files[i+j])
            for g in group2.values():
                if len(g)==1:
                    continue
                sub_maybe=set()
                for one in g:
                    sub_g=maybe_key.get(one)
                    if sub_g:
                        for oo in sub_g:
                            sub_maybe.add(oo)
                print 'maybe %d'%len(sub_maybe)

                for sm in sub_maybe:
                    if len(set(sm.group).intersection(g))==len(sm.group):
                        sm.count+=1

        for mg in maybe_groups:
            if mg.count>1:
                print mg
        break