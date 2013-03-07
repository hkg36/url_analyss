#-*-coding:utf-8-*-
import re
def Prepare(db):
    db.execute('create table if not exists dianpin_url(ip unsigned int,time float,shopid unsigned int,unique(ip,shopid))')
    db.execute('create index if not exists dianpin_index on dianpin_url(ip,time)')
def ProcessDianping(lines,db):
    reres=re.match('^http:\/\/www.dianping.com\/shop\/(?P<sid>\d*)',lines.get('ref'),re.I)
    if reres:
        db.execute('insert or ignore into dianpin_url(ip,time,shopid) values(?,?,?)',(lines.get('src_ip'),lines.get('time'),reres.group('sid')))
        print '%s \t %s'%(lines.get('url'),lines.get('ref'))
        return True
    return None
if __name__ == '__main__':
    pass