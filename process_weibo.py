#-*-coding:utf-8-*-
import re
def Prepare(db):
    db.execute('create table if not exists weibomain_url(vid varchar(64),time float,weibo_id varchar(32),unique(vid,weibo_id))')
    db.execute('create index if not exists weibomain_index on weibomain_url(vid,time)')
def ProcessWeiboCom(lines,db):
    reres=re.match('^(http|https):\/\/www\.weibo\.com\/(u/)?(?P<wid>[^?]+)\?.*&lf=reg$',lines.get('ref'),re.I)
    if reres:
        db.execute('insert or ignore into weibomain_url(vid,time,weibo_id) values(?,?,?)',(lines.get('vid'),lines.get('time'),reres.group('wid')))
        print '%s'%(lines.get('url'))
        return True
    return None
def RefWeiboCom(lines,db):
    reres=re.match('^(http|https):\/\/www\.weibo\.com\/(u/)?(?P<wid>[^?]+)\?.*&lf=reg$',lines.get('ref'),re.I)
    if reres:
        db.execute('insert or ignore into weibomain_url(vid,time,weibo_id) values(?,?,?)',(lines.get('vid'),lines.get('time'),reres.group('wid')))
        print '%s \t %s'%(lines.get('url'),lines.get('ref'))
        return True
    return None
if __name__ == '__main__':
    pass