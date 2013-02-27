#-*-coding:utf-8-*-
import re
def Prepare(db):
    db.execute('create table if not exists qzone_url(vid varchar(64),time float,qq varchar(32),unique(vid,qq))')
    db.execute('create index if not exists qzone_index on qzone_url(vid,time)')
def ProcessQZone(lines,db):
    reres=re.match('^http:\/\/user\.qzone\.qq\.com\/(?P<qq>\d{5,})\/infocenter$',lines.get('ref'),re.I)
    if reres:
        db.execute('insert or ignore into qzone_url(vid,time,qq) values(?,?,?)',(lines.get('vid'),lines.get('time'),reres.group('qq')))
        print '%s \t %s'%(lines.get('url'),lines.get('ref'))
        return True
    return None
def ProcessQZoneCustom(lines,db):
    reres=re.match('^http:\/\/user\.qzone\.qq\.com\/(?P<qq>\d{5,})\/main\/custom\/common$',lines.get('url'),re.I)
    if reres is None:
        reres=re.match('^http:\/\/user\.qzone\.qq\.com\/(?P<qq>\d{5,})\/main\/custom\/common$',lines.get('ref'),re.I)
    if reres:
        db.execute('insert or ignore into qzone_url(vid,time,qq) values(?,?,?)',(lines.get('vid'),lines.get('time'),reres.group('qq')))
        print '%s \t %s'%(lines.get('url'),lines.get('ref'))
        return True
    return None
if __name__ == '__main__':
    pass