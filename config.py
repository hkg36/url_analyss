#-*-coding:utf-8-*-

import process_qzone
import process_weibo
import process_dianping

process_table=[process_weibo.ProcessWeiboCom,
               process_weibo.RefWeiboCom,
               process_qzone.ProcessQZone,
               process_qzone.ProcessQZoneCustom,
               process_dianping.ProcessDianping
]
preparedb_table=[process_weibo.Prepare,
                 process_qzone.Prepare,
                 process_dianping.Prepare
                 ]

def GetProcessTable():
    return list(set(process_table))
def GetPrepareDBTable():
    return list(set(preparedb_table))
if __name__ == '__main__':
    pass