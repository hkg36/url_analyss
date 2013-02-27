#-*-coding:utf-8-*-

import process_qzone
import process_weibo

process_table=[process_weibo.ProcessWeiboCom,
               process_weibo.RefWeiboCom,
               process_qzone.ProcessQZone,
               process_qzone.ProcessQZoneCustom
]
preparedb_table=[process_weibo.Prepare,
                 process_qzone.Prepare]

def GetProcessTable():
    return list(set(process_table))
def GetPrepareDBTable():
    return list(set(preparedb_table))
if __name__ == '__main__':
    pass