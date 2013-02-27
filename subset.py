#-*-coding:utf-8-*-
from bitarray import bitarray
def getSubSet(a,min_cmb_len=0):
    cmbs=[]
    select_array=bitarray('0')
    while len(select_array):
        select_array.extend('0'*(len(a)-len(select_array)))

        cmb=[]
        for i in xrange(len(select_array)):
            if select_array[i]:
                cmb.append(a[i])
        if len(cmb)>min_cmb_len:
            cmbs.append(cmb)

        while len(select_array):
            c=select_array.pop()
            if c==1:
                continue
            c+=1
            select_array.append(c)
            break
    return cmbs

if __name__ == '__main__':
    a=[1,2,3,4,5,6]
    res=getSubSet(a)
    for i in res:
        print i
    print len(res)
    pass