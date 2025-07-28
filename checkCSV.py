#!/usr/bin/python
for iAnnee in range(1970,2000,1):
#for iAnnee in range(2010,2023,1):
    print(f"\n *** Annnee : {iAnnee} *** ")
    k=open('./DATAS/deces-'+str(iAnnee)+'.csv','r')
    for i in k:
        q=i.count(';')
        if q != 12:
            print(i)
        if i[0] == ';':
            print(i)


