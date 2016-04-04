'''
Created on Feb 17, 2016

@author: jumbrich
'''
import csv
from collections import defaultdict

def getBin(r_local):
    s=0
    if r_local ==0:
        s=0
    elif r_local <0.25:
        s=25
    elif r_local <0.5:
        s=50
    elif r_local <0.75:
        s=75
    elif r_local <1:
        s=99
    elif r_local ==1:
        s=100
    return s

if __name__ == '__main__':
    
    per_portal={}
    per_software={}
    
    total={}

    csvf="/Users/jumbrich/Data/eval/res_change_info.csv"
    r=csv.reader(open(csvf ))
    h=None
    cnt=0
    for row in r:
        cnt+=1
        if cnt==1:
            h=row
            continue

        if len(row)!= 10:
            continue

        #print row
        soft=row[1]
        portal=row[2]

        url= row[0]
        local=row[3]
        http_lm=row[4]
        meta_lm=row[6]
        
        soft_stats=per_software.setdefault(soft,{})
        p_stats=per_portal.setdefault(portal,{
                'isURL':0
                ,'total':0
                ,'local':0
                ,'heads':0
                ,'http_lm':0
                ,'no_lm':0
                ,'meta_lm':0
                ,'meta_lm_value':0
                ,'meta_lm_empty':0
                ,'meta_mis':0
                ,'local_lm':0
                ,'not_local_lm':0

                ,'software':soft
                ,'distinct':set([])
                                              })

        #how many resources could be monitored with HTTP? and how many have a last_modified field

        isURL = False
        try:
            import urlnorm
            url=urlnorm.norm(url)
            isURL=True
        except Exception as e:
            pass

        p_stats['total']+=1 # how many resources
        p_stats['local']+=1 if local=='True' else 0 # how many resources are local?


        p_stats['isURL']+=1 if isURL else 0 # how many are actual URLs
        p_stats['heads']+=1 if http_lm !='na' else 0 # for how many resources do have HTTP Head?


        p_stats['http_lm']+=1 if http_lm =='True' else 0 # how many resources have a header field?

        p_stats['meta_mis']+=1 if meta_lm =='mis' else 0 # how many resources have a metadata change time field?
        p_stats['meta_lm']+=1 if meta_lm !='mis' else 0 # how many resources have a metadata change time field?
        p_stats['meta_lm_value']+=1 if meta_lm=='value'  else 0 # how many resources have a value in the metadata change time field?
        p_stats['meta_lm_empty']+=1 if meta_lm=='empty' or meta_lm=='same'  else 0 # how many resources have a empty/static metadata change time field?

        p_stats['local_lm']+=1 if local=='True' and meta_lm=='value' else 0 #how many are local and have meta_lm
        p_stats['not_local_lm']+=1 if local!='True' and meta_lm=='value' else 0 #how many are not local and have metalm

        p_stats['no_lm']+=1 if http_lm != 'na' and http_lm=='False' and meta_lm =='mis' else 0 #how many are urls and have lm
        #print p_stats
        p_stats['distinct'].add(url)

    for i in range(3,len(h)):
        print i, h[i]


    p_stats_total={'isURL':0
                ,'total':0
                ,'local':0
                ,'heads':0
                ,'no_lm':0
                ,'http_lm':0
                ,'meta_lm':0
                ,'meta_mis':0
                ,'meta_lm_value':0
                ,'meta_lm_empty':0
                ,'local_lm':0
                ,'not_local_lm':0
                ,'distinct':set([])}


    hist={}
    hist_per_soft={'CKAN':{}, 'Socrata':{}, 'OpenDataSoft':{}}
    by_soft_stats={}
    for portal, stats in per_portal.items():

        soft_stats=by_soft_stats.setdefault(stats['software'],{
                'isURL':0
                ,'total':0
                ,'local':0
                ,'heads':0
                ,'http_lm':0
                ,'meta_lm':0
                ,'no_lm':0
                ,'meta_mis':0
                ,'meta_lm_value':0
                ,'meta_lm_empty':0
                ,'local_lm':0
                ,'not_local_lm':0
                ,'distinct':set([])
                                              })

        if 'distinct' in stats:
            p_stats_total['distinct'].update(stats['distinct'])
            soft_stats['distinct'].update(stats['distinct'])

            stats['distinct']=len(stats['distinct'])
        for k,v in stats.items():
            if k != 'distinct' and k != 'software':
                p_stats_total[k]+=v
                soft_stats[k]+=v


        local_ratio= stats['local']/(stats['total']*1.0)
        http_ratio= stats['isURL']/(stats['total']*1.0)
        head_ratio= stats['heads']/(stats['total']*1.0)
        http_lm_ratio= stats['http_lm']/(stats['heads']*1.0) if stats['heads']!=0 else 0

        meta_ratio= stats['meta_lm']/(stats['total']*1.0)
        meta_emtpy_ratio= stats['meta_lm_empty']/(stats['meta_lm']*1.0) if stats['meta_lm']!=0 else 0


        bins=[0,25,50,75,99,100]
        for h in [hist, hist_per_soft[stats['software']]]:

            hist_local= h.setdefault('local', { b:0 for b in bins})
            hist_local[getBin(local_ratio)]+=1

            hist_local= h.setdefault('url', { b:0 for b in bins})
            hist_local[getBin(http_ratio)]+=1

            hist_local= h.setdefault('head', { b:0 for b in bins})
            hist_local[getBin(head_ratio)]+=1

            hist_local= h.setdefault('http_lm', { b:0 for b in bins})
            hist_local[getBin(http_lm_ratio)]+=1

            hist_local= h.setdefault('meta', { b:0 for b in bins})
            hist_local[getBin(meta_ratio)]+=1

            hist_local= h.setdefault('meta_empty', { b:0 for b in bins})
            hist_local[getBin(meta_emtpy_ratio)]+=1


for soft, v in hist_per_soft.items():
    print soft
    print "&"+" &".join(v.keys())+"\\\\"
    for b in [0,25,50,75,99,100]:
        print b,
        print str(b)+"&"+" &".join([str(k[b]) for k in v.values()])+"\\\\"



from pprint import pprint
print "TOTAL"
p_stats_total['distinct']=len(p_stats_total['distinct'])
pprint(p_stats_total)

print "SoftStats"
for soft, stats in by_soft_stats.items():
    print soft
    stats['distinct']=len(stats['distinct'])
    pprint(stats)


import sys
sys.exit(0)
















    #     p_stats['software']=soft
    #
    #     for stats in [soft_stats,p_stats, total]:
    #         stats.setdefault('total',0)
    #         stats.setdefault('distinct',set([]))
    #
    #         stats['total']+=1
    #         stats['distinct'].add(row[0])
    #         for i in range(3,len(row)):
    #             c=stats.setdefault(i,{})
    #             c.setdefault(row[i],0)
    #             stats[i][row[i]]+=1
    #
    # import pprint
    # for i in range(3,len(h)):
    #     print i, h[i]
    #
    # for soft, stats in per_software.items():
    #     if 'distinct' in stats:
    #         stats['distinct']=len(stats['distinct'])
    #     print soft,stats['distinct'],stats['total']
    #     print stats
    #
    # for portal, stats in per_portal.items():
    #     if 'distinct' in stats:
    #         stats['distinct']=len(stats['distinct'])
    #
    #
    #
    # data=[]
    # age={}
    # for s,v in per_software.items():
    #     d={'total':v['total']}
    #     print v
    #     for k,vv in v.items():
    #         if isinstance(vv, dict):
    #             if 'False' in vv or 'True' in vv:
    #                 c=vv.get('True', 0)
    #                 d[h[k]]= (c / (sum(vv.values())*1.0))*100
    #             else:
    #                 d[h[k]+"_exists"]=((sum(vv.values())-vv['mis'])/(sum(vv.values())*1.0))*100
    #                 d[h[k]+"_value"]=((vv.get('value',0))/(sum(vv.values())*1.0))*100
    #
    #     data.append(d)
    #     httpv=v[4]
    #
    #     http_haveField=httpv.get('True', 0)
    #     http_total=httpv.get('True', 0)+httpv.get('False', 0)
    #     http_ratio= (http_haveField/(http_total*1.0)) if http_total!=0 else 0
    #
    #
    #
    #
    #
    #
    #     mv=v[6]
    #     #'mis': 5928, 'same': 360, 'value': 173652
    #     c=mv.get('value',0)
    #     t=mv.get('value',0)+mv.get('same',0)+mv.get('empty',0)
    #     if t==0:
    #         print s,mv
    #
    #     r_m_lm=(c/(t*1.0)) if t != 0 else 0
    #     print s, r_m_lm
    #     age[s]={
    #         'http_lm':{ 'total': sum(httpv.values())
    #                     ,'na':httpv.get('na',0)
    #                     ,'r':http_haveField
    #                     ,'ratio':http_ratio*100
    #                    },
    #         'meta_lm':{ 'total': sum(mv.values())
    #                     ,'mis':mv.get('mis',0)
    #                     ,'r':mv.get('value',0)
    #                     ,'ratio':r_m_lm*100
    #                    }
    #     }
    #
    #
    #
    #
    # import collections
    # local_ckan=collections.OrderedDict()
    # local_ckan[0]={ 'p':0,'r':0,'l':0}
    # local_ckan[25]={'p':0,'r':0,'l':0}
    # local_ckan[50]={'p':0,'r':0,'l':0}
    # local_ckan[75]={'p':0,'r':0,'l':0}
    # local_ckan[99]={'p':0,'r':0,'l':0}
    # local_ckan[100]={'p':0,'r':0,'l':0}
    #
    # x=[]
    # y=[]
    #
    # metadist={}
    # for s,v in per_portal.items():
    #     if v['software']=='CKAN':
    #         r_local= v[3].get('True', 0)/(sum(v[3].values())*1.0)
    #
    #         s=local_ckan[getBin(r_local)]
    #
    #
    #         s['p']+=1
    #         s['r']+=sum(v[3].values())
    #         s['l']+=v[3].get('True', 0)
    #
    #         x.append(r_local)
    #         vv=v[4]
    #         c=vv.get('True', 0)
    #         t= vv.get('True', 0)+vv.get('False', 0)
    #         r_lm= c/(t*1.0) if t!=0 else 0
    #         y.append(r_lm)
    #
    #
    #
    #     httpv=v[4]
    #
    #     http_haveField=httpv.get('True', 0)
    #     http_total=vv.get('True', 0)+vv.get('False', 0)
    #     http_ratio= (http_haveField/(http_total*1.0)) if http_total!=0 else 0
    #
    #
    #
    #     bins=[0,25,50,75,99,100]
    #     mdist=metadist.setdefault(v['software'],{})
    #
    #     dist= mdist.setdefault("http_lm",{})
    #
    #     for s in bins:
    #         dist.setdefault(s,{'total':0, 'mis':0, 'hasHeader':0, 'value':0, 'portals':0})
    #     s=getBin(http_ratio)
    #     d=dist[s]
    #
    #     d['portals'] +=1
    #     d['total'] +=sum(httpv.values())
    #     d['mis'] +=httpv.get('na',0)
    #     d['value'] +=http_haveField
    #     d['hasHeader'] +=http_total
    #
    #     mv=v[6]
    #     meta_total=sum(mv.values())
    #     meta_mis=mv.get('mis',0)
    #     meta_emtpy=mv.get('empty',0)+mv.get('same',0)
    #     meta_hasValue=mv.get('value',0)
    #     meta_ratio= 1-(meta_mis/(meta_total*1.0)) if meta_total != 0 else 0
    #
    #     dist= mdist.setdefault("meta_lm",{})
    #
    #     for s in bins:
    #         dist.setdefault(s,{'total':0, 'mis':0, 'hasMeta':0, 'value':0, 'portals':0})
    #     s=getBin(meta_ratio)
    #     d=dist[s]
    #
    #     d['portals']+=1
    #     d['total']+=meta_total
    #     d['mis']+=mv.get('mis',0)
    #     d['hasMeta'] +=(mv.get("same",0)+mv.get("value",0)+mv.get("empty",0))
    #     d['value']+=mv.get('value', 0)
    #
    #
    # import pandas as pd
    # df=pd.DataFrame(data)
    # #print df
    #
    #
    #
    # print 'LOCAL CKAN'
    # #########
    # # local
    #
    # total=per_software['CKAN']['total']
    # r=[]
    # l=[]
    # p=[]
    # for k, v in local_ckan.items():
    #     p.append(v['p'])
    #     l.append((v['l']/(1.0*v['r'])))
    #     r.append((v['r']/(1.0*total)))
    #
    # print "|p|", sum(p)
    # print "|l|", sum(l)
    # print "|r|", sum(r)
    #
    # print '-'*10
    # print "&".join(['0','$(0,0.25)$','$[0.25,0.5)$','$[0.5,0.75)$','$[0.75,1)$','1'])+"\\\\"
    #
    # print "&".join([str(i) for i in p])+"\\\\"
    # print "&".join(['{percent:.2%}'.format(percent=i) for i in r])+"\\\\"
    # print "&".join(['{percent:.2%}'.format(percent=i) for i in l])+"\\\\"
    # print '-'*10
    #
    # print 'AGE INFORMATION'
    #
    # for soft, mdist in metadist.items():
    #     print soft
    #     print '-'*10
    #
    #     totalres=per_software[soft]['total']
    #     print soft, totalres, type(totalres)
    #
    #
    #     #{'total':0, 'mis':0, 'hasHeader':0, 'value':0, 'portals':0})
    #
    #
    #     hasHeader=[]
    #     total=[]
    #     mis=[]
    #     portals=[]
    #     value=[]
    #     for i in [0,25,50,75,99,100]:
    #         v = mdist['http_lm'][i]
    #         portals.append(v['portals'])
    #         total.append((v['total']))
    #         mis.append((v['mis']))
    #         hasHeader.append((v['hasHeader']))
    #         value.append((v['value']))
    #
    #     print "|p|", sum(portals)
    #     print "|ds|", sum(total)
    #
    #
    #     print 'ratio$'+"&".join(['0','$(0,0.25)$','$[0.25,0.5)$','$[0.5,0.75)$','$[0.75,1)$','1'])+"\\\\"
    #     print '$|p|$&'+"&".join([str(i) for i in portals])+"\\\\"
    #     print 'total&'+"&".join([str(i) for i in total])+"\\\\"
    #     print 'value&'+"&".join([str(i) for i in value])+"\\\\"
    #     print 'hasHeader&'+"&".join([str(i) for i in hasHeader])+"\\\\"
    #     print 'mis&'+"&".join([str(i) for i in mis])+"\\\\"
    #     print '-'*10
    #
    #
    #
    #     hasMeta=[]
    #     total=[]
    #     mis=[]
    #     portals=[]
    #     value=[]
    #     for i in [0,25,50,75,99,100]:
    #         v = mdist['meta_lm'][i]
    #         portals.append(v['portals'])
    #         total.append((v['total']))
    #         mis.append((v['mis']))
    #         hasMeta.append((v['hasMeta']))
    #         value.append((v['value']))
    #
    #     print "|p|", sum(portals)
    #     print "|ds|", sum(total)
    #
    #     print "META"
    #     print '-'*10
    #     print 'ratio$'+"&".join(['0','$(0,0.25)$','$[0.25,0.5)$','$[0.5,0.75)$','$[0.75,1)$','1'])+"\\\\"
    #     print '$|p|$&'+"&".join([str(i) for i in portals])+"\\\\"
    #     print 'total&'+"&".join([str(i) for i in total])+"\\\\"
    #     print 'value&'+"&".join([str(i) for i in value])+"\\\\"
    #     print 'hasMeta&'+"&".join([str(i) for i in hasMeta])+"\\\\"
    #     print 'mis&'+"&".join([str(i) for i in mis])+"\\\\"
    #     print '-'*10
    #
    #
    # print '-'*10
    # print "&".join(['','','\\multicolumn{2}{c}{HTTP Header}','\\multicolumn{2}{c}{Metadata}' ])+"\\\\"
    # print "&".join(['software','|r|', 'missing','\%exist','missing','\%exist' ])+"\\\\"
    # for s,v in age.items():
    #     print "&".join([s,str(v['http_lm']['total']), str(v['http_lm']['na']),str(v['http_lm']['r']), str(v['meta_lm']['mis']),str(v['meta_lm']['r']) ])+"\\\\"
    # print '-'*10
    #
    #
    # import matplotlib.pyplot as plt
    # # plt.figure(figsize=(6, 6))
    # # plt.plot(x, y,marker='o', color='r', ls='')
    # # plt.xlabel('local ratio')
    # # plt.ylabel('http_lm ratio')
    # # plt.show()
    # #
    # # plt.savefig('temp.png')
    #
    # plt.figure(figsize=(6, 6))
    #
    # plt.scatter(x, y)
    # plt.xlabel('local ratio')
    # plt.ylabel('http_lm ratio')
    #
    # print "printing"
    # plt.savefig('temp.png')
    # print "printed"
    #
    #
    #
                
                