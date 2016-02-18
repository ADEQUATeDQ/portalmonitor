'''
Created on Aug 6, 2015

@author: jumbrich
'''
import matplotlib.pyplot as plt
import numpy as np
from numpy.core.numeric import arange

from odpw.analysers import AnalyserSet, process_all
from odpw.analysers.process_period_analysers import FetchProcessAnalyser
from odpw.db.dbm import PostgressDBM
from odpw.db.models import PortalMetaData


def getLabel(seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        #return "%d:%02d:%02d" % (h, m, s)
        return "%d:%02d" % (h, m)

if __name__ == '__main__':
    
    dbm = PostgressDBM(host="portalwatch.ai.wu.ac.at", port=5432)

    #id='data_wu_ac_at'
    snapshots=[1533, 1534, 1535, 1536, 1537, 1538, 1539, 1540, 1541,1542]
    
    data={}
    for sn in snapshots:
        it =PortalMetaData.iter(dbm.getPortalMetaDatas(snapshot=sn))
        aset = AnalyserSet()
    
        fpa= aset.add(FetchProcessAnalyser())
    
        process_all(aset, it)
    
        data[sn]= fpa.getResult()
        print sn," ",len(data[sn])
    
    #plot
    
    fig=plt.figure(figsize=(8,6), dpi=80)
    ax = fig.add_subplot(111)
    ax.set_yscale('log')
    ax.set_ylabel('time elapsed ($hh:mm$)')
    ax.set_xlabel('Portals')
        
    for sn, d in data.items():
        
        d_sorted = np.sort(np.array(d))
        p = 1. * arange(len(d)) / (len(d) - 1)
        
        # plot the sorted data:
        ax.plot(p,d_sorted)
        
    ax.set_xticklabels(['{:3.2f}%'.format(x*100) for x in ax.get_xticks()])
    plt.grid(True)

    
    labels = ax.get_yticks().tolist()
    #print labels
    
    i=0
    for t in labels:
        l= getLabel(int(t))
        labels[i]=l
        i+=1
    ax.set_yticklabels(labels)
    #plt.show()
    
    plt.savefig("portal_process.pdf")
    
    
    #--------------------------------- pmd = dbm.getPortalMetaDatas(snapshot=sn)
    #-------------------------------- #portals = dbm.getPortals(software='CKAN')
#------------------------------------------------------------------------------ 
    #---------------------------------------------- #iter = Portal.iter(portals)
    #------------------------------------------- iter = PortalMetaData.iter(pmd)
#------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------ 
    #---------------------------------------------------------- for pmd in iter:
        #--------------------------------------------------------- import pprint
        #------------------------------------------- pprint.pprint(pmd.qa_stats)
#------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------ 
#------------------------------------------------------------------------------ 
    #-------------------------------------------------------- ae = AnalyserSet()
    #-------- #ca=ae.add(ElementCountAnalyser(funct=lambda portal: portal.iso3))
    #---------------- bins = [0,50,100,500,1000,5000,10000,50000,100000,1000000]
    #----------------- ds_histogram = ae.add(PMDDatasetCountAnalyser(bins=bins))
#------------------------------------------------------------------------------ 
    #----------------------------------------------------- process_all(ae, iter)
#------------------------------------------------------------------------------ 
    #------------- #re = ElementCountReporter(ca, ['Country', 'Count'], topK=20)
    #---------------------------- print 'ds_histogram', ds_histogram.getResult()
    #---------------------------------------------------- #engine = Report([re])
    #-------------------------------------------------- #engine.csvreport('tmp')

