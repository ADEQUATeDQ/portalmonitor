'''
Created on Aug 21, 2015

@author: jumbrich
'''
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from numpy.core.numeric import arange

from odpw.reporting.reporters.reporters import CSVReporter, UIReporter, CLIReporter,\
    Reporter, PlotReporter, TexTableReporter


class TimePeriodReporter(Reporter, CLIReporter, UIReporter, CSVReporter):
    
    def __init__(self, analyser):
        super(TimePeriodReporter, self).__init__(analyser)
        
    
    def getDataFrame(self):
        
        mind=self.a.getResult()['min']
        maxd=self.a.getResult()['max']
        
        d={}
        if mind:
            d['min']=mind.isoformat()
        if maxd:
              d['max']=maxd.isoformat()
        if mind and maxd:
            delta=(maxd-mind)
            d['delta_sec']= delta.total_seconds()
        if self.df is None:
            self.df = pd.DataFrame(d.items())
        return self.df
    
    def uireport(self):
        mind=self.a.getResult()['min']
        maxd=self.a.getResult()['max']
        
        d={'min':None, 'max':None,'delta_sec':None }
        if mind:
            d['min']=mind.strftime('%Y-%m-%d')
        if maxd:
              d['max']=maxd.strftime('%Y-%m-%d')
        if mind and maxd:
            delta=(maxd-mind)
            d['delta_sec']= delta.total_seconds()
        
        return {self.name():d}
    
class FetchTimePeriodReporter(TimePeriodReporter):
    pass
    
class HeadTimePeriodReporter(TimePeriodReporter):
    pass

class FetchProcessReporter(Reporter,PlotReporter, TexTableReporter):
    
    
    def __init__(self, analysers):
        self.an=analysers
        
    
    def getLabel(self,seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        #return "%d:%02d:%02d" % (h, m, s)
        return "%d:%02d" % (h, m)
        

    def getDataFrame(self):
        
        df =None
        for a in self.an:
            res = a.getResult()
            
            res['time'] = self.getLabel(max(res['durations']))
            del res['durations']
            
            df1 = pd.DataFrame(res,index=[0])
            if df is None:
                df = df1
            else:
                df = df.append(df1,ignore_index=True)
            
            
            
    
        df.set_index("sn")
        print df
    
    def plotreport(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)
            
            
        fig=plt.figure(figsize=(10, 6)) 
        
        # These are the "Tableau 20" colors as RGB.    
        tableau20 = [(31, 119, 180), (174, 199, 232), (255, 127, 14), (255, 187, 120),    
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),    
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),    
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),    
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]    
  
        # Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.    
        for i in range(len(tableau20)):    
            r, g, b = tableau20[i]    
            tableau20[i] = (r / 255., g / 255., b / 255.) 
        
        
        
        # Remove the plot frame lines. They are unnecessary chartjunk.    
        ax = plt.subplot(111)    
        ax.spines["top"].set_visible(False)    
        ax.spines["bottom"].set_visible(True)    
        ax.spines["right"].set_visible(False)    
        ax.spines["left"].set_visible(True)  
        
        # Ensure that the axis ticks only show up on the bottom and left of the plot.    
        # Ticks on the right and top of the plot are generally unnecessary chartjunk.    
        ax.get_xaxis().tick_bottom()    
        ax.get_yaxis().tick_left()
          
        #ax.set_yscale('log')
        ax.set_ylabel('time elapsed ($hh:mm$)')
        ax.set_xlabel('% of portals')
        
        sn=[]
        i=0
        for a in self.an:
            res = a.getResult()
            sn.append(res['sn'])
            d = res['durations']
            
            d_sorted = np.sort(np.array(d))
            p = 1. * arange(len(d)) / (len(d) - 1)
        
            # plot the sorted data:
            ax.plot(p, d_sorted,color=tableau20[i],lw=2.5)
            i+=1
        
        #import seaborn as sns   
        #ax.set_color_cycle(sns.color_palette("coolwarm_r",len(sn)))
        ax.set_xticklabels(['{:3.2f}%'.format(x*100) for x in ax.get_xticks()])
        plt.grid(True)
        plt.legend(sn, loc='upper left',prop={'size':16})
    
        labels = ax.get_yticks().tolist()
        #print labels
        
        i=0
        for t in labels:
            l= self.getLabel(int(t))
            labels[i]=l
            i+=1
        ax.set_yticklabels(labels)
        #plt.show()
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
             ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(16)
        
        plt.savefig(os.path.join(folder,"portal_fetch_process.pdf"),dpi = 300)
        
    def textablereport(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)
        """ 
                     & sn_t, sn_(t+1), sn_,,,,
            |p|      & 240
            fetched  & 230
            finished &
            time     & 3:33
        """
        
        df = self.getDataFrame()
        sn={}
        
        for a in self.an:
            res = a.getResult()
            
            res['time'] = self.getLabel(max(res['durations']))
            del res['durations']
            
            sn[res['sn']]=res
        
        import collections
        od = collections.OrderedDict(sorted(sn.items()))
        
        s=[]
        t=[]
        f=[]
        d=[]
        e=[]
        #"sn":self.sn, "total":self.total, "fetched":self.processed,  "error":self.error
        for sn, da in od.items():
            s.append(sn) 
            t.append(da['total'])
            f.append(da['fetched'])
            e.append(da['total']-da['fetched']-da['error'])
            d.append(da['time'])
            
        
        with open(os.path.join(folder,"sys_stats_table.tex"),"w+") as file:
            
            file.write("\\begin{tabular}{l"+ ("r"*len(s))+"} \n")
            
            file.write("\\toprule \n")
            #header
            st= "&".join( "\\texttt{"+str(x)+"}" for x in s)
            file.write("\\texttt{snapshot}  &"+st+"\\\\ \n")
            file.write("\\midrule \n")
            #body
            file.write(" $|p|$ &"+" &".join(str(x) for x in t)+"\\\\\n")
            file.write(" fetched&"+" &".join(str(x) for x in f)+"\\\\\n")
            file.write(" aborted&"+" &".join(str(x) for x in e)+"\\\\\n")
            file.write(" time ($hh$:$mm$)&"+" &".join(str(x) for x in d)+"\\\\\n")
            file.write("\\bottomrule \n")
            file.write("\\end{tabular} \n") 
