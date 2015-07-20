from odpw.db.models import Portal
from odpw.quality.analysers import AnalyseEngine, PortalStatusAnalyser,\
    PortalMetaDataStatusAnalyser

from isoweek import Week
import matplotlib.pyplot as plt


from vincent import (Visualization, Scale, DataRef, Data, PropertySet,
                     Axis, ValueRef, MarkRef, MarkProperties, Mark)



class ReportingEngine(AnalyseEngine):
    
    def generatePlots(self):
        for c in self.analysers.itervalues():
            c.plot()
    
    
class Reporter:
    def __init__(self, analyser):
        self.analyer = analyser
        


#####
# OUTPUT
###
class VegaDataCreator:
    def getVegaData(self):
        pass
class PlotCreator:
    def plot(self):
        pass
class TableCreator:
    def table(self):
        pass        

from datetime import datetime, timedelta, date
def tofirstdayinisoweek(year, week):
    ret = datetime.strptime('%04d-%02d-1' % (year, week), '%Y-%W-%w')
    if date(year, 1, 4).isoweekday() > 4:
        ret -= timedelta(days=7)
    return ret


class PortalMetaDataStatusReporter(PortalMetaDataStatusAnalyser, VegaDataCreator):

    def getVegaData(self):
        data=[]
        for snapshot, statusDist in self.getDist().items():
            for status , val in statusDist.items():
                if status!='total':
                    year = snapshot.split("-")[0]
                    week=  snapshot.split("-")[1]
                    date =str(tofirstdayinisoweek(int(year), int(week)))
                    
                    data.append({'idx':date, "col":val['label'],"col1":status, 'val': val['count']})
        return data
        
class PortalStatusReporter(PortalStatusAnalyser, PlotCreator, TableCreator):
    
    def plot(self):
        df=self.getDataFrame()
        
        plt.pie(df['count'], labels=df.lable)
        
        plt.show()    
    

