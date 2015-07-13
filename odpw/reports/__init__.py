from odpw.db.models import Portal
from odpw.quality.analysers import AnalyseEngine, PortalStatusAnalyser


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

class PlotCreator:
    def plot(self):
        pass
class TableCreator:
    def table(self):
        pass        



class PortalStatusReporter(PortalStatusAnalyser, PlotCreator, TableCreator):
    
    def plot(self):
        df=self.getDataFrame()
        
        plt.pie(df['count'], labels=df.lable)
        
        plt.show()    
    

