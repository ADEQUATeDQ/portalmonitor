from odpw.reporting import plotting

__author__ = 'sebastian'

from reporting.reporters.reporters import Reporter, PlotReporter


class MultiScatterHistReporter(Reporter, PlotReporter):
    def __init__(self, data, hist_data, bins, labels, xlabel, ylabel, filename, colors=None):
        super(MultiScatterHistReporter, self).__init__()
        self.data = data
        self.hist_data = hist_data
        self.bins = bins
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.colors = colors

    def plotreport(self, dir):
        plotting.scatterplotHistplotComb(self.data, self.hist_data, self.bins, self.labels, self.xlabel, self.ylabel, dir, self.filename, colors=self.colors)


class MultiScatterReporter(Reporter, PlotReporter):
    def __init__(self, data, labels, xlabel, ylabel, filename, colors=None):
        super(MultiScatterReporter, self).__init__()
        self.data = data
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.colors = colors

    def plotreport(self, dir):
        plotting.scatterplotComb(self.data, self.labels, self.xlabel, self.ylabel, dir, self.filename, colors=self.colors)

class MultiHistogramReporter(Reporter, PlotReporter):
    def __init__(self, data, labels, xlabel, ylabel, filename, bins=None, colors=None, legend='upper right'):
        super(MultiHistogramReporter, self).__init__()
        self.data = data
        self.labels = labels
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.bins = bins
        self.colors = colors
        self.legend = legend

    def plotreport(self, dir):
        plotting.histplotComp(self.data, self.labels, self.xlabel, self.ylabel, dir, self.filename, bins=self.bins, colors=self.colors, legend=self.legend)

class HistogramReporter(Reporter, PlotReporter):
    def __init__(self, data, xlabel, ylabel, filename, bins=None, color=None):
        super(HistogramReporter, self).__init__()
        self.data = data
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.filename = filename
        self.bins = bins
        self.color = color

    def plotreport(self, dir):
        plotting.histplot(self.data, self.xlabel, self.ylabel, dir, self.filename, bins=self.bins, color=self.color)
