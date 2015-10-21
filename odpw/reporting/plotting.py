
#scipy, numpy, matplotlib
#import scipy as sp
import numpy as np
import matplotlib as mpl
from matplotlib.ticker import FuncFormatter
from matplotlib import pyplot as plt
from matplotlib.gridspec import GridSpec

import collections
import itertools
import os


def createDir(dName):
    if not os.path.exists(dName):
        os.makedirs(dName)

def format_percent(x, at_least=2, tex = False):
    s = '{0:.2f}'.format(x*100, at_least)
    if s == "0.00":
        s = str(0)
    if s == "100.00":
        s = str(100)
    # The percent symbol needs escaping in latex
    if plt.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    elif tex:
        return s + '$\\%$'
    else:
        return s + '%'


def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(100 * y)

    # The percent symbol needs escaping in latex
    if plt.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'

def histplotComp(data, labels, xlabel, ylabel, dir, filename, bins=None, colors=None, legend='upper right'):
    if bins is None:
        bins = np.linspace(0,1,11)

    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 4.5))
    ax = plt.subplot(111)

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    plt.xticks(fontsize=14)

    xtics = []
    tics = []
    for i in range(len(bins)-1):
        if i == len(bins)-2:
            tics.append("["+str(bins[i])+" - "+str(bins[i+1])+" ]")
        else:
            tics.append("["+str(bins[i])+" - "+str(bins[i+1])+" [")
        xtics.append(i/10.0+0.05)

    #print xtics
    plt.xticks(xtics, tics, rotation=45)

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    width = 1 * (bins[1] - bins[0])/(len(data)+1)
    center = (bins[:-1] + bins[1:]) / 2
    formatter = FuncFormatter(to_percent)

    # Set the formatter
    plt.gca().yaxis.set_major_formatter(formatter)

    m = 0
    for i, d in enumerate(data):
        hist = data[d]['hist']
        #bin_edges = res[d]['bin_edges']

        m =  max(hist.max()*1.0 / hist.sum(), m)

        if colors is None:
            col = (i*.3, i*.3, i*.3)
        else:
            col = colors[i]
        bars = plt.bar( bins[:-1]+(width/2)+(width*i),hist.astype(np.float32)/hist.sum(), width = width, color=col, label=labels[d])
    plt.ylim([0,m+0.05])
    plt.legend(loc=legend)

    createDir(dir)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight")


def scatterplotComb(data, labels, xlabel, ylabel, dir, filename, colors=None):

    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    if not colors:
        colors = ['red', 'blue', 'green', 'black']

    plt.figure(figsize=(8, 6))

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.set_xlim([-0.05, 1.05])
    ax.set_ylim([-0.05, 1.05])

    plt.xticks(fontsize=14)

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    #plt.title(title)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    formatter = FuncFormatter(to_percent)
    # Set the formatter
    #plt.gca().yaxis.set_major_formatter(formatter)
    #plt.gca().xaxis.set_major_formatter(formatter)


    for i, d in enumerate(data):
        x = data[d][0]
        y = data[d][1]
        sc_plt = plt.scatter(x, y, s=10, color=colors[i], label=labels[d])

    plt.legend(scatterpoints=1,
               loc='upper left',
               ncol=4,
               fontsize=10)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight")

def scatterplotHistplotComb(data, labels, xlabel, ylabel, dir, filename, colors=None):

    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    if not colors:
        colors = ['red', 'blue', 'green', 'black']

    plt.figure(figsize=(8, 6))

    gs = GridSpec(2, 2, width_ratios=[1,2], height_ratios=[4,1])

    ax1 = plt.subplot(gs[0])
    ax2 = plt.subplot(gs[1])
    ax = plt.subplot(gs[2])
    ax4 = plt.subplot(gs[3])



    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.set_xlim([-0.05, 1.05])
    ax.set_ylim([-0.05, 1.05])

    plt.xticks(fontsize=14)

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    #plt.title(title)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    formatter = FuncFormatter(to_percent)
    # Set the formatter
    #plt.gca().yaxis.set_major_formatter(formatter)
    #plt.gca().xaxis.set_major_formatter(formatter)


    for i, d in enumerate(data):
        x = data[d][0]
        y = data[d][1]
        sc_plt = plt.scatter(x, y, s=10, color=colors[i], label=labels[d])

    plt.legend(scatterpoints=1,
               loc='upper left',
               ncol=4,
               fontsize=10)

    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight")



def histplot(hist, xlabel, ylabel, dir, filename, bins=np.linspace(0,1,11), color=None):
    # You typically want your plot to be ~1.33x wider than tall.
    # Common sizes: (10, 7.5) and (12, 9)
    plt.figure(figsize=(8, 6))

    # Remove the plot frame lines. They are unnecessary chartjunk.
    ax = plt.subplot(111)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Ensure that the axis ticks only show up on the bottom and left of the plot.
    # Ticks on the right and top of the plot are generally unnecessary chartjunk.
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    plt.ylim([0,1.05])
    plt.xticks(fontsize=14)
    ax.set_xticks(bins)

    #Labels
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)

    #plt.title(title)

    plt.rc('text', usetex=True)
    plt.rc('font', family='serif')

    width = 1 * (bins[1] - bins[0])
    center = (bins[:-1] + bins[1:]) / 2
    formatter = FuncFormatter(to_percent)

    # Set the formatter
    plt.gca().yaxis.set_major_formatter(formatter)

    # plt.bar(center, hist, align='center', width=width)
    # plt.hist(data, bins= bins, normed=1, cumulative=0)
    bars = plt.bar(bins[:-1],hist.astype(np.float32)/hist.sum(), width=width, color=color)

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            #print format_percent(height)
            ax.text(rect.get_x()+rect.get_width()/2., 1.02*height, format_percent(height),
                ha='center', va='bottom')

    autolabel(bars)

    createDir(dir)
    print "Saving figure to ", os.path.join(dir, filename)
    plt.savefig(os.path.join(dir, filename), bbox_inches="tight")
