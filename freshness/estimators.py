import math
from statsmodels.distributions.empirical_distribution import ECDF
import matplotlib.pyplot as plt
import datetime

__author__ = 'sebastian'

class Estimator:
    def __init__(self):
        self.N = 0.0 # total number of accesses
        self.X = 0.0 # number of detected changes
        self.T = 0.0 # sum of the times from changes

    def estimate(self):
        raise NotImplementedError()


class ContentSampling(Estimator):
    def setInterval(self, I):
        self.I = I

    def update(self, Xi):
        raise NotImplementedError()


class AgeSampling(Estimator):
    def setStart(self, S):
        self.S = S

    def update(self, Ti, Ii):
        raise NotImplementedError()



class ChoGarciaFrequencyEstimator(ContentSampling):
    """
    X/T
    X: number of detected changes
    T: monitoring period
    N: number of accesses
    I: interval
    f: frequency = 1/I
    T = nI = n/f

    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """
    def update(self, Xi):
        self.N += 1
        # Has the element changed? (X is 0 or 1)
        self.X += Xi


class IntuitiveFrequency(ChoGarciaFrequencyEstimator):
    """
    Poisson process:
    X(t): number of occurences of a change in the interval (0,t]
    lambda: average frequency that a change occurs
    goal: estimate lambda, given X and T
    estimate ratio r=lamda/f

    Estimator:
    r = X/n
    lambda = rf
    """
    def estimate(self):
        self.f = 1/float(self.I)

        self.r = self.X/float(self.N)
        return self.r * self.f


class ImprovedFrequency(ChoGarciaFrequencyEstimator):
    """
    r = -log((n - X + 0.5)/(n + 0.5))
    """
    def estimate(self):
        self.f = 1/float(self.I)

        self.r = -math.log((self.N - self.X + 0.5)/(self.N + 0.5))
        return self.r * self.f


class ChoGarciaLastModifiedEstimator(AgeSampling):
    """
    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """
    def update(self, Ti, Ii):
        t = (Ti - self.S).total_seconds()
        # Ti is the time to the previous change in the ith access
        self.N += 1
        # Has the element changed?
        if t < Ii:
            self.X += 1
            self.T += t
        else:
            # element has not changed
            self.T += Ii


class NaiveLastModified(ChoGarciaLastModifiedEstimator):
    def estimate(self):
        return self.X/self.T if self.T > 0 else -1


class ImprovedLastModified(ChoGarciaLastModifiedEstimator):
    def estimate(self):
        if self.T > 0:
            return -1
        x = (self.X - 1) - self.X/(self.N * math.log(1 - self.X/self.N))
        return x/self.T



class EmpiricalDistribution:
    def __init__(self, deltas):
        self.deltas = deltas

    def plotDistribution(self):
        days = [t/86400 for t in self.deltas]
        cdf = ECDF(days)
        days.sort()
        F = cdf(days)
        plt.step(days, F)
        plt.show()


class AgeSamplingEmpiricalDistribution(AgeSampling):
    def __init__(self):
        AgeSampling.__init__(self)
        self.dates = set()

    def update(self, Ti, Ii):
        self.dates.add(Ti)

    def plotDistribution(self):
        deltas = []
        prev = None
        for d in sorted(self.dates):
            if prev:
                deltas.append((d- prev).total_seconds())
            prev = d

        days = [t/86400 for t in deltas]
        cdf = ECDF(days)
        days.sort()
        F = cdf(days)
        plt.step(days, F)
        plt.show()