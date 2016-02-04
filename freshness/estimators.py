__author__ = 'sebastian'


class IntuitiveFrequencyEstimator:
    """
    X/T
    X: number of detected changes
    T: monitoring period
    n: number of accesses
    I: interval
    f: frequency = 1/I
    T = nI = n/f

    Poisson process
    X(t): number of occurences of a change in the interval (0,t]
    lambda: average frequency that a change occurs
    goal: estimate lambda, given X and T
    first estimate ratio r=lamda/f

    Estimator:
    r = X/n
    lambda = rf

    Estimating frequency of change,
    Cho, Junghoo
    Garcia-Molina, Hector
    """

    def __init__(self, p, interval):
        self.I = interval
        self.f = 1/float(self.I)

        revisions = p.iterContentSampling(self.I)
        self.n = len(revisions)
        self.X = revisions.count(1)
        self.r = self.X/float(self.n)

        print self.r
