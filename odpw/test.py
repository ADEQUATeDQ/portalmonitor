__author__ = 'jumbrich'

from ConfigParser import SafeConfigParser

if __name__ == '__main__':
    p=SafeConfigParser()
    p.read('odpw.ini')
    print p.get('db', 'port')

    s=['t','p','d']
    print ' AND '.join(s)



    import faststat

    stats=faststat.Stats()
    for i in range(1,100):
        stats.add(i)

    print stats.n