#!/usr/bin/python

import socket
from time import time

def resolve_slow(hosts):
    """
    Given a list of hosts, resolves them and returns a dictionary
    containing {'host': 'ip'}.
    If resolution for a host failed, 'ip' is None.
    """
    resolved_hosts = {}
    for host in hosts:
        try:
            host_info = socket.gethostbyname(host)
            resolved_hosts[host] = host_info
        except socket.gaierror, err:
            resolved_hosts[host] = None
    return resolved_hosts

if __name__ == "__main__":

    hosts=["www.comune.terzolas.tn.it", "www.reddit.com", "www.nonexistz.net"]
    start = time()
    resolved_hosts = resolve_slow(hosts)
    end = time()

    print "It took %.2f seconds to resolve %d hosts." % (end-start, len(hosts))