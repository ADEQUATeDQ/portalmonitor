from time import sleep
__author__ = 'jumbrich'

from ConfigParser import SafeConfigParser



from multiprocessing import Process, Pool

def fetch(i):
    print "fetch",i
    sleep(50)

if __name__ == '__main__':
    ps= range(10)
    
    
    nop=4
    
    processes = {}
    
    for i in ps:
        
        p = Process(target=fetch, args=((i,)))
        p.start()
        print "Started",i, 'with', p.pid
        processes[i]=p.pid
            
        while len(processes) > 0:
            for n in processes.keys():
                (p,a) = processes[n]
                sleep(0.5)
                if p.exitcode is None and not p.is_alive(): # Not finished and not running
                    # Do your error handling and restarting here assigning the new process to processes[n]
                    print p,a, "Not finished and not running "
                elif p.exitcode < 0:
                    print ('Process Ended with an error or a terminate', a)
                    # Handle this either by restarting or delete the entry so it is removed from list as for else
                else:
                    print (a, 'finished')
                    p.join() # Allow tidyup
                    del processes[n] # Removed finished items from the dictionary 
                # When none are left then loop will end
