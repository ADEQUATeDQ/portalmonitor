from time import sleep
__author__ = 'jumbrich'

from ConfigParser import SafeConfigParser



from multiprocessing import Process, Pool

def fetch(i):
    print "fetch",i
    sleep(10)

if __name__ == '__main__':
    ps= range(10)
    
    
    nop=4
    
    processes = {}
    
    for i in ps:
        
        p = Process(target=fetch, args=((i,)))
        p.start()
        print "Started",i, 'with', p.pid
        processes[i]=(p.pid, p)
            
        while len(processes) >= nop:
            for n in processes.keys():
                
                (pid,process) = processes[n]
                print "pid", pid, process.is_alive(), process.exitcode
                if not process.is_alive():
                    status = process.exitcode
                    print (pid, 'finished')
                    process.join() # Allow tidyup
                    del processes[n] # Removed finished items from the dictionary
            sleep(1)
                # When none are left then loop will end
