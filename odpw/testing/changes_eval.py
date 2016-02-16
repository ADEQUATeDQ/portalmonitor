'''
Created on Feb 11, 2016

@author: jumbrich
'''
import operator

if __name__ == '__main__':
    dataFile="/Users/jumbrich/Dev/odpw/data/changes.pkl"
    
    import pickle
    data=pickle.load(open(dataFile))
    
    
    with open("/Users/jumbrich/Dev/odpw/data/changes.txt",'w') as out:
        for p in data:
            out.write(p+"\n")
            out.write("--\n")
            print p
            sorted_x = sorted(data[p].items(), key=lambda kv: kv[1], reverse=True)
            for k, v in sorted_x:
                out.write(" "+k+","+str(v)+"\n")
                print ' ', k,v 
            out.write("--\n")