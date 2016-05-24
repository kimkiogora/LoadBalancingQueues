#!/usr/bin/env python
"""
Author: Kim Kiogora <kimkiogora@gmail.com>
Usage : Sample Impl of a load balancing lists (dummy queues)

Class to load balance queues
"""
class Balancer:
    global balanced_queues

    def __init__(self):
        self.balanced_queues = []

    def balance(self,queues):
        f_len = len(queues)
        per_queue = map(lambda x: len(x), queues)
        total = reduce(lambda x,y: x+y, per_queue)

        print  "Transaction size is {0} shared across {1} queues. Each queue => {2}".format(total,f_len,per_queue)
        
        share = round( total / f_len)
        remainder = total % f_len
         
        print "Each queue will process {0} transaction(s). The remainder {1} will be added to the first {1} queues".format(share,remainder)
        
        #restructure the queues
        print "Format queue and repopulate data"        

        for n,v in enumerate(queues):
            del v[:]
    
        "Formatted list"
        print "Formatted queues"
        print queues
        
        for n,v in enumerate(queues):
            for n in range(int(share)):
                v.append(10)

        print "Equally distributed queues"            
        print queues
        if remainder == 0:
            self.balanced_queues = queues
            return self.balanced_queues

        if remainder == 1:
            queues[0].append(10)
        elif remainder > 1:
            for i,v in enumerate(queues):
                if i < remainder:
                    v.append(10)
        
        
        print "Final load balanced queues"    
        print queues
        
        self.balanced_queues = queues
        return self.balanced_queues
