#!/usr/bin/env python
"""
Author: Kim Kiogora <kimkiogora@gmail.com>
Usage : Sample Impl of a load balancing lists (dummy queues)
"""

import itertools

queue_n1=[10,10,10,10]
queue_n2=[10]
queue_n3=[10,10,10,10,10,10]


all_queues = []
all_queues.append(queue_n1)
all_queues.append(queue_n2)
all_queues.append(queue_n3)

print "All queues"
print all_queues



"""Class to load balance queues"""
class LoadBalancer:
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
        print "Copying queue"
        copy_of_queues = queues[:]
        
        print "Done. Format queue and repopulate data"        

        for n,v in enumerate(queues):
            del v[:]
    
        "Formatted list"
        print "Formatted queues"
        print queues
        #[queues.append(10) for s in range(f_len*per_queue)]
        for n,v in enumerate(queues):
            for n in range(int(share)):
                v.append(10)

        print "Equally distributed queues"            
        print queues

        if remainder == 1:
            queues[0].append(10)
        elif remainder > 1:
            for i,v in enumerate(queues):
                if i < remainder:
                    v.append(10)
        
        
        print "Final load balanced queues"    
        print queues
            
        return self.balanced_queues


load_balancer = LoadBalancer()
result = load_balancer.balance(all_queues);

print "Balanced Queues"
print result
