"""
Balancer Unit Tests Class
"""
#!/usr/bin/env python
"""
Author: Kim Kiogora <kimkiogora@gmail.com>
Usage : Sample Impl of a load balancing lists (dummy queues)
"""
from LoadBalancer import Balancer

queue_n1=[10,10,10,10]
queue_n2=[10]
queue_n3=[10,10,10,10,10,10]


all_queues = []
all_queues.append(queue_n1)
all_queues.append(queue_n2)
all_queues.append(queue_n3)

print "All queues"
print all_queues

load_balancer = LoadBalancer()
result = load_balancer.balance(all_queues);

print "Balanced Queues"
print result
