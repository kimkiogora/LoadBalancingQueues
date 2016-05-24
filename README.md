# LoadBalancing - Sample WorkStealing Python

This is an example, non optimized code for LoadBalancing and workstealing implemetation in Python for use in services such as Messaging

Dependencies
------------
Python 2.7 +

Sample Output
-------------
All queues
[[10, 10, 10, 10], [10, 10], [10, 10, 10, 10, 10, 10]]

Transaction size is 12 shared across 3 queues. Each queue => [4, 2, 6]
Each queue will process 4.0 transaction(s). The remainder 0 will be added to the first 0 queues
Copying queue
Done. Format queue and repopulate data

Formatted queues
[[], [], []]

Equally distributed queues
[[10, 10, 10, 10], [10, 10, 10, 10], [10, 10, 10, 10]]

Final load balanced queues
[[10, 10, 10, 10], [10, 10, 10, 10], [10, 10, 10, 10]]

Balanced Queues
[[10, 10, 10, 10], [10, 10, 10, 10], [10, 10, 10, 10]]
