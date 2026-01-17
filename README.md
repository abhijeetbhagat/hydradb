# HydraDB

a distributed KV store based on bitcask. 
- uses the openraft library for consensus.
- uses sledb for storing raft logs.
- uses a concurrent hashmap for caching file descriptors during reads. 
- append only log for fast writes.
- a read requires one seek operation.
- manual merging.
