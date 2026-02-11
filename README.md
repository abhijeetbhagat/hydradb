# HydraDB

a distributed KV store based on bitcask. 
- uses the openraft library for consensus.
- uses sledb for storing raft logs.
- uses a concurrent hashmap for caching file descriptors during reads. 
- append only log for fast writes.
- a read requires one seek operation.
- manual merging/compaction.
- snapshotting support.

## Use as a library

```rust

    let mut db = HydraDBBuilder::new()
    .with_cask("data")
    .with_file_limit(60)
    .build()?;

    db.put("abhi", "rust")?;
    db.put("ashu", "java")?;

    let val = db.get("abhi")?;

    db.del("abhi")?;
    
```

## Use as a distributed KV store

1. spin up the leader node
```
./server --namespace test --id 1 --port 9896 > leader.log 2&>1 &
```

2. spin up 2 follower nodes
```
./server --namespace test --id 2 --port 9896 > follower1.log 2&>1 &
./server --namespace test --id 3 --port 9896 > follower2.log 2&>1 &
```

3. initialize leader
```
curl 'http://localhost:9896/init' -X POST -H "Content-Type: application/json" --data '[]' 
```

4. add learners (wait for them to catchup to the leader before making them followers) 
```
curl 'http://localhost:9896/add-learner' -X POST -H "Content-Type: application/json" --data '[2, "127.0.0.1:9897"]'
curl 'http://localhost:9896/add-learner' -X POST -H "Content-Type: application/json" --data '[3, "127.0.0.1:9898"]'
```

5. make learners as followers
```
curl 'http://localhost:9896/change-membership' -X POST -H "Content-Type: application/json" --data '[1,2,3]'
```

6. write something to the leader
```
curl 'http://localhost:9896/write' -X POST  -H "Content-Type: application/json" --data '{"Put":{"key":"ashu", "value":"rust"}}'
```

7. read it from the follower1
```
curl 'http://localhost:9897/read' -X POST  -H "Content-Type: application/json" --data '"ashu"'
```
