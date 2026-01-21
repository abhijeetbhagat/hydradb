# HydraDB

a distributed KV store based on bitcask. 
- uses the openraft library for consensus.
- uses sledb for storing raft logs.
- uses a concurrent hashmap for caching file descriptors during reads. 
- append only log for fast writes.
- a read requires one seek operation.
- manual merging.

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
