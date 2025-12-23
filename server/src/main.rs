use core::sotradb::SotraDB;
use log::info;
use std::sync::{Arc, RwLock};
use tokio::{io::AsyncWriteExt, sync::mpsc::Sender};

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    // TODO this namespace needs to come as a cli arg
    let mut db = Arc::new(RwLock::new(SotraDB::new("name-to-addresses")?));

    let listener = TcpListener::bind("127.0.0.1:9898").await?;
    info!("SotraDB v0.1.0 listening on localhost:9898");
    while let (socket, _) = listener.accept().await? {
        let db = db.clone();
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }

    Ok(())
}

async fn process(mut stream: TcpStream, db: Arc<RwLock<SotraDB>>) -> Result<()> {
    let (reader, writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);
    let mut cmd = String::new();
    loop {
        reader.read_line(&mut cmd).await?;
        let db_cmd = parse(&cmd);
        match db_cmd {
            DBCmd::Put(key, val) => {
                info!("command recvd: put {key} {val}");
                let mut guard = db.write().unwrap();
                guard.put(key, val);
            }
            DBCmd::Get(key) => {
                info!("command recvd: get {key}");
                let val;
                {
                    let guard = db.read().unwrap();
                    val = guard.get(&key);
                }
                info!("db returned {:?}", val);
                if let Ok(Some(val)) = val {
                    writer.write(val.as_bytes()).await?;
                    writer.flush().await?;
                } else {
                    writer
                        .write(format!("key {key} not found").as_bytes())
                        .await?;
                    writer.flush().await?;
                }
            }
            _ => {}
        }
        cmd.clear();
    }

    Ok(())
}

enum DBCmd {
    Put(String, String),
    Get(String),
    Del(String),
    Invalid,
}

fn parse(cmd: &str) -> DBCmd {
    let arr: Vec<&str> = cmd.split_whitespace().collect();

    if arr[0].contains("put") {
        return DBCmd::Put(arr[1].into(), arr[2].into());
    } else if arr[0].contains("get") {
        return DBCmd::Get(arr[1].into());
    } else if arr[0].contains("del") {
        return DBCmd::Del(arr[1].into());
    } else {
        return DBCmd::Invalid;
    }
}
