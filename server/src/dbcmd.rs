use anyhow::Result;
use core::sotradb::SotraDB;
use log::info;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

enum DBCmd {
    Put(String, String),
    Get(String),
    Del(String),
    ListAll,
    Invalid,
}

fn parse(cmd: &str) -> DBCmd {
    let arr: Vec<&str> = cmd.split_whitespace().collect();

    if arr[0].contains("put") {
        DBCmd::Put(arr[1].into(), arr[2].into())
    } else if arr[0].contains("get") {
        DBCmd::Get(arr[1].into())
    } else if arr[0].contains("del") {
        DBCmd::Del(arr[1].into())
    } else if arr[0].contains("listall") {
        DBCmd::ListAll
    } else {
        DBCmd::Invalid
    }
}

pub async fn process(mut stream: TcpStream, db: Arc<RwLock<SotraDB>>) -> Result<()> {
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
                    writer.write(format!("{}\n", val).as_bytes()).await?;
                    writer.flush().await?;
                } else {
                    writer
                        .write(format!("key {key} not found\n").as_bytes())
                        .await?;
                    writer.flush().await?;
                }
            }
            DBCmd::ListAll => {
                info!("command recvd: listall");
            }
            _ => {}
        }
        cmd.clear();
    }

    Ok(())
}
