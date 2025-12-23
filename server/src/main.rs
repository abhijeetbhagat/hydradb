use core::sotradb::SotraDB;
use tokio::sync::mpsc::Sender;

use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<DBCmd>(100);

    std::thread::spawn(move || -> Result<()> {
        // TODO this namespace needs to come as a cli arg
        let mut db = SotraDB::new("name-to-addresses")?;
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                DBCmd::Put(key, val) => {
                    db.put(key, val);
                }
                DBCmd::Get(key) => db.get(key),
                _ => {}
            }
        }

        Ok(())
    });

    let listener = TcpListener::bind("127.0.0.1:9898").await?;
    while let (socket, _) = listener.accept().await? {
        let tx = tx.clone();
        tokio::spawn(async move {
            process(socket, tx).await;
        });
    }

    Ok(())
}

async fn process(stream: TcpStream, tx: Sender<DBCmd>) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut cmd = String::new();
    loop {
        reader.read_line(&mut cmd).await?;
        tx.send(parse(&cmd)).await?;
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
