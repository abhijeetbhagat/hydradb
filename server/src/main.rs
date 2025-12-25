use core::sotradb::SotraDB;
use log::info;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;

mod dbcmd;

use dbcmd::process;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    namespace: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    // TODO this namespace needs to come as a cli arg
    let mut db = Arc::new(RwLock::new(SotraDB::new(args.namespace)?));

    let listener = TcpListener::bind("127.0.0.1:9898").await?;
    info!("SotraDB v0.1.0 listening on localhost:9898");

    while let (socket, _) = listener.accept().await? {
        let db = db.clone();
        tokio::spawn(async move {
            let _ = process(socket, db).await;
        });
    }

    Ok(())
}
