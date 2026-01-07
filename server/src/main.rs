use core::start_raft_node;
use log::info;

use anyhow::Result;
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    namespace: String,

    #[arg(short, long)]
    id: u64,

    #[arg(short, long)]
    port: u16,
}

#[actix_web::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    info!(
        "SotraDB v0.1.0 id: {} listening on localhost:{}",
        args.id, args.port
    );

    start_raft_node(args.id, args.port, args.namespace).await
}
