mod raft;

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

    #[clap(short, long, default_value = "1000")]
    logs_per_snapshot: u64,

    #[clap(short, long, default_value = "500")]
    snapshot_retention: u64,
}

#[actix_web::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    info!(
        "HydraDB v0.2.0 id: {} listening on localhost:{}",
        args.id, args.port
    );

    raft::start_raft_node(
        args.id,
        args.port,
        args.namespace,
        args.logs_per_snapshot,
        args.snapshot_retention,
    )
    .await
}
