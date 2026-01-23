use anyhow::Result;
use log::debug;
use serde_json::json;
use tokio::sync::Semaphore;

static PERMITS: Semaphore = Semaphore::const_new(100);

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let client = reqwest::Client::new();
    let mut v = vec![];

    for i in 0..100 {
        let key = format!("key-{:05}", i);
        let val = format!("val-{}", i);

        let payload = json!({
            "Put": {
                "key": key,
                "value": val
            }
        });

        let _permit = PERMITS.acquire().await.unwrap();

        let client = client.clone();
        let t = tokio::spawn(async move {
            if let Err(e) = client
                .post("http://localhost:9896/write")
                .json(&payload)
                .send()
                .await
            {
                debug!("error sending request: {e}")
            } else {
                debug!("write sent")
            }
        });
        v.push(t);
    }

    for t in v {
        let _ = t.await;
    }

    Ok(())
}
