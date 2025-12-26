use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use openraft::error::CheckIsLeaderError;
use openraft::error::Infallible;
use openraft::error::RaftError;
use openraft::BasicNode;
use web::Json;

use crate::app::App;
use crate::NodeId;
use crate::Request;

/**
 * Application API
 *
 * This is where you place your application, you can use the example below to create your
 * API. The current implementation:
 *
 *  - `POST - /write` saves a value in a key and sync the nodes.
 *  - `POST - /read` attempt to find a value from a given key.
 */
#[post("/write")]
pub async fn write(app: Data<App>, req: Json<Request>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(req.0).await;
    Ok(Json(response))
}

#[post("/del")]
pub async fn del(app: Data<App>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let response = app.raft.client_write(Request::Del { key: req.0 }).await;
    Ok(Json(response))
}

#[post("/read")]
pub async fn read(app: Data<App>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let state_machine = app.state_machine_store.state_machine.read().await;
    let key = req.0;
    tracing::info!("key {key}");
    let value = state_machine.data.get(&key);
    tracing::info!("val {:?}", value);
    if let Ok(Some(value)) = value {
        Ok(Json(value))
    } else {
        Ok(Json("not found".to_string()))
    }
}

#[post("/consistent_read")]
pub async fn consistent_read(
    app: Data<App>,
    req: Json<String>,
) -> actix_web::Result<impl Responder> {
    let ret = app.raft.ensure_linearizable().await;

    match ret {
        Ok(_) => {
            let state_machine = app.state_machine_store.state_machine.read().await;
            let key = req.0;
            let value = state_machine.data.get(&key);
            if let Ok(Some(value)) = value {
                return Ok(Json(value));
            } else {
                return Ok(Json("not found".to_string()));
            }
        }
        Err(e) => Ok(Json("not found".to_string())),
    }
}
