use actix_web::post;
use actix_web::web;
use actix_web::web::Data;
use actix_web::Responder;
use web::Json;

use crate::app::App;
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
        Ok(Json(str::from_utf8(&value).unwrap().to_string()))
    } else {
        Ok(Json("not found".to_owned()))
    }
}

#[post("/merge")]
pub async fn merge(app: Data<App>, req: Json<String>) -> actix_web::Result<impl Responder> {
    let mut state_machine = app.state_machine_store.state_machine.write().await;
    state_machine.data.merge();
    Ok(Json("done".to_owned()))
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
                Ok(Json(str::from_utf8(&value).unwrap().to_string()))
            } else {
                Ok(Json("not found".to_string()))
            }
        }
        Err(_) => Ok(Json("not found".to_string())),
    }
}
