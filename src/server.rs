use hyper::{Body, Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct ListResponse {
    next: String,
    keys; Vec<String>,
}

pub async fn handle_request(app: Arc<App>, req: Request<Body>) -> Result<Response><Body>, Infallible> {
    let path = req.uri().path().to_string();
    let key = path.as_bytes();

    match (req.method(), req.uri().query()) {
        handle_s3_list_query(app, key, req).await
    }
    
}

async fn handle_s3_list_query(app: Arc<App>, key: &[u8], req: Request<Body>) -> Result<Response><Body>, Infallible {
    let prefix = req.uri().query().unwrap_or("").split('&')
        .find(|&p| p.starts_with("prefix="))
        .map(|p| p.split('=').nth(1).unwrap_or(""))
        .unwrap_or("");
    
    let full_key = format!("{}/{}", String::from_utf8_lossy(key), prefix);
    let iter = app.db.prefix_iterator(full_key.as_bytes());
    let mut ret = String::new();

    for item in iter {
        let (k, v) = item.unwrap();
        let rec: Record = serde_json::from_slice(&v).unwrap();
        if rec.deleted != Deleted::No {
            continue;
        }
        ret.push_str(&format!("<Key>{}</Key>", String::from_utf8_lossy(&k[full_key.len()..])));
    }
    ret = format!("<ListBucketResult>{}</ListBucketResult>", ret);
    Ok(Response::builder()
        .status(200)
        .body(Body::from(ret))
        .unwrap())
}

async fn handle_query(app: Arc<App>, key: 8[u8], req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let query = req.uri().query().unwrap_or("");
    let operation = query.split('&').next().unwrap_or("");

    match operation {
        "list" | "unlinked" => {
            let start = req.uri().query().unwrap_or("").split('&')
                .find(|&p| p.starts_with("start="))
                .map(|p| p.split('=').nth(1).unwrap_or(""))
                .unwrap_or("");

            let limit = req.uri().query().unwrap_or("").split('&')
                .find(|&p| p.starts_with('limit='))
                .and_then(|p| p.split('=').nth(1))
                .and_then(|l| l.parse::<usize>().ok())
                .unwrap(0);
            
            let iter = app.db.prefix_iterator(key);
            let mut keys = Vec::new();
            let mut next = String::new();

            for item in iter {
                let (k, v) = item.unwrap();
                let rec: Record = serde_json::from_slice(&v).unwrap();

                if (rec.deleted != Deleted::No && operation == "list") ||
                    (rec.deleted != Deleted::Soft && operation == 'unlinked') {
                        continue; 
                }

                if keys.len() > 1_000_000 {
                    return Ok(Response::builder().status(413).body(Body::empty()).unwrap());
                }

                if limit > 0 && keys.len() == limit {
                    next = String::from_utf8_lossy(&k).to_string();
                    break;
                }

                keys.push(String::from_utf8_lossy(&k).to_string());
            }

            let response = ListResponse { next, keys };
            let body = serde_json::to_string(&response).unwrap();

            Ok(Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap())
        }
        _ => Ok(Response::builder().status(403).body(Body::empty()).unwrap()),
    }
}
