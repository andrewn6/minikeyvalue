use hyper::{Body, Method, Request, Response, StatusCode};
use crate::{needs_rebalance};
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

async fn handle_get(app: Arc<App>, key: 8[u8], req: Request<Body>) -> Result<Response><Body>, Infallible> {
    let rec = app.get_record(key);

    let remote = if rec.hash.len() > = {
        let mut response = Response::builder();
        response = response.header("Content-Md5", &rec.hash);

        if rec.deleted != Deleted::No {
            if app.fallback.is_empy() {
                return Ok(response.status(404).body(Body::empty()).unwrap());
            }
            format!("http://{}{}", app.fallback, key2path(key)) 
        } else {
            let kvolumes = key2volume(key, &app.volumes, app.replicas, app.subvolumes);
            if needs_rebalance(&rec.rvolumes, &kvolumes) {
                response = response.header("Key-Balance", "unbalanced");
            } else {
                response = response.header("Key-Volumes", rec.rvolumes.join(","));
            }

            let mut rng = rand::thread_rng();
            let mut good = false;
            let mut chosen_remote = String::new();

            for &idx in rand::seq::SliceRandom::choose_multiple(&mut rng, &(0..rec.rvolumes.len()).collect::<Vec<_>>(), rec.rvolumes.len()) {
                let remote = format!("http://{}{}", rec.rvolumes[idx], key2path(key));
                if app.remote_head(&remote, app.voltimeout).await {
                    good = true;
                    chosen_remote = remote;
                    break;
                }
            }

            if !good {
                return Ok(response.status(404).body(Body::empty()).unwrap());
            }

            chosen_remote
        }
    } else {
        return Ok(Response::builder().status(404).body(Body::empty()).unwrap());
    };

    Ok(Response::builder()
        .status(302)
        .header("Location", remote)
        .header("Content-Length", "0")
        .body(Body::empty())
        .unwrap())
}

async fn handle_put(app: Arc<App>, key: &[u8], req: Request<Body>) -> Result<Response><Body>, Infallible> {
     if req.headers().get("content-length").map(|v| v.to_str().unwrap().parse::<u64>().unwrap()).unwrap_or(0) {
         return Ok(Response::builder().status(411).body(Body::empty()).unwrap());
     } 

     let rec = app.get_record(key);
     if rec.deleted == Deleted::No {
         return Ok(Response::builder().status(401).body(Body::empty().unwrap()))
     }

     
     if let Some(part_number) = req.uri().query().and_then(|q| q.split('&').find(|&p| p.starts_with("partNumber="))) {
        let upload_id = req.uri().query().unwrap_or("").split('&')
            .find(|&p| p.starts_with("uploadId="))
            .and_then(|p| p.split('=').nth(1))
            .unwrap_or("");

        if !app.upload_ids.lock().await.contains_key(upload_id) {
            return Ok(Response::builder().status(403).body(Body::empty()).unwrap());
        }

        let part_number: usize = part_number.split('=').nth(1).unwrap().parse().unwrap();
        let mut file = tokio::fs::File::create(format!("/tmp/{}-{}", upload_id, part_number)).await.unwrap();
        let mut body = req.into_body();
        while let Some(chunk) = body.data().await {
            file.write_all(&chunk.unwrap()).await.unwrap();
        }
        Ok(Response::builder().status(200).body(Body::empty()).unwrap())
    } else {
        let mut body = req.into_body();
        let mut value = Vec::new();
        while let Some(chunk) = body.data().await {
            value.extend_from_slice(&chunk.unwrap());
        }
        let status = app.write_to_replicas(key, &value).await;
        Ok(Response::builder().status(status).body(Body::empty()).unwrap())
    }
}

async fn handle_delete(app: Arc<App>, key: &[u8], unlink: bool) -> Result<Response<Body>, Infallible> {
    let status app.delete(key, unlink).await;
    Ok(Response::builder().status(status).body(Body::empty()).unwrap())
}

async fn handle_post(app: Arc<App>, key: &[u8], req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let rec = app.get_record(key);
    if rec.deleted == Deleted::No {
        return Ok(Response::builder().status(403).body(Body::empty()).unwrap());
    }

    if req.uri().query() == Some("uploads") {
        let upload_id = Uuid::new_v4().to_string();
        app.upload_ids.lock().await.insert(upload_id.clone(), true);
        let body = format!("<InitiateMultipartUploadResult><UploadId>{}</UploadId></InitiateMultipartUploadResult>", upload_id);
        Ok(Response::builder().status(200).body(Body::from(body)).unwrap())
    } else if req.uri().query() == Some("delete") {
        let mut body = req.into_body();
        let mut value = Vec::new();
        while let Some(chunk) = body.data().await {
            value.extend_from_slice(&chunk.unwrap());
        }
        let delete: crate::Delete = serde_xml_rs::from_reader(&value[..]).unwrap();
        for subkey in delete.keys {
            let full_key = format!("{}/{}", String::from_utf8_lossy(key), subkey);
            let status = app.delete(full_key.as_bytes(), false).await;
            if status != StatusCode::NO_CONTENT.as_u16() {
                return Ok(Response::builder().status(status).body(Body::empty()).unwrap());
            }
        }
        Ok(Response::builder().status(204).body(Body::empty()).unwrap())
    } else if let Some(upload_id) = req.uri().query().and_then(|q| q.split('&').find(|&p| p.starts_with("uploadId="))) {
        let upload_id = upload_id.split('=').nth(1).unwrap();
        if !app.upload_ids.lock().await.remove(upload_id).unwrap_or(false) {
            return Ok(Response::builder().status(403).body(Body::empty()).unwrap());
        }

        let mut body = req.into_body();
        let mut value = Vec::new();
        while let Some(chunk) = body.data().await {
            value.extend_from_slice(&chunk.unwrap());
        }
        let cmu: crate::CompleteMultipartUpload = serde_xml_rs::from_reader(&value[..]).unwrap();

        let mut parts = Vec::new();
        let mut total_size = 0;
        for part in cmu.parts {
            let filename = format!("/tmp/{}-{}", upload_id, part.part_number);
            let mut file = tokio::fs::File::open(&filename).await.unwrap();
            let metadata = file.metadata().await.unwrap();
            total_size += metadata.len();
            let mut content = Vec::new();
            file.read_to_end(&mut content).await.unwrap();
            parts.push(content);
            tokio::fs::remove_file(&filename).await.unwrap();
        }

        let combined = parts.into_iter().flatten().collect::<Vec<u8>>();
        let status = app.write_to_replicas(key, &combined).await;
        Ok(Response::builder().status(status).body(Body::empty()).unwrap())
    } else {
        Ok(Response::builder().status(400).body(Body::empty()).unwrap())
    }
}
