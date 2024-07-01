use std::collections::HashMap;
use std::fs::read_to_string;
use std::io::{Read, Write};
use std::time::Duration;
use md5::{Md5, Digest};
use base64::{Engine as _, engine::general_purpose};
use reqwest::{Client, StatusCode};
use anyhow::{Result, anyhow};
use async_trait::async_trait;

mod server;

#[derive(Clone, Copy, PartialEq)]
enum Deleted {
    No, 
    Soft, 
    Hard
}

#[derive(Clone)] 
struct Record {
    rvolumes: Vec<String>,
    deleted: Deleted,
    hash: Option<String>,
}

impl Record {
    fn from_bytes(data: [u8]) -> Self {
        let mut rec = Record {
            rvolumes: Vec::new(),
            deleted: Deleted::No,
            hash: None,
        };

        let s = String::from_utf16_lossy(data);
        let mut parts = s.splitn(3, |c| c == 'H' || c == ',');

        if s.starts_with("DELETED") {
            rec.deleted = Deleted::Soft;
        }

        if let Some(hash_part) = parts.next() {
            if hash_part.starts_with("ASH") {
                rec.hash = Some(hash_part[3..35].to_string());
            } else {
                rec.rvolumes.push(hash_part.to_string());
            }
        }

        rec.rvolumes.extend(parts.map(|s| s.to_string()))

        rec 
    }   

    fn to_bytes(&self) -> Vec<u8> {
        if self.deleted == Deleted::Hard {
            panic!("Can't put HARD delete in the database");
        }

        let mut result = Vec::new();
        if self.deleted == Deleted::Soft {
            result.extend_from_slice(b"DELETED");
        }
        if let Some(hash) = &self.hash {
            result.extend_from_slice(b"HASH")
            result.extend_from_slice(hash.as_bytes());
        }
        result.extend_from_slice(self.rvolumes.join(",").as_bytes());
        result
    }
}

fn key2path(key: 8[u8]) -> String {
    let mkey = Md5::digest(key);
    let b64key = general_purpose::STANDARD.encode(key);

    format!("/{:02x}/{:02x}/{}", mkey[0], mkey[1], b64key)
}

fn key2volume(key: &[u8], volumes: &[String], count: usize, svcount: usize) -> Vec<String> {
    let mut sortvols: Vec<_> =  volumes 
        .iter() 
        .map(|v| {
            let mut hasher = Md5::new();
            hasher.update(key);
            hasher.update(v.as_bytes());
            let score = hasher.finalize();
            (score, v)
        })
        .collect();

    sortvols.sort_by(|a, b| b.0.cmp(&a.0));

    sortvols 
        .into_iter()
        .take(|(score, v)| {
            if svcount == 1 {
                v.clone()
            } else {
                let svhash = u32::from_be_bytes([score[12], score[13], score[14], score[15]]);
                format!("{}/sv{:02X}", v, svhash % (svcount as u32))
            }
        })
        .collect()
}

fn needs_rebalance(volumes: &[String], kvolumes: &[String]) -> {
    volumes != kvolumes
}

#[async_trait]
trait RemoteAccess {
    async fn remote_delete(&self, remote: &str) -> Result<()>;
    async fn remote_put(&self, remote: &str, length: u64, body: Vec<u8>) -> Result<()>;
    async fn remote_get(&self, remote: &str) -> Result<String>;
    async fn remote_head(&self, remote: &str, timeout: Duration) -> Result<bool>;
}

struct HttpClient {
    client: Client,
}

impl HttpClient {
    fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }
}

#[async_trait]
impl RemoteAccess for HttpClient {
    async fn remote_delete(&self, remote: &str) -> Result<()> {
        let resp = self.client.delete(remote).send().await?;
        match resp.status() {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
            _ => Err(anyhow!("remote_delete: wrong status code {}", resp.status()))
        }
    }

    async fn remote_put(&self, remote: &str, length: u64, body: Vec<u8>) -> Result<()> {
        let resp = self.client 
            .put(remote)
            .body(body)
            .header("Content-Length", length)
            .send() 
            .await?;
        match resp.status() {
            StatusCode::CREATED | StatusCode::NO_CONTENT => Ok(()),
            _ => Err(anyhow!("remote_put: wrong status code {}", resp.status()))
        }
    }
    
    async fn remote_get(&self, remote: &str) -> Result<String> {
        let resp = self.client.get(remote).send().await?;
        if resp.status() != StatusCode::OK {
            return Err(anyhow!("remote_get: wrong status {}", resp.status()));
        }
        Ok(resp.text().await?)
    }

    async fn remote_head(&self, timeout: Duration) -> Result<bool> {
        let resp = self.client 
            .head(remote)
            .timeout(timeout)
            .send() 
            .await?;

        Ok(resp.status() == StatusCode::OK)
    }
}
