use std::collections::HashMap;
use std::fs::read_to_string;
use std::io::{Read, Write};
use std::time::Duration;
use md5::{Md5, Digest};
use base64::{Engine as _, engine::general_purpose};
use reqwest::{Client, StatusCode};
use anyhow::{Result, anyhow};
use async_trait::async_trait;

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