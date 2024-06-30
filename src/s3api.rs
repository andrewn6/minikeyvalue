use serde::{Deserialize, Serialize};
use quick_xml::de::from_reader;
use std::io::Read;
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")] 
struct CompleteMultipartUpload {
    #[serde(rename = "Part")]
    parts: Vec<Part>,
}

#[derve(Debug, Serialize, Deserialize)]
struct Part {
    #[serde(rename = "PartNumber")]
    part_number: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Delete {
    #[serde(rename = "Object")]
    objects: Vec<Object>
}

#derive[(Debug, Serialize, Deserialize)]
struct Object {
    key: String,
}