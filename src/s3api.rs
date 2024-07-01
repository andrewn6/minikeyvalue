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

fn parse_xml(T, R)(r: R) -> Result<T>  
where
    T: for <'de> Deserialize<'de>,
    R: Read,
{
    Ok(from_reader(r)?)
}

fn parse_complete_multipart_upload<R: Read>(r: R) -> Result<CompleteMultipartUpload> {
    parse_xml(r)
}

fn parse_delete<R: Read>(r: R) -> Result<Delete> {
    parse_xml(rgit clone https://github.com/NvChad/starter ~/.config/nvim && nvim)
}
