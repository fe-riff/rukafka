extern crate kafka;
extern crate byteorder;
extern crate serde_json;

use std::collections::HashMap;
use std::str;
use byteorder::BigEndian;
use byteorder::ByteOrder;

pub fn from_kafka(bytes: &[u8]) -> Result<Message, &str> {
    let mut offset: usize = 0;
    if bytes[offset] != 0xff {
        return Err("expected 0xff as the leading byte");
    }
    offset += 1;

    let header_count = bytes[offset];
    offset += 1;
    println!("header_count: {}", header_count);

    let mut headers: HashMap<String, [String; 1]> = HashMap::new();
    for _i in 0..header_count {
        let len = bytes[offset] as usize;
        offset += 1;

        let name = match str::from_utf8(&bytes[offset..offset + len]) {
            Ok(n) => n,
            Err(_e) => return Err("invalid UTF-8 detected")
        };
        offset += len;

        let len = BigEndian::read_u32(&bytes[offset..offset + 4]) as usize;
        println!("big endian len: {}", len);
        offset += 4;
        let s = str::from_utf8(&bytes[offset..offset + len]).unwrap();
        println!("about to deserialise {}", s);

        let value = serde_json::from_str(s).unwrap();

        headers.insert(String::from(name), value);
        offset += len;
    }
    Ok(new_message(&bytes[offset..], headers))
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message<'a> {
    pub payload: &'a [u8],
    pub headers: HashMap<String, [String; 1]>, // TODO: support headers with more than one value in the list
}

pub fn new_message<'a>(bytes: &'a [u8], headers: HashMap<String, [String; 1]>) -> Message<'a> {
    Message {
        payload: bytes,
        headers: headers,
    }
}

pub fn to_kafka<'a>(message: Message) -> Vec<u8> {
    let num_headers = message.headers.len();
    let mut header_values: HashMap<String, String> = HashMap::new();
    for (k, v) in message.headers {
        let hv = json!(v);
        header_values.insert(k, hv.to_string());
    }
    let mut result: Vec<u8> = Vec::new();
    result.push(0xff);
    result.push(num_headers as u8);
    for (k, v) in header_values {
        println!("encoding header {} with value {}", k, v);
        let l = k.chars().count();
        result.push(l as u8);
        for c in k.chars() {
            result.push(c as u8);
        }
        let bytes: [u8; 4] = transform_u32_to_array_of_u8(v.len() as u32);
        result.extend_from_slice(&bytes);
        for c in v.chars() {
            result.push(c as u8);
        }
    }
    result.extend_from_slice(&message.payload);

    result
}

fn transform_u32_to_array_of_u8(x: u32) -> [u8; 4] {
    let b1: u8 = ((x >> 24) & 0xff) as u8;
    let b2: u8 = ((x >> 16) & 0xff) as u8;
    let b3: u8 = ((x >> 8) & 0xff) as u8;
    let b4: u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_ok_no_headers() {
        let headers = HashMap::new();
        let msg = new_message("hello".as_bytes(), headers);


        assert_eq!(msg, from_kafka(to_kafka(msg.clone()).as_ref()).unwrap());
    }

    #[test]
    fn roundtrip_headers() {
        let mut headers = HashMap::new();
        headers.insert(String::from("a"), [String::from("vwxyz"); 1]);
        let msg = new_message("hello".as_bytes(), headers);


        assert_eq!(msg, from_kafka(to_kafka(msg.clone()).as_ref()).unwrap());
    }
}
