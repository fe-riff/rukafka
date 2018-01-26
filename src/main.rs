extern crate kafka;
extern crate byteorder;
extern crate serde_json;

use std::collections::HashMap;
use std::str;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset};

fn main() {
//    let mut producer =
//        Producer::from_hosts(vec!("localhost:9092".to_owned()))
//            .with_ack_timeout(Duration::from_secs(1))
//            .with_required_acks(RequiredAcks::One)
//            .create()
//            .unwrap();
//
//    let mut buf = String::with_capacity(2);
//    for i in 0..10 {
//        let _ = write!(&mut buf, "{}", i); // some computation of the message data to be sent
//        println!("sending a message");
//        producer.send(&Record::from_value("test", buf.as_bytes())).unwrap();
//        buf.clear();
//    }

//    producer.send(&Record::from_value("test", "glyn".as_bytes())).unwrap();


    let mut consumer =
        Consumer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_topic("atopic".to_owned())
            .with_fallback_offset(FetchOffset::Earliest)
            .create()
            .unwrap();
    loop {
        for ms in consumer.poll().unwrap().iter() {
            println!("consumer poll returned");
            for m in ms.messages() {
                let msg = from_kafka(m.value).unwrap();
                println!("{:?}", msg);
            }
            consumer.consume_messageset(ms).unwrap();
        }
        consumer.commit_consumed().unwrap();
    }
}

fn from_kafka(bytes: &[u8]) -> Result<Message, &str> {
    let mut offset: usize = 0;
    if bytes[offset] != 0xff {
        return Err("expected 0xff as the leading byte");
    }
    offset += 1;

    let header_count = bytes[offset];
    offset += 1;

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
        offset += 4;

        let value = serde_json::from_str(str::from_utf8(&bytes[offset..offset + len]).unwrap()).unwrap();

        headers.insert(String::from(name), value);
        offset += len;
    }
    Ok(new_message(&bytes[offset..], headers))
}

#[derive(Debug)]
struct Message<'a> {
    pub payload: &'a [u8],
    pub headers: HashMap<String, [String; 1]>, // TODO: support headers with more than one value in the list
}

fn new_message<'a>(bytes: &'a [u8], headers: HashMap<String, [String; 1]>) -> Message<'a> {
    Message {
        payload: bytes,
        headers: headers,
    }
}
