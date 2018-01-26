extern crate kafka;
extern crate byteorder;
#[macro_use]
extern crate serde_json;

mod wireformat;

use std::collections::HashMap;
use std::str;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use kafka::producer::{Producer, Record, RequiredAcks, AsBytes};
use kafka::consumer::{Consumer, FetchOffset};
use std::time::Duration;

fn main() {
    let mut producer =
        Producer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

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
                let msg = wireformat::from_kafka(m.value).unwrap();
                println!("{:?}", msg);
                producer.send(&Record::from_value("replies",
                                                  wireformat::to_kafka(msg).as_bytes())).unwrap();
                println!("reply sent");
            }
            consumer.consume_messageset(ms).unwrap();
        }
        consumer.commit_consumed().unwrap();
    }
}
