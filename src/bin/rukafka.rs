extern crate kafka;
extern crate byteorder;
extern crate rukafka;

use rukafka::wireformat;
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
        for _ in 1..101 {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    let msg = wireformat::from_kafka(m.value).unwrap();
                    producer.send(&Record::from_value("replies",
                                                      wireformat::to_kafka(msg).as_bytes())).unwrap();
                }
                consumer.consume_messageset(ms).unwrap();
            }
        }
        consumer.commit_consumed().unwrap();
    }
}
