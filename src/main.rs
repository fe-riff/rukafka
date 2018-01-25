extern crate kafka;

use std::fmt::Write;
use std::time::Duration;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

fn main() {
    let mut producer =
        Producer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

    let mut buf = String::with_capacity(2);
    for i in 0..10 {
        let _ = write!(&mut buf, "{}", i); // some computation of the message data to be sent
        println!("sending a message");
        producer.send(&Record::from_value("test", buf.as_bytes())).unwrap();
        buf.clear();
    }

    producer.send(&Record::from_value("test", "glyn".as_bytes())).unwrap();


    let mut consumer =
        Consumer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_topic("test".to_owned())
            .with_fallback_offset(FetchOffset::Earliest)
            .create()
            .unwrap();
    loop {
        for ms in consumer.poll().unwrap().iter() {
            println!("consumer poll returned");
            for m in ms.messages() {
                println!("{:?}", String::from_utf8_lossy(m.value));
            }
            consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
    }
}
