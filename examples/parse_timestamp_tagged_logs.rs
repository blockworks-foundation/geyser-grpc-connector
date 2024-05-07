use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use regex::Regex;

pub fn parse_log_entry_subscriber(log_entry: &str) -> HashMap<String, String> {
    let re = Regex::new(r".*got account update: write_version=(?P<write_version>\d+);timestamp_us=(?P<timestamp_us>\d+);slot=(?P<slot>\d+)").unwrap();
    let caps = re.captures(log_entry).unwrap();

    let mut result = HashMap::new();
    result.insert("write_version".to_string(), caps["write_version"].to_string());
    result.insert("timestamp_us".to_string(), caps["timestamp_us"].to_string());
    result.insert("slot".to_string(), caps["slot"].to_string());

    result
}

pub fn parse_log_entry_source(log_entry: &str) -> HashMap<String, String> {
    let re = Regex::new(r".*account update: write_version=(?P<write_version>\d+);timestamp_us=(?P<timestamp_us>\d+);slot=(?P<slot>\d+)").unwrap();
    let caps = re.captures(log_entry).unwrap();

    let mut result = HashMap::new();
    result.insert("write_version".to_string(), caps["write_version"].to_string());
    result.insert("timestamp_us".to_string(), caps["timestamp_us"].to_string());
    result.insert("slot".to_string(), caps["slot"].to_string());

    result
}



fn read_subscriber_log(log_file: PathBuf) {

    let file = File::open(log_file).expect("file must exist");
    let reader = io::BufReader::new(file);
    for line in reader.lines().take(10) {
        let line = line.expect("must be parsable");
        let parsed = parse_log_entry_subscriber(&line);
        println!("{:?}", parsed);
    }

}

fn read_source_log(log_file: PathBuf) {
    let file = File::open(log_file).expect("file must exist");
    let reader = io::BufReader::new(file);
    for line in reader.lines().take(10) {
        let line = line.expect("must be parsable");
        let parsed = parse_log_entry_source(&line);
        println!("{:?}", parsed);
    }
}


pub fn main() {


    read_subscriber_log(PathBuf::from("/Users/stefan/mango/projects/geyser-misc/accounts-stream-performance/macbook.log"));

    read_source_log(PathBuf::from("/Users/stefan/mango/projects/geyser-misc/accounts-stream-performance/solana-validator-macbook.log"));



}
