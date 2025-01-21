use clap::Parser;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;

pub fn parse_log_entry_subscriber(log_entry: &str) -> (u64, u64) {
    let re = Regex::new(r".*got account update: write_version=(?P<write_version>\d+);timestamp_us=(?P<timestamp_us>\d+);slot=(?P<slot>\d+)").unwrap();
    let caps = re.captures(log_entry).unwrap();

    // let mut result = HashMap::new();
    // result.insert("write_version".to_string(), caps["write_version"].to_string());
    // result.insert("timestamp_us".to_string(), caps["timestamp_us"].to_string());
    // result.insert("slot".to_string(), caps["slot"].to_string());

    let write_version: u64 = caps["write_version"].parse().unwrap();
    let timestamp_us: u64 = caps["timestamp_us"].parse().unwrap();

    (write_version, timestamp_us)
}

pub fn parse_log_entry_source(log_entry: &str) -> (u64, u64) {
    let re = Regex::new(r".*account update: write_version=(?P<write_version>\d+);timestamp_us=(?P<timestamp_us>\d+);slot=(?P<slot>\d+)").unwrap();
    let caps = re.captures(log_entry).unwrap();

    // let mut result = HashMap::new();
    // result.insert("write_version".to_string(), caps["write_version"].to_string());
    // result.insert("timestamp_us".to_string(), caps["timestamp_us"].to_string());
    // result.insert("slot".to_string(), caps["slot"].to_string());

    let write_version: u64 = caps["write_version"].parse().unwrap();
    let timestamp_us: u64 = caps["timestamp_us"].parse().unwrap();

    (write_version, timestamp_us)
}

fn read_subscriber_log(log_file: PathBuf) -> HashMap<u64, u64> {
    let mut map: HashMap<u64, u64> = HashMap::new();

    let file = File::open(log_file).expect("file must exist");
    let reader = io::BufReader::new(file);
    for line in reader.lines().take(1000) {
        let line = line.expect("must be parsable");
        let (write_version, timestamp_us) = parse_log_entry_subscriber(&line);
        // println!("{:?}", parsed);
        map.insert(write_version, timestamp_us);
    }

    map
}

fn read_source_log(log_file: PathBuf) -> HashMap<u64, u64> {
    let mut map: HashMap<u64, u64> = HashMap::new();

    let file = File::open(log_file).expect("file must exist");
    let reader = io::BufReader::new(file);
    for line in reader.lines().take(1000) {
        let line = line.expect("must be parsable");
        let (write_version, timestamp_us) = parse_log_entry_source(&line);
        // println!("{:?}", parsed);
        map.insert(write_version, timestamp_us);
    }

    map
}

// cat macbook.log |cut -b 111- | tr -d 'a-z_=' > macbook.log.csv
// cat solana-validator-macbook.log | cut -b 96- | tr -d 'a-z_='
fn read_from_csv(csv_file: PathBuf) -> HashMap<u64, u64> {
    csv::ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(false)
        .from_path(csv_file)
        .unwrap()
        .into_deserialize()
        .map(|record| {
            let record: Vec<String> = record.unwrap();
            let write_version = record[0].parse::<u64>().unwrap();
            let timestamp_us = record[1].parse::<u64>().unwrap();
            (write_version, timestamp_us)
        })
        .collect::<HashMap<u64, u64>>()
}

#[allow(dead_code)]
fn read_subscriber_log_csv(csv_file: PathBuf) -> HashMap<u64, u64> {
    csv::ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(false)
        .from_path(csv_file)
        .unwrap()
        .into_deserialize()
        .map(|record| {
            let record: Vec<String> = record.unwrap();
            let write_version = record[0].parse::<u64>().unwrap();
            let timestamp_us = record[1].parse::<u64>().unwrap();
            (write_version, timestamp_us)
        })
        .collect::<HashMap<u64, u64>>()
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub csv_file_source: String,
    #[arg(long)]
    pub csv_file_subscriber: String,
}

pub fn main() {
    let Args {
        csv_file_source,
        csv_file_subscriber,
    } = Args::parse();

    println!("Reading source log ...");
    let source_timestamps = read_from_csv(PathBuf::from(csv_file_source));

    println!("Reading subscriber log ...");
    let subscriber_timestamps = read_from_csv(PathBuf::from(csv_file_subscriber));

    for (write_version, timestamp_us) in subscriber_timestamps.into_iter() {
        if let Some(source_timestamp) = source_timestamps.get(&write_version) {
            let diff = (timestamp_us as i128) - (*source_timestamp as i128);
            println!(
                "write_version: {}, subscriber: {}, source: {}, diff: {:.1}ms",
                write_version,
                timestamp_us,
                source_timestamp,
                diff as f64 / 1000.0
            );
        }
    }
}

pub fn main__() {
    println!("Reading subscriber log ...");
    let subscriber_timestamps = read_subscriber_log(PathBuf::from(
        "/Users/stefan/mango/projects/geyser-misc/accounts-stream-performance/macbook.log",
    ));

    println!("Reading source log ...");
    let source_timestamps = read_source_log(PathBuf::from("/Users/stefan/mango/projects/geyser-misc/accounts-stream-performance/solana-validator-macbook.log"));

    println!("Comparing ...");

    for (write_version, timestamp_us) in subscriber_timestamps.into_iter() {
        // println!("write_version: {}, subscriber: {}", write_version, timestamp_us);
        if let Some(source_timestamp) = source_timestamps.get(&write_version) {
            let diff = (timestamp_us as i128) - (*source_timestamp as i128);
            println!(
                "write_version: {}, subscriber: {}, source: {}, diff: {}",
                write_version, timestamp_us, source_timestamp, diff
            );
        }
    }
}
