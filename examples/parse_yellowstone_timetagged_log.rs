use clap::Parser;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub log_file: String,
}

pub fn main() {
    let Args { log_file } = Args::parse();
    println!("Reading log file: {}", log_file);

    let log_file = PathBuf::from(log_file);

    const LIMIT_LINES: usize = 10000000;

    let mut timetag_sending_to_buffer: HashMap<u64, u64> = HashMap::new();
    let mut timetag_before_sending_grpc: HashMap<u64, u64> = HashMap::new();
    // contains only matches from previous sets
    let mut timetag_geyser: HashMap<u64, u64> = HashMap::new();

    let mut count_sending_to_buffer_channel = 0;
    {
        let file = File::open(&log_file).expect("file must exist");
        let reader = io::BufReader::new(file);
        for line in reader.lines().take(LIMIT_LINES) {
            let line = line.expect("must be parsable");
            // println!("-> buffer channel");
            if let Some((write_version, timestamp_us)) =
                parse_log_entry_sending_to_buffer_channel(line)
            {
                count_sending_to_buffer_channel += 1;
                timetag_sending_to_buffer.insert(write_version, timestamp_us);
            }
        }
    }

    let mut count_sending_grpc = 0;
    {
        let file = File::open(&log_file).expect("file must exist");
        let reader = io::BufReader::new(file);
        for line in reader.lines().take(LIMIT_LINES) {
            let line = line.expect("must be parsable");
            // println!("-> when sending to grpc");
            if let Some((write_version, timestamp_us)) = parse_log_entry_before_sending_grpc(line) {
                count_sending_grpc += 1;
                timetag_before_sending_grpc.insert(write_version, timestamp_us);
            }
        }
    }

    // THIS is by far the largest set
    let mut count_at_geyser = 0;
    {
        let file = File::open(&log_file).expect("file must exist");
        let reader = io::BufReader::new(file);
        for line in reader.lines().take(LIMIT_LINES) {
            let line = line.expect("must be parsable");
            // println!("-> at geyser interface");
            if let Some((write_version, timestamp_us)) = parse_log_entry_at_geyser_interface(line) {
                count_at_geyser += 1;
                if timetag_sending_to_buffer.contains_key(&write_version)
                    && timetag_before_sending_grpc.contains_key(&write_version)
                {
                    timetag_geyser.insert(write_version, timestamp_us);
                }
            }
        }
    }

    println!("Count at geyser interface: {}", count_at_geyser);
    println!(
        "Count sending to buffer channel: {}",
        count_sending_to_buffer_channel
    );
    println!("Count sending to grpc: {}", count_sending_grpc);

    for (write_version, geyser_timestamp_us) in timetag_geyser {
        let timestamp_sending_to_buffer = timetag_sending_to_buffer.get(&write_version).unwrap();
        let timestamp_before_sending_grpc =
            timetag_before_sending_grpc.get(&write_version).unwrap();
        let delta1 = timestamp_sending_to_buffer - geyser_timestamp_us;
        let delta2 = timestamp_before_sending_grpc - timestamp_sending_to_buffer;
        println!(
            "Write Version: {}, geyser - {}us - buffer - {}us - grpc",
            write_version, delta1, delta2
        );
    }
}

fn parse_log_entry_at_geyser_interface(log_line: String) -> Option<(u64, u64)> {
    if !log_line.contains("account update inspect from geyser") {
        return None;
    }

    // Split the log line by ': ' to separate the prefix from the data
    let parts: Vec<&str> = log_line.split(": ").collect();

    // The second part contains the data we need
    let data = parts[1];

    // Split the data by ';' to separate the different fields
    let fields: Vec<&str> = data.split(';').collect();

    // For each field, split by '=' to separate the key from the value
    let write_version: u64 = fields[0].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();
    let timestamp_us: u64 = fields[1].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();
    let _slot: u64 = fields[2].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();

    Some((write_version, timestamp_us))
}

fn parse_log_entry_sending_to_buffer_channel(log_line: String) -> Option<(u64, u64)> {
    if !log_line.contains("sending to buffer channel") {
        return None;
    }

    // Split the log line by ': ' to separate the prefix from the data
    let parts: Vec<&str> = log_line.split(": ").collect();

    // The second part contains the data we need
    let data = parts[1];

    // Split the data by ';' to separate the different fields
    let fields: Vec<&str> = data.split(';').collect();

    // For each field, split by '=' to separate the key from the value
    let write_version: u64 = fields[0].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();
    let timestamp_us: u64 = fields[1].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();
    let _slot: u64 = fields[2].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();

    Some((write_version, timestamp_us))
}

fn parse_log_entry_before_sending_grpc(log_line: String) -> Option<(u64, u64)> {
    if !log_line.contains("before sending to grpc") {
        return None;
    }

    // Split the log line by ': ' to separate the prefix from the data
    let parts: Vec<&str> = log_line.split(": ").collect();

    // The third part contains the data we need
    let data = parts[1];

    // Split the data by ';' to separate the different fields
    let fields: Vec<&str> = data.split(';').collect();

    // For each field, split by '=' to separate the key from the value
    let write_version: u64 = fields[0].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();
    let timestamp_us: u64 = fields[1].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();
    let _slot: u64 = fields[2].split('=').collect::<Vec<&str>>()[1]
        .parse()
        .unwrap();

    Some((write_version, timestamp_us))
}
