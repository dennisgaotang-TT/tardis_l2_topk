use std::fs::File;
use std::io::{BufReader, BufWriter};
use bincode::{deserialize_from, serialize_into};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use regex::Regex;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use std::cmp;
use std::env;

fn count_common_key_value_pairs<K, V>(map1: &BTreeMap<K, V>, map2: &BTreeMap<K, V>) -> usize
where
    K: Ord + Eq,
    V: PartialEq,
{
    map1.iter()
        .filter(|(key, value)| map2.get(key) == Some(value))
        .count()
}

fn parse_filename(filename: &str) -> Option<(String, String, String, String)> {
    let path = Path::new(filename);
    let file_name_osstr = path.file_name().unwrap_or_default();

    // Create a regex pattern to match the date in "YYYY-MM-DD" format
    let date_regex = Regex::new(r"\d{4}-\d{2}-\d{2}").unwrap();
    // Find the date in the filename using the regex
    let file_name_without_path = file_name_osstr.to_str().unwrap();
    let date_match = date_regex.find(&file_name_without_path)?;
    // Get the date part from the match
    let date = date_match.as_str().to_string();

    // Process the substring before date
    let (start, _) = &file_name_without_path.split_at(date_match.start());
    let exchange_underscore_datatype = &start[..start.len()-1]; // remove the last char
    let i_first_underscore = exchange_underscore_datatype.find('_').unwrap();
    let exchange = exchange_underscore_datatype[..i_first_underscore].to_string();
    let datatype = exchange_underscore_datatype[i_first_underscore+1..].to_string();

    // Process the substring after date
    let (_, split_by_end_end) = &file_name_without_path.split_at(date_match.end());
    let symbol_dot_extension = &split_by_end_end[1..]; // removed the first char

    // Split the symbol_dot_extension part by '.' to get symbol
    let symbol_parts: Vec<&str> = symbol_dot_extension.split('.').collect();
    let symbol = symbol_parts[0].to_string();

    // println!("parse result: exchange={}, date={}, sym={} ", exchange, date, symbol);
    Some((path.parent().unwrap().to_string_lossy().to_string(), exchange, date, symbol))
}


#[derive(Debug, Serialize, Deserialize)]
struct BidsAsksData {
    initial_snapshot_finished_timestamp: i64,
    initial_snapshot_finished_local_timestamp: i64,
    start_day_bids: BTreeMap<OrderedFloat<f64>, f64>,
    start_day_asks: BTreeMap<OrderedFloat<f64>, f64>,

    end_day_timestamp: i64,
    end_day_local_timestamp: i64,
    end_day_bids: BTreeMap<OrderedFloat<f64>, f64>,
    end_day_asks: BTreeMap<OrderedFloat<f64>, f64>,
}

struct MapState {
    timestamp: i64, 
    local_timestamp: i64,
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}


fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run <bin_file_folder_path>");
        return;
    }

    // let folder_path = "/Users/tanggao/Desktop/projects/datasets_okex_swap/bin";
    let folder_path = &args[1];
    
    // Get a list of all .bin files in the specified folder
    let mut bin_files = match std::fs::read_dir(folder_path) {
        Ok(files) => files
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_file() && path.extension().unwrap_or_default() == "bin" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        Err(e) => {
            eprintln!("Error reading directory: {}", e);
            return;
        }
    };

    // Sort the files by their names in ascending order
    bin_files.sort_by(|a, b| {
        let a_name = a.file_name().unwrap().to_str().unwrap();
        let b_name = b.file_name().unwrap().to_str().unwrap();
        a_name.cmp(b_name)
    });

    let mut prev_date:String = String::from("");
    let mut prev_end_day_timestamp: i64 = 0;
    let mut prev_end_day_local_timestamp: i64 = 0;
    let mut prev_end_day_bids: BTreeMap<OrderedFloat<f64>, f64> = BTreeMap::new();
    let mut prev_end_day_asks: BTreeMap<OrderedFloat<f64>, f64> = BTreeMap::new();

    let mut i:u32 = 1;
    println!("Report: comparing the gap between previous day end map state and current day initial snapshot state");
    let first_file_name = bin_files.first().and_then(|path| path.file_name()).unwrap_or_default();
    let last_file_name = bin_files.last().and_then(|path| path.file_name()).unwrap_or_default();
    println!("Datasets: consecutive datasets from {:?} to {:?}\n", first_file_name, last_file_name);
    println!("*************************************************************************************************");
    for file in bin_files {
        // Part 0: parse the input file to get the date
        let mut original_parent_folder = String::new();
        let mut exchange_in = String::new();
        let mut date_in = String::new();
        let mut symbol_in = String::new();
        // parse the input file to decide the output file name
        if let Some((pth, exch, dt, sym)) = parse_filename(file.to_str().unwrap()) {
            original_parent_folder = pth;
            exchange_in = exch;
            date_in = dt;
            symbol_in = sym;
        } else {
            println!("Invalid filename format");
        }
        

        // Part 1: push the 
        let bin_file_name = file.to_str().unwrap();
        let file = File::open(bin_file_name).expect("Failed to open the .bin file");
        let reader = BufReader::new(file);
        let bids_asks_state_data: BidsAsksData = deserialize_from(reader).expect("Failed to deserialize data");
        // Now you can use the deserialized data

        if i == 1 {
            prev_end_day_timestamp = bids_asks_state_data.end_day_timestamp;
            prev_end_day_local_timestamp = bids_asks_state_data.end_day_local_timestamp;
            prev_end_day_bids = bids_asks_state_data.end_day_bids;
            prev_end_day_asks = bids_asks_state_data.end_day_asks;
            prev_date = date_in;
            i += 1;
            continue;
        }
        let current_start_day_timestamp = bids_asks_state_data.initial_snapshot_finished_timestamp;
        let current_start_day_local_timestamp = bids_asks_state_data.initial_snapshot_finished_local_timestamp;
        let mut current_start_day_bids = bids_asks_state_data.start_day_bids;
        let mut current_start_day_asks = bids_asks_state_data.start_day_asks;

        // compare prev day's end state and current day's start state
        
        println!("Result comparing the gap between day {}'s end state and day {}'s start state: \n", prev_date, date_in);
        println!("Len: \t prev_asks_len = {} ; prev_bids_len = {}", prev_end_day_asks.len(), prev_end_day_bids.len());
        println!("     \t curr_asks_len = {} ; curr_bids_len = {}\n", current_start_day_asks.len(), current_start_day_bids.len());
        
        let asks_common_count = count_common_key_value_pairs(&current_start_day_asks, &prev_end_day_asks);
        let bids_common_count = count_common_key_value_pairs(&current_start_day_bids, &prev_end_day_bids);
        let min_len_asks = cmp::min(current_start_day_asks.len(), prev_end_day_asks.len());
        let min_len_bids = cmp::min(current_start_day_bids.len(), prev_end_day_bids.len());

        println!("#Common key-value pairs: ");
        println!("\t asks_common_count={}; asks_min_len={} ", asks_common_count, min_len_asks);
        println!("\t bids_common_count={}; bids_min_len={} \n", bids_common_count, min_len_bids);

        println!("Descrepency: min(map1.len, map2.len) - #common-pair");
        println!("\t asks: {}, {}% ", min_len_asks - asks_common_count, (min_len_asks - asks_common_count) as f64/ min_len_asks as f64 / 100.0);
        println!("\t bids: {}, {}% \n", min_len_bids - bids_common_count, (min_len_bids - bids_common_count) as f64 / min_len_asks as f64 / 100.0);

        println!("Lowest_ask & highest bid: ");
        let first_entry = prev_end_day_asks.first_entry().unwrap();
        let (first_key, first_value) = (first_entry.key().0, first_entry.get());
        let last_entry = prev_end_day_bids.last_entry().unwrap();
        let (last_key, last_value) = (last_entry.key().0, last_entry.get());
        println!("\t prev_low_ask_price = {:?} , prev_low_ask_quantity = {:?} ; prev_high_bid_price = {:?} , prev_high_bid_quantity = {:?}", first_key , first_value, last_key, last_value);
        let cur_first_entry = current_start_day_asks.first_entry().unwrap();
        let (cur_first_key, cur_first_value) = (cur_first_entry.key().0, cur_first_entry.get());
        let cur_last_entry = current_start_day_bids.last_entry().unwrap();
        let (cur_last_key, cur_last_value) = (cur_last_entry.key().0, cur_last_entry.get());
        println!("\t prev_low_ask_price = {:?} , prev_low_ask_quantity = {:?} ; prev_high_bid_price = {:?} , prev_high_bid_quantity = {:?}\n", cur_first_key , cur_first_value, cur_last_key, cur_last_value);

        // timestamp!
        println!("Timestamps: ");
        println!("\t prev_timestamp = {}; prev_local_timestamp = {}", DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(prev_end_day_timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"), 
                DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(prev_end_day_local_timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"));
        println!("\t curr_timestamp = {}; prev_local_timestamp = {}", DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(current_start_day_timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"), 
                DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(current_start_day_local_timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"));
        let timestamp_descrep = (current_start_day_timestamp - prev_end_day_timestamp) as f64 / 1000.0;
        let local_timestamp_descrep = (current_start_day_local_timestamp - prev_end_day_local_timestamp) as f64 / 1000.0;
        println!("Summary: ");
        println!("\t asks map: {} different pairs occured out of minLen={} in {}(timestamp)/{}(local_timestamp) miliseconds.", min_len_asks - asks_common_count, min_len_asks, timestamp_descrep, local_timestamp_descrep);
        println!();

        println!("*********************************************************************************************************************************************");
        // update end state
        prev_end_day_timestamp = bids_asks_state_data.end_day_timestamp;
        prev_end_day_local_timestamp = bids_asks_state_data.end_day_local_timestamp;
        prev_end_day_bids = bids_asks_state_data.end_day_bids;
        prev_end_day_asks = bids_asks_state_data.end_day_asks;

        prev_date = date_in;
        i += 1;
    }

    // let bin_file_name = "/Users/tanggao/Desktop/projects/verify/binance-futures_start_end_map_2023-03-06_BTCUSDT.bin";

    
    // // Read the BidsAsksData struct from the .bin file
    // let file = File::open(bin_file_name).expect("Failed to open the .bin file");
    // let reader = BufReader::new(file);
    // let bids_asks_state_data: BidsAsksData = deserialize_from(reader).expect("Failed to deserialize data");

    // // let start_day_timestamp = bids_asks_state_data.initial_snapshot_finished_timestamp;
    // // let start_day_local_timestamp = bids_asks_state_data.initial_snapshot_finished_local_timestamp;

    // // Now you can use the deserialized data
    // println!("{:?}", bids_asks_state_data);
    
}


// fn main() {
//     // Read the BidsAsksData struct from the file using Bincode deserialization
//     let file = File::open("bids_asks.bin").unwrap();
//     let reader = BufReader::new(file);
//     let bids_asks_data: BidsAsksData = deserialize_from(reader).unwrap();

//     // Access the bids and asks BTreeMaps from the BidsAsksData struct
//     let bids = bids_asks_data.bids;
//     let asks = bids_asks_data.asks;

//     // Print the BTreeMaps (just for demonstration)
//     println!("Bids: {:?}", bids);
//     println!("Asks: {:?}", asks);
// }
