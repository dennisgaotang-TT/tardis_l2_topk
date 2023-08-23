extern crate csv;
use std::fs;
use std::error::Error;
use csv::Writer;
use std::fs::File;
use flate2::read::GzDecoder;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::collections::BTreeMap;
use colored::*;
use ordered_float::OrderedFloat;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use regex::Regex;
use std::io::Read;
use serde_derive::Deserialize;
use serde_json;
use std::env;

// struct for reading config parameters
#[derive(Deserialize)]
struct Config {
    num_levels: usize,
    choose_to_maintain_smaller_map: bool,
    maintain_rate: usize,
    choose_to_fixed_time_snapshot: bool,
    snapshot_fixed_time_interval: i64,
}

// helper function to find the n-th key in the TreeMap (ascending)
fn find_kth_key_ascending<K: Ord, V>(map: &BTreeMap<K, V>, k: usize) -> Option<&K> {
    let mut count = 0;

    for key in map.keys() {
        if count == k {
            return Some(key);
        }
        count += 1;
    }

    None
}

// helper to find n-th key in the TreeMap (descending)
fn find_kth_key_descending<K: Ord, V>(map: &BTreeMap<K, V>, k: usize) -> Option<&K> {
    let mut count = 0;

    for key in map.keys().rev() {
        if count == k {
            return Some(key);
        }
        count += 1;
    }

    None
}

fn parse_boolean(s: &str) -> Option<bool> {
    match s {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_filename(filename: &str) -> Option<(String, String, String, String)> {
    let path = Path::new(filename);
    let file_name_osstr = path.file_name().unwrap_or_default();
    println!("file_name_osstr={:?}", file_name_osstr);

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
    if datatype != "incremental_book_L2" {
        panic!("The input filename is not of dataset type: 'incremental_book_L2', it is {}", datatype);
    }

    // Process the substring after date
    let (_, split_by_end_end) = &file_name_without_path.split_at(date_match.end());
    let symbol_dot_extension = &split_by_end_end[1..]; // removed the first char

    // Split the symbol_dot_extension part by '.' to get symbol
    let symbol_parts: Vec<&str> = symbol_dot_extension.split('.').collect();
    let symbol = symbol_parts[0].to_string();

    println!("parse result: exchange={}, date={}, sym={} ", exchange, date, symbol);
    Some((path.parent().unwrap().to_string_lossy().to_string(), exchange, date, symbol))
}

fn main() -> Result<(), Box<dyn Error>> {
    //---Part 0: User-defined main() parameters read from config.json file
    // Read the configuration file
    let mut config_file = File::open("config.json").expect("Failed to open config file");
    let mut config_content = String::new();
    config_file.read_to_string(&mut config_content).expect("Failed to read config file");

    // Deserialize the configuration using serde_json
    let config: Config = serde_json::from_str(&config_content).expect("Failed to parse config JSON");

    // Access the parameters from the config
    let num_levels = config.num_levels; // the number of level of snapshot specified by user

    // feature 1: options to shrink the internal size of the map
    let choose_to_maintain_smaller_map:bool = config.choose_to_maintain_smaller_map;
    let maintain_rate = config.maintain_rate; // the maintain rate used to decide the size of the internal map
    let num_levels_maintained = num_levels * maintain_rate;

    // feature 2: choosing whether the snapshot time to be fixed time window or by event;
    //            when choosing "by event", user choose to take a snapshot of the orderbook whenever there are updates
    //            in the snapshot within the specified depth 
    let choose_to_fixed_time_snapshot:bool = config.choose_to_fixed_time_snapshot;
    let snapshot_fixed_time_intervel:i64 = config.snapshot_fixed_time_interval; // the fixed length snapshot time interval(millisecond) to determine snapshot frequency

    //
    // The program name is always the first argument, so we skip it.
    // args[1] = 

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run -- <file_path>");
        return Err("args error".into());
    }
    println!("Start processing {}", args[1]);


    // ---Part 1: parsing in the file
    // Open the input file
    let path_in = Path::new(&args[1]);
    let file_in = File::open(&path_in)?;
    let decoder = GzDecoder::new(file_in);
    let reader = BufReader::new(decoder);
    let mut total_num_rows: u32 = 0;
    let mut total_num_rows_write = 0;

    //Part 1.5: open the output file
    // input file name: binance-futures_incremental_book_L2_2023-03-06_BTCUSDT.csv.gz
    // parse the input file to decide the output file name
    let mut original_parent_folder = String::new();
    let mut exchange_in = String::new();
    let mut date_in = String::new();
    let mut symbol_in = String::new();
    // parse the input file to decide the output file name
    if let Some((pth, exch, dt, sym)) = parse_filename(&args[1]) {
        original_parent_folder = pth;
        exchange_in = exch;
        date_in = dt;
        symbol_in = sym;
    } else {
        println!("Invalid filename format");
        return Ok(());
    }
    
    // output: format!("/Users/tanggao/Desktop/projects/datasets/{}_book_snapshot_200_{}_{}.csv", exchange_in, date_in, symbol_in)
    println!("original_parent_folder = {}", original_parent_folder);
    let out_file_name = format!("/{}_book_snapshot_200_{}_{}.csv", exchange_in, date_in, symbol_in);
    // store the output file into the original folder
    // Create the directory
    let new_dir_name = original_parent_folder.to_string()+ "/orderbook_snapshots/";
    match fs::create_dir(&new_dir_name) {
        Ok(()) => println!("Directory created successfully."),
        Err(err) => println!("Error creating directory: {}", err),
    }
    let file_out = File::create(new_dir_name + &out_file_name)?;
    let mut writer = Writer::from_writer(file_out);
    // Initial two headers
    let initial_headers: Vec<String> = vec!["exchange".to_string(), "symbol".to_string(), "timestamp".to_string(), "local_timestamp".to_string()];
    // Generate the map headers
    let map1_headers: Vec<String> = (0..num_levels)
        .flat_map(|i| vec![format!("asks[{}].price", i), format!("asks[{}].amount", i)])
        .collect();
    // Generate the map2 headers
    let map2_headers: Vec<String> = (0..num_levels)
        .flat_map(|i| vec![format!("bids[{}].price", i), format!("bids[{}].amount", i)])
        .collect();
    // Combine all headers
    let headers: Vec<String> = initial_headers
        .into_iter()
        .chain(map1_headers.into_iter())
        .chain(map2_headers.into_iter())
        .collect();
    //println!("{:?}", headers);
    writer.write_record(&headers)?; // Write headers


    let mut bids: BTreeMap<OrderedFloat<f64>, f64> = BTreeMap::new();  // All bids (price -> amount)
    let mut asks: BTreeMap<OrderedFloat<f64>, f64> = BTreeMap::new();  // All asks (price -> amount)

    // previous line of variables
    let mut prev_symbol:String = String::from(symbol_in);
    let mut prev_local_timestamp:i64 = 0;
    let mut prev_is_snapshot:bool = false;

    // orderbook clock state measures
    let mut initial_snapshot_build_completed_ready_to_snapshot:bool = false;
    let mut next_snapshot_time:i64 = -1;

    // detector measures
    let mut rule1_count:u32 = 0;
    let mut reset_count:u32 = 0;
    let mut abnormal_gap_count:u64 = 0; 
    let mut abnormal_gap_count_equal:i64 = 0;
    let mut abnormal_gap_count_overlap:i64 = 0;
    let mut num_initial_orderbook_finished_setup = 0;
    let mut initial_snapshot_depth_at_finished_setup: Vec<(usize, usize)> = Vec::new();
    let mut num_snapshots = 0;
    let mut unique_local_timestamp_count:u32 = 1;

    // Skip the header line
    let mut lines = reader.lines();
    let _header = lines.next();
    println!("{:?}", _header);

    for line in lines {
        // variable within the for loop
        let mut is_time_to_snapshot = false; // only snapshot when "is_snapshot" == false
                                                    // because true means the initial order book state is still being constructed

        //---Part 1: readin column fields
        // Unwrap the Result
        let line = line?;
        // Split the line into fields
        let fields: Vec<&str> = line.split(',').collect();
        

        // Extract the necessary values from the columns
        let exchange = String::from(fields[0]);
        let symbol = String::from(fields[1]);
        let timestamp: i64 = i64::from_str_radix(fields[2], 10)?;
                               // Value from the timestamp column
        // // convert timestamp
        // // Convert microseconds to seconds
        // let timestamp_utc = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(timestamp).unwrap(), Utc);
        // // Format the timestamp with millisecond precision
        // let timestamp_formatted = timestamp_utc.format("%Y-%m-%d %H:%M:%S%.6f");

        let local_timestamp: i64 = i64::from_str_radix(fields[3], 10)?;
                      // Value from the local timestamp column
        // // convert timestamp
        // // Convert microseconds to seconds
        // let local_timestamp_utc = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(local_timestamp).unwrap(), Utc);
        // // Format the timestamp with millisecond precision
        // let local_timestamp_formatted = local_timestamp_utc.format("%Y-%m-%d %H:%M:%S%.6f");

        let is_snapshot: bool = parse_boolean(fields[4]).ok_or_else(|| {
            eprintln!("Error parsing is_snapshot");
            io::Error::new(io::ErrorKind::InvalidData, "Invalid is_snapshot")
        })?;

        let side = String::from(fields[5]);                             
        let price: f64 = fields[6].parse()?;
        let amount: f64 = fields[7].parse()?;
        // increments the # readin rows
        total_num_rows += 1;


        //---Part 2: implement the rules
        // Rule 0:
        if prev_symbol != symbol {
            panic!("Generation terminated due to symbol unmatch: prev_symbol = {}, cur_symbol = {}", prev_symbol, symbol);
        }

        // Rule 1: Check if local timestamp indicates a consistent state
        if local_timestamp < prev_local_timestamp {
            // skip this line of local order book state as it is not consistent
            // store the inconsistent rows
            rule1_count += 1;
            continue;
        }

        // Rule 2: is_snapshot from false to true: rebuilt initial snapshot
        if is_snapshot && !prev_is_snapshot {
            reset_count += 1;
            // reset your local order book state
            bids.clear();
            asks.clear();
            initial_snapshot_build_completed_ready_to_snapshot = false;
        }
        

        // Rule 3: Remove price level with amount set to zero
        if amount == 0.0 {
            if side == "bid" {
                bids.remove(&OrderedFloat::from(price));
            } else if side == "ask" {
                asks.remove(&OrderedFloat::from(price));
            }
        }

        // Rule 4: Update or add price level with non-zero amount
        if amount != 0.0 {
            // let order = Order { price, amount };
            if side == "bid" {
                if bids.insert(OrderedFloat::from(price), amount).is_none() {
                    //real insertion
                    // maintain the length after initial build up cut. i.e is_snapshot=false
                    if choose_to_maintain_smaller_map && (is_snapshot == false && bids.len() > num_levels_maintained) {
                        bids.pop_first();
                    }
                }
            } else if side == "ask" {
                if asks.insert(OrderedFloat::from(price), amount).is_none() {
                    // maintain the length after initial build up cut. i.e is_snapshot=false
                    if choose_to_maintain_smaller_map && (is_snapshot == false && asks.len() > num_levels_maintained) {
                        asks.pop_last();
                    }
                }
            }
        }

        //--- Part 4: Display for each iter
        //println!("Processing...");
        // // Display the total depth
        // if (true) {
        //     println!("bids_len = {}", bids.len());
        //     println!("asks_len = {}", asks.len());
        // }

        // When the initial snapshot finished built, setting up the snapshot timer 
        if prev_is_snapshot && !is_snapshot {
            initial_snapshot_build_completed_ready_to_snapshot = true;
            num_initial_orderbook_finished_setup += 1; 
            next_snapshot_time = local_timestamp;
        }

        //---Part: option to maintain a smaller internal map
        // when is_snapshot switch from true to false, the initial snapshot 
        // has been built up, we need to maintain the snapshot bound asks_200, bids_200
        if choose_to_maintain_smaller_map {
            if prev_is_snapshot && !is_snapshot {
                // record
                initial_snapshot_depth_at_finished_setup.push((asks.len(), bids.len()));
                println!("is_snapshot switch from true to false record at timestamp = {}: ", DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"));
                println!("---before cut: asks.len = {}; bids.len = {}", asks.len(), bids.len());
        
                if asks.len() > num_levels_maintained {
                    //cut asks
                    let asks_split_off_key = find_kth_key_ascending(&asks, num_levels_maintained).unwrap();
                    asks.split_off(&asks_split_off_key.clone());
                }
                if bids.len() > num_levels_maintained {
                    // cut bids
                    let bids_split_off_key = find_kth_key_descending(&bids, num_levels_maintained - 1).unwrap();
                    let mut bids_top_n: BTreeMap<OrderedFloat<f64>, f64> = bids.split_off(&bids_split_off_key.clone());
                    bids = bids_top_n;
                }
                println!("--- after cut: asks.len = {}; bids.len = {}", asks.len(), bids.len());
            
                // Exception handle: check if the len of map exceed the user required depth
                if asks.len() > num_levels_maintained || bids.len() > num_levels_maintained {
                    panic!("Wrong Case: The user required {} depth of snapshot, the program maintains {}-depth of maps: 
                    The actual asks.len() = {}, bids.len() = {}", 
                    num_levels, num_levels_maintained, asks.len(), bids.len());
                }
            }
        }
        
        //Part--- detect snapshot time: Using price(key) to check if the update is within centered 200 level
        // The following block is to set the trigger variable "is_time_to_snapshot"
        if initial_snapshot_build_completed_ready_to_snapshot && local_timestamp > prev_local_timestamp && asks.len() >= num_levels && bids.len() >= num_levels {
            // only take snapshots after initial snapshot built up
            // to see if the update is within top 200 layers
            if choose_to_fixed_time_snapshot {
                if local_timestamp >= next_snapshot_time {
                    // set the snapshot trigger for this iteration
                    is_time_to_snapshot = true;
                    // println!("next_snapshot_time = {}", next_snapshot_time);
                    next_snapshot_time += snapshot_fixed_time_intervel* 1000;
                    num_snapshots += 1;
                }
            } else {
                // the user choose the event trigger snapshot
                let asks_kth_lowest = find_kth_key_ascending(&asks, num_levels - 1);
                let bids_kth_highest = find_kth_key_descending(&bids, num_levels - 1);
                match (asks_kth_lowest, bids_kth_highest) {
                    (Some(up_bound), Some(low_bound)) => {
                        if &price >= &low_bound.into_inner() && &price <= &up_bound.into_inner() {
                            // set the snapshot trigger for this iteration
                            is_time_to_snapshot = true;
                            num_snapshots += 1;
                        } 
                    }
                    (Some(_), None) => panic!("Case: row{}: asks map's depth < {}", total_num_rows, num_levels),
                    (None, Some(_)) => panic!("Case: row{}: bids map's depth < {}", total_num_rows, num_levels),
                    (None, None) => panic!("Case: row{}: Both asks and bids map < {}", total_num_rows, num_levels),
                }
            }
        } 

        //---Part 5: Write the snapshot to a .csv file book_snapshot_200
        if is_time_to_snapshot {
            // write to the .csv file
            let mut row = Vec::new();
            // initial values: exchange symbol timestamp, local_timestamp
            row.push(exchange);
            row.push(symbol.clone());
            row.push(timestamp.to_string());
            row.push(local_timestamp.to_string());
            for (key, value) in asks.iter().take(num_levels) {
                row.push(key.0.to_string());
                row.push(value.to_string());
            }
            for (key, value) in bids.iter().rev().take(num_levels) {
                row.push(key.0.to_string());
                row.push(value.to_string());
            }
    
            writer.write_record(&row)?;
            total_num_rows_write += 1;
        }

        // //---Part 6: Display a depth-10 snapshot when abnormaly gap happens
        // let highest_bid = bids.last_key_value();
        // let lowest_ask = asks.first_key_value();
        
        // // detect abnormal gaps
        //  if (lowest_ask != None && highest_bid != None && local_timestamp > prev_local_timestamp) {
        //     let lowest_ask_f64 = lowest_ask.unwrap().0.0;
        //     let highest_bid_f64 = highest_bid.unwrap().0.0;
        //     if (lowest_ask_f64 <= highest_bid_f64) {
        //         if lowest_ask_f64 < highest_bid_f64 {
        //             abnormal_gap_count_overlap += 1;
        //         }
        //         if lowest_ask_f64 == highest_bid_f64 {
        //             abnormal_gap_count_equal += 1;
        //         }
        //         let asks_first_10: Vec<_> = asks.iter().take(10).collect();
        //         let asks_first_10_rev = asks_first_10.into_iter().rev().collect::<Vec<_>>();
        //         for (key, value) in asks_first_10_rev {
        //             println!("{},    {}", key.0.to_string().red(), value.to_string().red());
        //         }

        //         for (key, value) in bids.iter().rev().take(10) {
        //             println!("{},    {}", key.0.to_string().green(), value.to_string().green());
        //         }
        //         println!("the abnormal line ={}", total_num_rows);
        //         abnormal_gap_count += 1;
        //         println!("abnormal_gap_ask_bid_prices =  {},    {}", lowest_ask_f64, highest_bid_f64);
        //         println!("total_num_rows =  {}", total_num_rows);
        //         println!("num_snapshots =  {}", num_snapshots);
        //         println!("timestamp       = {}", DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"));
        //         println!("local_timestamp = {}", DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(local_timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"));
        //     }
        // }
        if local_timestamp != prev_local_timestamp {
            unique_local_timestamp_count += 1;
        }

        //---Part 6: update the flags
        prev_symbol = String::from(symbol);
        prev_local_timestamp = local_timestamp;
        prev_is_snapshot = is_snapshot;

        // println!("timestamp = {}", DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp_micros(timestamp).unwrap(), Utc).format("%Y-%m-%d %H:%M:%S%.6f"));
        // end of for loop
    }

    writer.flush()?; // Ensure all data is written to the file

    println!("Finished: \n");
    println!("File: {}", args[1]);
    println!("total rows processed: {}", total_num_rows);
    println!("unique_local_timestamp_count = {}", unique_local_timestamp_count);
    println!("total rows written: {}", total_num_rows_write);
    println!("rule1_count = {}", rule1_count);
    println!("reset_count = {}", reset_count);
    println!("num_initial_orderbook_finished_setup = {}", num_initial_orderbook_finished_setup);
    println!("abnormal_gap_count = {}", abnormal_gap_count);
    println!("abnormal_gap_count_equal = {}", abnormal_gap_count_equal);
    println!("abnormal_gap_count_overlap = {}", abnormal_gap_count_overlap);
    println!("initial_snapshot_depth_at_finished_setup = {:?}", initial_snapshot_depth_at_finished_setup);
    Ok(())
}
