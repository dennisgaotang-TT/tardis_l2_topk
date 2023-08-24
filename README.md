# tardis_l2_topk
This repo contains Rust code for two tasks: 
1. re-building orderbook snapshot with specific depth from incremental_orderbook_l2.csv.gz.
2. verify the built orderbook snapshots' correctness and the data source's quality by comparing the previous day's last orderbook state(represented by program-maintained map) with the current day's first exchange-provided initial orderbook snapshot
   
## Task 1: rebuild orderbook snapshot
The programs for rebuilding orderbook snapshot from flat incremental_orderbook_l2.csv.gz file are in the two Rust projects below: processing_crate and multi, where "processing_crate" rebuilds an orderbook snapshot file for one input incremental orderbook .csv.gz data whereas "multi" utilize multi-processing to process a folder containing bunch of such files.
### 1.1 processing_crate
* input file: one incremental_orderbook_l2.csv.gz
* output file: one l2_orderbook_snapshot.csv
* user-specified parameters: config.json(this file should be put into path "../processing_crate")
  1. **config.num_levels: usize**: the user-required depth of the output orderbook snapshot, e.g 200
  2. **config.choose_to_maintain_smaller_map: bool**: a boolean value to specify whether the user choose to maintain a smaller map to record the orderbook snapshot states, e.g num_levels_maintained = asks.len = 1000 and bids.len = 1000 
  3. **config.maintain_rate: usize**: an positive integer to specify the maintain rate used to decide the size of the internal map, where num_levels_maintained = num_levels * maintain_rate. This parameter is used only when choose_to_maintain_smaller_map is set to "true"
  4. **config.choose_to_fixed_time_snapshot: bool**: a boolean value to specify when to take a snapshot, choosing whether the snapshot time to be fixed time window or by event.
     * When choosing "by event"(false), user choose to take a snapshot of the orderbook whenever there are updates in the snapshot within the specified depth
     * When choosing by "fixed_time_interval"(true), user choose to take a snapshot of the orderbook by fixed time interval, e.g. every 20/100ms
  5. **config.snapshot_fixed_time_interval: i64**: the fixed length snapshot time interval(millisecond) to determine snapshot frequency. This parameter is used only when choose_to_fixed_time_snapshot is set to "true".
* To execute:
  1. specified the user parameters according needs by editing config.json as above
  2. have a downloaded input incremental_orderbook_l2.csv.gz file and its full path <input_file_path>
  3. by default the generated output file will be stored in "<input_file_path>/orderbook_snapshots/" folder
  4. commands:
     * cd to folder processing_crate
     * run "cargo build --release"
     * run "./target/release/processing_crate <input_file_path>"
     * e.g: "./target/release/processing_crate  /Users/tanggao/Desktop/projects/datasets/binance-futures_incremental_book_L2_2023-03-09_BTCUSDT.csv.gz"
     

### 1.2 multi
 * input files: all incremental_orderbook_l2.csv.gz files contained in the <input_folder_path>
 * output file: multiple l2_orderbook_snapshot.csv each corresponding to one input folder
 * user-specified parameters: config.json(this file should be put into path "../multi")
 * To execute:
  1. specified the user parameters according needs by editing config.json as above and put it as path "../multi/config.json"
  2. have some downloaded input incremental_orderbook_l2.csv.gz files in a folder, which has full path as <input_folder_path>
  3. by default the generated output files will be stored in "<input_folder_path>/orderbook_snapshots/" folder
  4. commands:
     * cd to folder "../multi"
     * run "cargo run <input_folder_path>" e.g: "cargo run /Users/tanggao/Desktop/projects/datasets/"
       
## Task 2: verify/test the generated orderbook snapshot by day to day comparison
 The way this program verifies whether the orderbook snapshot being built is correct is by comparing the previous day's last orderbook state(represented by program-maintained map) with the current day's first exchange-provided initial orderbook snapshot.
Before execution, the user needs to first have consecutive-date incremental_orderbook_l2.csv.gz files downloaded and stored in the full **<input_folder_path>**. 
### to execute:
To verfy and generate the report of the orderbook snapshots to see the descrepency between consecutive days' orderbook snapshots, run the executable by specifying the input file folder:
* cd to the main folder of this repo "../tardis_l2_topk", you will see a .sh file named "run_generate_report.sh"
* 1. run command "chmod +x run_generate_report.sh" to make it executable
* 2. run command "./run_generate_report.sh **<input_folder_path>**"
     * example "./run_generate_report.sh /Users/tanggao/Desktop/projects/datasets"


