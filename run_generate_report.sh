#!/bin/bash

# compile the "verify" Rust program
cd ./verify
cargo build --release
cd ..

# compile and run verify_multi to generate the bin folder containing the .bin files, which store the start and end state of the map for a given day
cd ./verify_multi
cargo run "$1"
cd ..

# execute the read_bin_generate_report program
cd ./read_bin_generate_report
cargo build --release
./target/release/read_bin_generate_report "$1/bin/" >> "$1/report.txt"