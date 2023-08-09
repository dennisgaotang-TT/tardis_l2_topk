use std::env;
use std::process::Command;



fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run -- <folder_path>");
        return;
    }
    let binary_executable = "./../verify/target/release/verify";
    let folder_path = &args[1];

    // Get a list of all .csv.gz files in the specified folder
    let csv_files = match std::fs::read_dir(folder_path) {
        Ok(files) => files
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        if ext == "gz" {
                            let file_stem = path.file_stem().unwrap_or_default().to_string_lossy();
                            if file_stem.ends_with(".csv") {
                                Some(path)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
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

    let mut child_processes: Vec<std::process::Child> = vec![];

    for file in csv_files {
        println!("the file is={:?}", file);
        let child = Command::new(&binary_executable)
            .arg(file)
            .spawn()
            .expect("Failed to spawn child process");

        child_processes.push(child);
    }

    // Wait for all child processes to complete
    for mut child in child_processes {
        child.wait().expect("Failed to wait for child process");
    }


}
