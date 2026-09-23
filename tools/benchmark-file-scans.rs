#[path = "../src/globwalk.rs"]
mod globwalk;
#[path = "../src/process/paths.rs"]
mod shared;
mod baseline {
    include!(env!("DON_SCAN_BASELINE"));
}

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let workspace = PathBuf::from(args.next().ok_or("Missing workspace path")?);
    let inputs = std::fs::read_to_string(args.next().ok_or("Missing input JSON path")?)?;
    let inputs: Vec<(String, Vec<String>, Vec<String>)> = serde_json::from_str(&inputs)?;
    let pairs: usize = args.next().unwrap_or_else(|| "20".into()).parse()?;
    let since = SystemTime::now() + Duration::from_secs(3600);
    println!("pair,position,variant,seconds");
    for pair in 0..=pairs {
        let order = if pair % 2 == 1 {
            ["baseline", "shared"]
        } else {
            ["shared", "baseline"]
        };
        for (position, variant) in order.into_iter().enumerate() {
            let started = Instant::now();
            let mut scan = shared::FileChangeScan::default();
            for (name, patterns, ignores) in &inputs {
                let changed = if variant == "baseline" {
                    baseline::any_glob_path_changed_since(&workspace, patterns, ignores, since)
                } else {
                    scan.any_changed_since(&workspace, patterns, ignores, since)
                };
                if changed {
                    return Err(format!("Unexpected future-dated file in {name}").into());
                }
            }
            drop(scan);
            let seconds = started.elapsed().as_secs_f64();
            if pair > 0 {
                println!("{pair},{},{variant},{seconds:.9}", position + 1);
            }
        }
    }
    Ok(())
}
