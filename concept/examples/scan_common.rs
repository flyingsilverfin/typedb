/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Measurement helpers shared by the scan benchmarks (`has_reverse_scan`, `concept_scan`).

#![allow(dead_code)]

use std::{env, hint::black_box, time::Instant};

pub fn arg(args: &[String], index: usize, default: u64) -> u64 {
    args.get(index).map(|a| a.replace('_', "").parse().expect("numeric argument")).unwrap_or(default)
}

/// If `HRSCAN_PROFILE=<dir>` is set, samples repeated runs of `scan` with pprof for `HRSCAN_PROFILE_SECS` (default 5)
/// seconds and writes `<dir>/<name>.folded` (thread;root;...;leaf count, inlined frames expanded) and
/// `<dir>/<name>.lines` (leaf function @ file:line count).
pub fn profile(name: &str, scan: &mut impl FnMut() -> u64) {
    let Ok(dir) = env::var("HRSCAN_PROFILE") else { return };
    let secs: f64 = env::var("HRSCAN_PROFILE_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(5.0);
    let guard = pprof::ProfilerGuardBuilder::default().frequency(1999).build().unwrap();
    let start = Instant::now();
    let mut runs = 0;
    while start.elapsed().as_secs_f64() < secs {
        black_box(scan());
        runs += 1;
    }
    let report = guard.report().build().unwrap();
    let mut folded = String::new();
    let mut lines: std::collections::HashMap<String, isize> = std::collections::HashMap::new();
    for (frames, count) in &report.data {
        let mut stack = vec![frames.thread_name_or_id()];
        for frame in frames.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                stack.push(symbol.name().replace(';', ":"));
            }
        }
        folded.push_str(&format!("{} {}\n", stack.join(";"), count));
        if let Some(leaf) = frames.frames.first().and_then(|frame| frame.first()) {
            let key = format!("{} @ {}:{}", leaf.name(), leaf.filename(), leaf.lineno());
            *lines.entry(key).or_default() += count;
        }
    }
    let mut lines: Vec<_> = lines.into_iter().collect();
    lines.sort_by(|a, b| b.1.cmp(&a.1));
    let lines: String = lines.into_iter().map(|(key, count)| format!("{count} {key}\n")).collect();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(format!("{dir}/{name}.folded"), folded).unwrap();
    std::fs::write(format!("{dir}/{name}.lines"), lines).unwrap();
    println!("  {name:<12} profiled {runs} runs into {dir}/{name}.{{folded,lines}}");
}

pub fn layer_enabled(name: &str) -> bool {
    env::var("HRSCAN_LAYERS").map(|layers| layers.split(',').any(|layer| layer == name)).unwrap_or(true)
}

/// A single, non-inlined entry point for every measured scan, so that profilers can attribute to it
/// (e.g. `valgrind --tool=callgrind --collect-atstart=no --toggle-collect=*scan_once`).
#[inline(never)]
pub fn scan_once(scan: &mut dyn FnMut() -> u64) -> u64 {
    scan()
}

/// Times `passes + 1` runs of `scan`, which returns the number of keys visited; the first run warms the cache.
pub fn measure(name: &str, passes: u64, expected: u64, mut scan: impl FnMut() -> u64) {
    if !layer_enabled(name) {
        return;
    }
    let mut rates = Vec::new();
    for pass in 0..=passes {
        let start = Instant::now();
        let count = scan_once(&mut scan);
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(count, expected, "{name} visited {count} keys, expected {expected}");
        let rate = count as f64 / secs;
        if pass == 0 {
            println!("  {name:<12} warm-up: {:>6.2}M keys/s ({secs:.2}s)", rate / 1e6);
        } else {
            rates.push(rate);
        }
    }
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = rates[rates.len() / 2];
    println!(
        "{name:<12} median {:>6.2}M keys/s ({:>5.0} ns/key) | best {:>6.2}M | worst {:>6.2}M",
        median / 1e6,
        1e9 / median,
        rates[rates.len() - 1] / 1e6,
        rates[0] / 1e6
    );
    profile(name, &mut scan);
}

