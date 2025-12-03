use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::File,
    io::{BufWriter, Write, stdout},
    num::NonZero,
    thread::available_parallelism,
};

use anyhow::{Context, Result};
use gxhash::{HashMap, HashMapExt};
use memchr::memchr;
use memmap2::{Advice, Mmap};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn main() -> anyhow::Result<()> {
    let file = File::open("./measurements.txt")
        .context("Failed to open measurements file at ./measurements.txt")?;
    // SAFTEY: This file won't be modified while in use.
    let map = unsafe { Mmap::map(&file) }.context("Failed to mmap measurements file")?;
    for advice in [Advice::Sequential, Advice::HugePage, Advice::WillNeed] {
        map.advise(advice)
            .with_context(|| format!("Failed to advise kernel about mmap: advise {advice:?}"))?;
    }

    let cores = available_parallelism().context("Unable to get number of cores")?;
    eprintln!("Using {cores} cores");
    let chunks = chunk_data(&map, cores, b'\n');
    let results = chunks
        .into_par_iter()
        .map(|chunk| {
            eprintln!("Processing chunk {} bytes", chunk.len());
            process_chunk(chunk)
        })
        .collect::<Result<Vec<_>>>()
        .context("One or more chunks could not be processed")?;

    let total: u32 = results.iter().map(|(v, _)| v).sum();
    eprintln!("Total lines processed: {total}");

    let merged_and_sorted = merge_and_sort(results.into_iter().flat_map(|(_, v)| v));
    println!("Num stations: {}", merged_and_sorted.len());
    print(merged_and_sorted).context("Failed to display results")?;
    Ok(())
}

fn chunk_data(data: &[u8], parts: NonZero<usize>, needle: u8) -> Box<[&[u8]]> {
    let mut chunks = Vec::with_capacity(parts.get());
    let jump = data.len() / parts;
    let mut data = data;
    while chunks.len() < parts.get() - 1
        && data.len() > jump
        && let Some(offset) = memchr(needle, &data[jump..])
    {
        let (chunk, rest) = data.split_at(jump + offset + 1);
        chunks.push(chunk);
        data = rest;
    }
    chunks.push(data);
    chunks.into_boxed_slice()
}

fn process_chunk(data: &[u8]) -> Result<(u32, impl Iterator<Item = (&[u8], Stat)>)> {
    let mut results = HashMap::<&[u8], Stat>::with_capacity(10_000);
    let mut total = 0;
    let mut data = data;
    while let Some(idx) = memchr(b'\n', data) {
        let line = &data[..idx];
        data = &data[idx + 1..];
        if line.is_empty() {
            break;
        }
        total += 1;
        let idx = memchr(b';', line).context("No semicolon in line")?;
        let before = line.get(..idx).context("index out of bounds")?;
        let after = line.get(idx + 1..).context("index out of bounds")?;
        let num = parse_number(after)?;
        match results.get_mut(before) {
            Some(r) => r.update(num),
            None => {
                results.insert(before, Stat::new(num));
            }
        }
    }
    Ok((total, results.into_iter()))
}

fn merge_and_sort<'a>(
    unsorted_with_dups: impl Iterator<Item = (&'a [u8], Stat)>,
) -> impl ExactSizeIterator<Item = (&'a [u8], Stat)> {
    let mut merged = HashMap::with_capacity(10_000);
    for (key, value) in unsorted_with_dups {
        merged
            .entry(key)
            .and_modify(|v: &mut Stat| v.merge(&value))
            .or_insert(value);
    }
    BTreeMap::from_iter(merged).into_iter()
}

fn print<'a>(sorted_items: impl Iterator<Item = (&'a [u8], Stat)>) -> Result<()> {
    let mut writer = BufWriter::new(stdout().lock());
    writer.write_all(b"{")?;
    let mut peekable = sorted_items.peekable();
    while let Some((station, stat)) = peekable.next() {
        writer.write_all(station)?;
        write!(writer, "={stat}")?;
        if peekable.peek().is_some() {
            writer.write_all(b", ")?;
        }
    }
    writer.write_all(b"}\n")?;
    Ok(())
}

fn parse_number(data: &[u8]) -> Result<i16> {
    let negative = data.first() == Some(&b'-');
    Ok(match data[usize::from(negative)..] {
        [ones @ b'0'..=b'9', b'.', decimal @ b'0'..=b'9'] => {
            let ones = (ones - b'0') as i16;
            let frac = (decimal - b'0') as i16;
            (ones * 10 + frac) * (i16::from(negative) * 2 - 1)
        }
        [
            tens @ b'0'..=b'9',
            ones @ b'0'..=b'9',
            b'.',
            decimal @ b'0'..=b'9',
        ] => {
            let tens = (tens - b'0') as i16;
            let ones = (ones - b'0') as i16;
            let frac = (decimal - b'0') as i16;
            (tens * 100 + ones * 10 + frac) * (i16::from(negative) * 2 - 1)
        }
        _ => anyhow::bail!("invalid number format"),
    })
}

struct Stat {
    min: i16,
    max: i16,
    total: i64,
    count: u32,
}
impl Stat {
    fn new(num: i16) -> Self {
        Self {
            min: num,
            max: num,
            total: num.into(),
            count: 1,
        }
    }
    fn update(&mut self, num: i16) {
        self.min = self.min.min(num);
        self.max = self.max.max(num);
        self.total += i64::from(num);
        self.count += 1;
    }
    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.total += other.total;
        self.count += other.count;
    }
}
impl Display for Stat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut avg = (self.total as f32 / self.count as f32).round() / 10.;
        if avg == -0. {
            avg = 0.
        }
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min as f32 / 10.,
            avg,
            self.max as f32 / 10.
        )
    }
}
