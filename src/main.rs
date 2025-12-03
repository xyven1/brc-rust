use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::File,
    io::{BufWriter, Write, stdout},
    thread::available_parallelism,
};

use anyhow::{Context, Result};
use gxhash::{HashMap, HashMapExt};
use memchr::memchr;
use memmap2::{Advice, Mmap};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn main() -> anyhow::Result<()> {
    let file = File::open("measurements.txt")?;
    let map = unsafe { Mmap::map(&file)? };
    map.advise(Advice::Sequential)?;
    map.advise(Advice::HugePage)?;
    map.advise(Advice::WillNeed)?;

    let (totals, tables) = split(&map, available_parallelism()?.get(), b'\n')?
        .into_par_iter()
        .map(|chunk| {
            eprintln!("Processing chunk of {} bytes", chunk.len());
            process_chunk(chunk)
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let total: u32 = totals.iter().sum();
    eprintln!("Total lines processed: {total}");

    let mut sorted = BTreeMap::new();
    for (key, value) in tables.into_iter().flatten() {
        sorted
            .entry(key)
            .and_modify(|v: &mut Stat| v.merge(&value))
            .or_insert(value);
    }

    let mut writer = BufWriter::new(stdout());
    writer.write_all(b"{")?;
    let mut peekable = sorted.into_iter().peekable();
    while let Some((station, stat)) = peekable.next() {
        writer.write_all(station)?;
        write!(writer, "={stat}")?;
        if peekable.peek().is_some() {
            writer.write_all(b", ")?;
        }
    }
    writer.write_all(b"}")?;
    Ok(())
}

fn split(data: &[u8], parts: usize, needle: u8) -> Result<Box<[&[u8]]>> {
    let mut chunks = Vec::with_capacity(parts);
    let jump = data.len() / parts;
    let mut data = data;
    loop {
        if chunks.len() == parts - 1 {
            break;
        }
        if data.len() <= jump {
            break;
        }
        let Some(idx) = memchr(needle, &data[jump..]) else {
            break;
        };
        let idx = jump + idx;
        chunks.push(&data[..idx + 1]);
        data = &data[idx + 1..];
    }
    chunks.push(data);
    Ok(chunks.into_boxed_slice())
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
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min as f32 / 10.,
            self.total as f32 / 10. / self.count as f32,
            self.max as f32 / 10.
        )
    }
}

fn process_chunk(data: &[u8]) -> Result<(u32, HashMap<&[u8], Stat>)> {
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
        if total % 10_000_000 == 0 {
            eprintln!("Processed {total} lines...");
        };
        let idx = memchr(b';', line).context("No semicolon in line")?;
        let (before, after) = (&line[..idx], &line[idx + 1..]);
        let num = parse_number(after)?;
        match results.get_mut(before) {
            Some(r) => r.update(num),
            None => {
                results.insert(before, Stat::new(num));
            }
        }
    }
    Ok((total, results))
}

fn parse_number(data: &[u8]) -> Result<i16> {
    let negative = if data.first() == Some(&b'-') { -1 } else { 1 };
    Ok(match data[if negative < 1 { 1 } else { 0 }..] {
        [ones @ b'0'..=b'9', b'.', decimal @ b'0'..=b'9'] => {
            let ones = (ones - b'0') as i16;
            let frac = (decimal - b'0') as i16;
            (ones * 10 + frac) * negative
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
            (tens * 100 + ones * 10 + frac) * negative
        }
        _ => anyhow::bail!("invalid number format"),
    })
}
