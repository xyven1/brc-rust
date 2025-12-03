use std::{collections::BTreeMap, fs::File};

use anyhow::Context;
use gxhash::{HashMap, HashMapExt};
use memmap2::{Advice, Mmap};

fn main() -> anyhow::Result<()> {
    let file = File::open("measurements.txt")?;
    let map = unsafe { Mmap::map(&file)? };
    map.advise(Advice::Sequential)?;
    map.advise(Advice::HugePage)?;
    map.advise(Advice::WillNeed)?;
    let lines = map.split(|&c| c == b'\n');
    let mut results = HashMap::<Vec<u8>, (i16, i16, i64, u32)>::with_capacity(10_000);
    let mut total = 0;
    for line in lines {
        if line.is_empty() || total >= 100_000_000 {
            break;
        }
        total += 1;
        if total % 10_000_000 == 0 {
            eprintln!("Processed {total} lines...");
        }
        let mut iter = line.split(|&c| c == b';');
        let (Some(before), Some(after), None) = (iter.next(), iter.next(), iter.next()) else {
            anyhow::bail!("invalid line format");
        };
        let num = parse_number(after);
        match results.get_mut(before) {
            Some((min, max, total, count)) => {
                *min = (*min).min(num);
                *max = (*max).max(num);
                *total += i64::from(num);
                *count += 1;
            }
            None => {
                results.insert(before.into(), (num, num, num.into(), 1));
            }
        }
    }
    eprintln!("Total processed lines: {total}");
    print!("{{");
    for (station, (min, max, total, count)) in BTreeMap::from_iter(results.iter().map(|(k, v)| {
        (
            std::str::from_utf8(k)
                .context("invalid utf-8 in station name")
                .unwrap(),
            *v,
        )
    })) {
        print!(
            "\"{station}\"={:.1}/{:.1}/{:.1},",
            min as f32 / 10.,
            total as f32 / 10. / count as f32,
            max as f32 / 10.
        );
    }
    print!("}}");
    Ok(())
}

fn parse_number(data: &[u8]) -> i16 {
    let negative = if data.first() == Some(&b'-') { -1 } else { 1 };
    match data[if negative < 1 { 1 } else { 0 }..] {
        [ones @ b'0'..=b'9'] => {
            let ones = (ones - b'0') as i16;
            ones * 10 * negative
        }
        [ones @ b'0'..=b'9', b'.', decimal @ b'0'..=b'9'] => {
            let ones = (ones - b'0') as i16;
            let frac = (decimal - b'0') as i16;
            (ones * 10 + frac) * negative
        }
        [tens @ b'0'..=b'9', ones @ b'0'..=b'9'] => {
            let tens = (tens - b'0') as i16;
            let ones = (ones - b'0') as i16;
            (tens * 100 + ones * 10) * negative
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
        _ => panic!("invalid number format"),
    }
}
